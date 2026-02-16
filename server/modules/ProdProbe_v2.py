import pytz
from bson import ObjectId
from datetime import datetime
from langsmith import traceable
from typing import AsyncIterable
from modules.LLMAdapter import LLMAdapter
from modules.MongoWrapper import monet_db
from modules.ServerLogger import ServerLogger
from models.Survey import PySurvey, PySurveyQuestion
from modules.ProdNSightGenerator import NSIGHT, NSIGHT_v2
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage

india = pytz.timezone('Asia/Kolkata')
logger = ServerLogger()

QnAs = monet_db.get_collection("QnAs")

class Probe(LLMAdapter):

    __version__ = "2.2.0"

    invalid = False

    def __init__(self,
        mo_id: str, # user ref
        metadata: PySurvey, # survey ref
        question: PySurveyQuestion, # survey question ref
        simple_store=False,
        session_no:int = 0,
        ):
        super().__init__(metadata.config.llm, 0.7, streaming=True)
        self.id = f"{metadata.id}-{question.id}-{mo_id}"
        self.__metric_llm__ = self.llm.with_structured_output(NSIGHT)
        self.metadata = metadata
        self.counter = 0
        self.simple_store = simple_store
        self.question = question
        self.mo_id = mo_id
        self.su_id = ObjectId(self.metadata.id)
        self.qs_id = ObjectId(question.id)
        self.ended = False
        self.session_no = session_no

        if question.config.probes > question.config.max_probes:
            self.invalid = True

        self.__prompt_chunks__ = {
            "main-chk": """You are an expert survey moderator. Your task is to generate a single, concise follow-up question based on the provided inputs.""",
            "rule-chk": """
                Logic & Instructions:
                    Analyze the User Response against the Original Question, Intended Purpose, and Survey Description.

                    SCENARIO A: The response is relevant.
                    - If the response aligns with the Intended Purpose, generate a follow-up that explores the User Response more deeply.
                    - Goal: Uncover specific details, feelings, or reasons behind their answer.

                    SCENARIO B: The response is irrelevant/off-topic.
                    - If the response does not match the Original Question or falls outside the Survey Description, generate a follow-up that politely steers the user back to the Original Question.
                    - Goal: Redirect focus without offering specific ideas, examples, or answers.

                    Constraints:
                    - Length: Maximum 15 words.
                    - Context: Strictly adhere to the Survey Description. Do not hallucinate scenarios or assume facts not present in the description.
                    - Format: Single sentence only. No multi-part questions. No generic "Tell me more."
                    - Tone: Neutral, curious, and encouraging.

                    Output: [Generate only the follow-up question]

                    Inputs:
            """
        }

        self.__system_prompt__ = PromptTemplate(
            template = """
                {main-chk}
                {rule-chk}
                """
            ).invoke(
                {
                    "main-chk": self.__prompt_chunks__["main-chk"],
                    "rule-chk": self.__prompt_chunks__["rule-chk"]
                }
            ).text
    
        # survey level context (switch)
        if self.metadata.config.add_context:
            self.__system_prompt__ = PromptTemplate(
                template = """
                    {main_prompt}
                    
                    **Survey Description**: {survey_description}
                """
            ).invoke(
                {
                    "main_prompt": self.__system_prompt__,
                    "survey_description": self.metadata.description
                }
            ).text

        # survey question level context (switch) 
        if self.question.config.add_context:
            self.__system_prompt__ = PromptTemplate(
                template = """
                    {main_prompt}
                    
                    **Original Question**: {original_question}
                    
                    **Question's Intended Purpose**: {question_context}
                """
            ).invoke(
                {
                    "main_prompt": self.__system_prompt__,
                    "original_question": self.question.question,
                    "question_context": self.question.description
                }
            ).text        
        else:
            self.__system_prompt__ = PromptTemplate(
                template = """
                    {main_prompt}
                    
                    **Original Question**: {original_question}
                """
            ).invoke(
                {
                    "main_prompt": self.__system_prompt__,
                    "original_question": self.question.question
                }
            ).text
            
        # language prompt (switch)
        if self.metadata.config.language != "English":
            self.__system_prompt__ = PromptTemplate(
                template = """
                    {main_prompt}
                    
                    Please ask Questions in {language} language.
                """
            ).invoke(
                {
                    "main_prompt": self.__system_prompt__,
                    "language": self.metadata.config.language
                }
            ).text
        
        self._history = []
        self._ensure_system_message()

    def _ensure_system_message(self):
        if not self._history:
            self._history.append(SystemMessage(content=self.__system_prompt__))

    async def _stream_with_history_update(self, chain, inputs: dict, run_config: dict):
        full_content = ""
        async for chunk in chain.astream(inputs, config=run_config):
            content = chunk.content if hasattr(chunk, "content") else str(chunk)
            full_content += content
            yield chunk
        if full_content:
            self._history.append(AIMessage(content=full_content))

    @traceable(run_type="chain", name="Gen Streamed Follow Up")
    def gen_streamed_follow_up(self, question: str, response: str) -> tuple[AsyncIterable[str], AsyncIterable[NSIGHT]]:
        next_counter = self.counter + 1
        user_text = f"User Response: {next_counter}. {response}"
        self._history.append(HumanMessage(content=user_text))
        self.counter = next_counter
        prompt = ChatPromptTemplate.from_messages(self._history)
        chain = prompt | self.llm
        metric_chain = prompt | self.__metric_llm__

        # Define metadata for tracing (User ID, Survey ID, Question ID)
        run_config = {
            "metadata": {
                "mo_id": self.mo_id,
                "su_id": str(self.su_id),
                "qs_id": str(self.qs_id),
                "session_no": self.session_no
            },
            "tags": ["probe", "websocket"]
        }

        llm_stream: str = self._stream_with_history_update(chain, {}, run_config)
        
        metric_llm_stream: NSIGHT = metric_chain.astream({}, config={**run_config, "tags": ["metrics", "websocket"]})
        return (llm_stream, metric_llm_stream)

    @traceable(run_type="tool", name="Store Response")
    def store_response(self, nsight_v2: NSIGHT_v2, session_no: int):
        now_india = datetime.now(india)
        insert_one_res = QnAs.insert_one({
            **nsight_v2.model_dump(),
            "ended": self.ended,
            "mo_id": self.mo_id,
            "su_id": self.su_id, 
            "qs_id": self.qs_id,
            "qs_no": self.counter + 1,
            "created_at": now_india.isoformat(),
            "session_no": session_no,
        })
        logger.info("Inserted one doc successfully")
        logger.info(insert_one_res)
        return insert_one_res