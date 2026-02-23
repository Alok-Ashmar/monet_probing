import os
import pytz
from redis import Redis
from bson import ObjectId
from datetime import datetime
from langsmith import traceable
from typing import AsyncIterable
from utils.intent import extract_intent
from services.LLMAdapter import LLMAdapter
from utils.ServerLogger import ServerLogger
from database.MongoWrapper import monet_db  # type: ignore
from langchain_core.messages import SystemMessage
from services.ProdNSightGenerator import NSIGHT, NSIGHT_v2
from models.Survey import PySurvey, PySurveyQuestion, SurveyResponse
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate
from langchain_community.chat_message_histories import RedisChatMessageHistory

india = pytz.timezone('Asia/Kolkata')
logger = ServerLogger()

QnAs = monet_db.get_collection("QnAs")

class Probe(LLMAdapter):

    __version__ = "3.0.0"

    invalid = False

    def __init__(self,
        mo_id: str, 
        metadata: PySurvey,
        question: PySurveyQuestion,
        simple_store=False,
        session_no:int = 0,
        survey_details: SurveyResponse = None,
        ):
        super().__init__(metadata.config.llm, 0.7, streaming=True)
        self.id = f"{metadata.id}-{question.id}-{mo_id}"
        self.__metric_llm__ = self.llm.with_structured_output(NSIGHT, method="function_calling")
        self.metadata = metadata
        self.counter = 0
        self.simple_store = simple_store
        self.question = question
        self.mo_id = mo_id
        self.su_id = self.metadata.id
        self.qs_id = question.id
        self.ended = False
        self.session_no = session_no
        self.survey_details = survey_details

        if question.config.probes > question.config.max_probes:
            self.invalid = True

        self.__prompt_chunks__ = {
            "main-chk": """
                You are a video analysis partner. Your goal is to extract truth from the user's input based *strictly* on the provided context description, while **mirroring the user's level of specificity**.
                    1. **Valid/General Subject:** If the user uses general terms (e.g., "the actor", "the music"), ask a detail question using those SAME general terms. **DO NOT** insert specific character/actor names from context unless the user wrote them first.
                    2. **Mixed Subject (Real + Fake):** If the user links an unverified subject with a verified one, **IGNORE the unverified subject** and ask only about the verified one.
                    3. **Purely Fake/Off-Topic:** If the user *only* mentions a person/object NOT in the context, you MUST ask: "Where did you notice [Subject] in **[Video Title]**?".""",

            "rule-chk": """
                Subject Verification Logic (MANDATORY)
                    - **Scenario A: Purely On-Topic / General**
                    (User mentions verified elements or general concepts like 'the actor', 'the setting')
                    -> Action: Ask a natural follow-up about visual/audio details.
                    -> **CRITICAL:** Use generic terms (e.g., "the performer", "that character"). **NEVER** swap a general word for a specific name (e.g., do NOT say the Actor's Name) unless the user named them first.

                    - **Scenario B: Mixed Input (Verified + Unverified)**
                    (User links a correct element with an incorrect/hallucinated one)
                    -> Action: The user is adding false details. **Pivot immediately to the Verified element.**
                    -> Example: "What specific details of **[Verified Subject's]** performance stood out in that moment?" (Ignore the incorrect part).

                    - **Scenario C: Purely Off-Topic**
                    (User mentions a person/object completely absent from the context)
                    -> Action: Challenge them using the Video Title from the context.
                    -> Example: "Where did you notice [Unverified Subject] *in [Video Title]*?"

                Stay Anchored
                    - Keep the conversation relevant to the original question.
                    - Do not introduce new themes or interpretations.
                    - If on topic, use user's last idea to build your follow-up.
                    - Redirect to original question's intent/context if user diverts too much away from the topic.
                    - Do not validate hallucinations.

                Ask One Clear Question
                    - Keep it short (max 15 words).
                    - No multi-part questions.
                    - Avoid emotional or symbolic language unless the user introduces it.

                Encourage Elaboration
                    - Provide hints and contexts subtly wherever required.
                    - Focus on visible evidence.
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
    
        self._history_redis_url = os.environ.get(
            "REDIS_URL",
            "redis://localhost:6379/0"
        )
        self._redis = Redis.from_url(self._history_redis_url)

        # survey level context (switch)
        if self.metadata.config.add_context:
            self.__system_prompt__ = PromptTemplate(
                template = """
                    {main_prompt}
                    
                    Survey Description: {survey_description}
                    
                """
            ).invoke(
                {
                    "main_prompt": self.__system_prompt__,
                    "survey_description": self.metadata.description
                }
            ).text

        # survey question level context (switch) 
        if self.question.config.add_context:
            extracted_intent = extract_intent(
                question_description=self.question.description,
                question_text=self.question.question,
                survey_details=self.survey_details,
                invoke_fn=self.invoke,
                logger=logger,
                redis_client=self._redis,
                ttl_seconds=int(os.environ.get("REDIS_TTL_SECONDS_INTENT", 86400)) # 24 hours
            )
            if not extracted_intent:
                extracted_intent = self.question.description

            self.__system_prompt__ = PromptTemplate(
                template = """
                    {main_prompt}
                    
                    Original Question: {original_question}
                    
                    Question Intended Purpose: {question_intent}
                """
            ).invoke(
                {
                    "main_prompt": self.__system_prompt__,
                    "original_question": self.question.question,
                    "question_intent": extracted_intent
                }
            ).text        
        else:
            self.__system_prompt__ = PromptTemplate(
                template = """
                    {main_prompt}
                    
                    Original Question: {original_question}
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

                    <-- Language Instruction -->
                    Please ask Questions in {language} language.
                    <-- Language Instruction -->
                """
            ).invoke(
                {
                    "main_prompt": self.__system_prompt__,
                    "language": self.metadata.config.language
                }
            ).text
        
        self._history = RedisChatMessageHistory(
            session_id=self._session_id(),
            url=self._history_redis_url,
            ttl=int(os.environ.get("REDIS_TTL_SECONDS", 3600))
        )

        self._ensure_system_message()


    def _session_id(self) -> str:
        # Use a consistent session ID across the entire interaction for this user/question
        return f"{self.id}"


    def _ensure_system_message(self):
        if not self._history.messages:
            self._history.add_message(SystemMessage(content=self.__system_prompt__))

    def __getstate__(self):
        state = self.__dict__.copy()
        # Exclude unpicklable objects
        state.pop('llm', None)
        state.pop('_LLMAdapter__llama_client', None)
        state.pop('_Probe__metric_llm__', None)
        state.pop('_redis', None)
        state.pop('_history', None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Re-initialize unpicklable objects
        # 1. Re-init LLM (from LLMAdapter)
        super(Probe, self).__init__(self.metadata.config.llm, 0.7, streaming=True)
        # 2. Re-init metric LLM
        self.__metric_llm__ = self.llm.with_structured_output(NSIGHT, method="function_calling")
        # 3. Re-init Redis connection
        self._redis = Redis.from_url(self._history_redis_url)
        # 4. Re-init History
        self._history = RedisChatMessageHistory(
            session_id=self._session_id(),
            url=self._history_redis_url,
            ttl=int(os.environ.get("REDIS_TTL_SECONDS", 3600))
        )


    async def _stream_with_history_update(self, chain, inputs: dict, run_config: dict, update_history: bool = True):
        full_content = ""
        async for chunk in chain.astream(inputs, config=run_config):
            content = chunk.content if hasattr(chunk, "content") else str(chunk)
            full_content += content
            yield chunk
        if full_content and update_history:
            self._history.add_ai_message(full_content)


    def add_user_response(self, response: str):
        self.counter += 1
        user_text = f"Response {self.counter}. {response}"
        self._history.add_user_message(user_text)

    @traceable(run_type="chain", name="Get Metrics")
    async def gen_metrics(self, response: str) -> NSIGHT:
        prompt = ChatPromptTemplate.from_messages(self._history.messages)
        metric_chain = prompt | self.__metric_llm__
        
        run_config = {
            "metadata": {
                "mo_id": self.mo_id,
                "su_id": str(self.su_id),
                "qs_id": str(self.qs_id),
                "session_no": self.session_no
            },
            "tags": ["metrics", "websocket"]
        }
        
        return await metric_chain.ainvoke({}, config=run_config)


    @traceable(run_type="chain", name="Gen Streamed Follow Up")
    def gen_follow_up_stream(self, use_redirection: bool = False) -> AsyncIterable[str]:
        if use_redirection:
            # Inject redirection steering as a system-like instruction at the end of history for this turn
            redirect_msg = f"The user's response was irrelevant to the original question. Please politely acknowledge their response but firmly rephrase the original question to redirect them back to the topic: {self.question.question}"
            messages = self._history.messages + [SystemMessage(content=redirect_msg)]
            prompt = ChatPromptTemplate.from_messages(messages)
            # We don't want to save the "redirection AI response" to permanent history in the same way 
            # or maybe we do? Usually we do to keep context.
            update_history = True 
        else:
            prompt = ChatPromptTemplate.from_messages(self._history.messages)
            update_history = True

        chain = prompt | self.llm
        
        run_config = {
            "metadata": {
                "mo_id": self.mo_id,
                "su_id": str(self.su_id),
                "qs_id": str(self.qs_id),
                "session_no": self.session_no
            },
            "tags": ["probe", "websocket"]
        }

        return self._stream_with_history_update(chain, {}, run_config, update_history=update_history)


    @traceable(run_type="chain", name="Legacy Gen Streamed Follow Up")
    def gen_streamed_follow_up(self, question: str, response: str) -> tuple[AsyncIterable[str], AsyncIterable[NSIGHT]]:
        # This remains for backward compatibility or if called directly without threshold logic
        metric_stream = self.gen_metrics(response)
        llm_stream = self.gen_follow_up_stream(use_redirection=False)
        return (llm_stream, metric_stream)


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
