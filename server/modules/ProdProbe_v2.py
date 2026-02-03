import pytz
from bson import ObjectId
from datetime import datetime
from typing import AsyncIterable
from modules.MongoWrapper import monet_db
from modules.LLMAdapter import LLMAdapter
from modules.ServerLogger import ServerLogger
from models.Survey import PySurvey, PySurveyQuestion
from modules.ProdNSightGenerator import NSIGHT, NSIGHT_v2
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate

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
        super().__init__(metadata.config.llm, 0.65, streaming=True)
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

        self.conversation = []

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
    
        # survey level context (switch)
        if self.metadata.config.add_context:
            self.__system_prompt__ = PromptTemplate(
                template = """
                    {main_prompt}
                    
                    <!-- Survey Description -->
                    {survey_description}
                    <!-- Survey Description -->
                    
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
                    
                    **Original Question**
                    {original_question}
                    
                    **Question's Intended Purpose**
                    {question_context}
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
                    
                    **Original Question**
                    {original_question}
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
        
        self.conversation.append(("system", self.__system_prompt__))

    def gen_streamed_follow_up(self, question: str, response: str) -> tuple[AsyncIterable[str], AsyncIterable[NSIGHT]]:
        # handle response
        self.conversation.append(("user", """Response {question_num}. {response_text}""")) # Add this user response
        prompt = ChatPromptTemplate.from_messages(self.conversation)
        chain = prompt | self.llm
        metric_chain = prompt | self.__metric_llm__

        llm_stream: str = chain.astream({
            "question_num": self.counter + 1,
            "response_text": response
        })
        metric_llm_stream: NSIGHT = metric_chain.astream({
            "question_num": self.counter + 1,
            "response_text": response
        })
        return (llm_stream, metric_llm_stream)

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