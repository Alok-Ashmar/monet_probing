import os
import json
import pytz
from redis import Redis
from bson import ObjectId
from datetime import datetime
from typing import AsyncIterable
from utils.intent import extract_intent
from modules.LLMAdapter import LLMAdapter
from utils.ServerLogger import ServerLogger
from langchain_core.messages import SystemMessage
from modules.ProdNSightGenerator import NSIGHT, NSIGHT_v2
from models.schemas import PySurvey, PySurveyQuestion, SurveyResponse
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate
from langchain_community.chat_message_histories import RedisChatMessageHistory

india = pytz.timezone('Asia/Kolkata')
logger = ServerLogger()

class Probe(LLMAdapter):

    __version__ = "3.0.0"

    invalid = False

    def __init__(self,
        mo_id: str, # user ref
        metadata: PySurvey, # survey ref
        question: PySurveyQuestion, # survey question ref
        simple_store=False,
        session_no:int = 0,
        survey_details: SurveyResponse = None,
        ):
        super().__init__(metadata.config.llm, 0.7, streaming=True)
        self.id = f"{metadata.id}-{question.id}-{mo_id}"
        self.__metric_llm__ = self.llm.with_structured_output(NSIGHT.model_json_schema())
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
        
        # Load counter from probe state
        self._history_redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        self._redis = Redis.from_url(self._history_redis_url)
        
        try:
            state_key = f"probe_state:{self.su_id}:{self.qs_id}:{self.mo_id}"
            cached_state = self._redis.get(state_key)
            if cached_state:
                state_dict = json.loads(cached_state)
                if state_dict.get("session_no") == self.session_no:
                    self.counter = int(state_dict.get("counter", 0))
        except Exception as e:
            logger.error(f"Failed to load counter from Redis state: {e}")

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
        return f"{self.id}:{self.session_no}"


    def _ensure_system_message(self):
        if not self._history.messages:
            self._history.add_message(SystemMessage(content=self.__system_prompt__))

    async def _stream_with_history_update(self, chain, inputs: dict):
        full_content = ""
        async for chunk in chain.astream(inputs):
            content = chunk.content if hasattr(chunk, "content") else str(chunk)
            full_content += content
            yield chunk
        if full_content:
            self._history.add_ai_message(full_content)


    def gen_streamed_follow_up(self, question: str, response: str) -> tuple[AsyncIterable[str], AsyncIterable[NSIGHT]]:
        user_text = f"Response {self.counter}. {response}"
        self._history.add_user_message(user_text)
        prompt = ChatPromptTemplate.from_messages(self._history.messages)
        chain = prompt | self.llm
        metric_chain = prompt | self.__metric_llm__

        llm_stream: str = self._stream_with_history_update(chain, {})
        
        metric_llm_stream: NSIGHT = metric_chain.astream({})
        return (llm_stream, metric_llm_stream)
