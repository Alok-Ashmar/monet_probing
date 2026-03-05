from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field

from .types import PyObjectId
from .enums import StatusEnum, LLMEnum, StrategyEnum


# -- Configuration Schemas
class SurveyConfig(BaseModel):
    """
    Configuration settings for a Survey.
    Includes settings for the language model, metrics tracking, and context addition.
    """

    language: str = "English"
    metrics: bool = True
    llm: LLMEnum = LLMEnum("chatgpt")
    llm_role: str = "survey assistant"
    content_type: str = "Movie Trailer"
    mediaAI: bool = False  # overall check for the feature
    add_context: bool = False
    adaptive_probing: bool = False


class SurveyMedia(BaseModel):
    """
    Media information associated with a Survey, such as external video links or metadata.
    """

    media_url: str = ""
    media_filename: str = ""
    media_content_type: str = "video/mp4"
    media_description: str = ""
    media_name: str = ""
    media_movie_name: str = ""
    media_upload_timestamp: datetime = Field(default_factory=datetime.now)


class TargetConfig(BaseModel):
    """
    Configuration for probing targets to apply to questions.
    """

    target: str
    priority: int = 1
    strategy: StrategyEnum


class MediaConfig(BaseModel):
    """
    Configuration indicating if specific media types are available for a question.
    """

    available: bool = False
    audio: bool = False
    video: bool = False


class QuestionConfig(BaseModel):
    """
    Configuration specific to a single survey question.
    Controls probing behavior, contextual settings, and acceptable quality thresholds.
    """

    probes: int = 0
    max_probes: int = 0
    targets: List[TargetConfig] = []
    description: str = ""
    media: MediaConfig = Field(
        default_factory=MediaConfig
    )  # survey config media should be true in order for this to work
    add_context: bool = False
    allow_pasting: bool = False
    quality_threshold: int = 4
    gibberish_score: int = 7


# -- Base Survey Schemas
class Survey(BaseModel):
    """
    Base Pydantic schema for creating or representing a Survey metadata object.
    """

    title: str
    description: str
    media: SurveyMedia = Field(default_factory=SurveyMedia)
    config: SurveyConfig = Field(default_factory=SurveyConfig)
    createdAt: Optional[datetime] = None
    status: StatusEnum
    tags: List[str] = []
    display: bool


class SurveyQuestion(BaseModel):
    """
    Base Pydantic schema for creating or representing a specific Question within a Survey.
    """

    su_id: Optional[str] = None
    question: str
    description: str = ""
    seq_num: int = Field(..., description="Sequence position of the question")
    config: QuestionConfig = Field(default_factory=QuestionConfig)


class SurveyResponse(BaseModel):
    """
    Base Pydantic schema for processing or submitting user responses.
    """

    su_id: str
    mo_id: str
    qs_id: str
    cnt_id: Optional[str] = None
    question: str
    response: str
    comment: Optional[str] = None
    relevant: bool = True


# -- MongoDB (PyObjectId) Wrapper Schemas
class PySurveyQuestion(SurveyQuestion):
    """
    SurveyQuestion schema bound to MongoDB IDs.
    """

    su_id: PyObjectId = Field(default_factory=PyObjectId, alias="su_id")
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")


class PySurveyResponse(SurveyResponse):
    """
    SurveyResponse schema bound to a MongoDB ID.
    """

    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")


class PySurvey(Survey):
    """
    Full Survey schema bound to MongoDB IDs, complete with its list of questions.
    """

    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    questions: List[PySurveyQuestion] = []


class CreateSurvey(Survey):
    """
    Schema for handling Survey creation payloads.
    """

    id: Optional[PyObjectId] = None
    questions: List[SurveyQuestion]


# -- Controller / DB Switcher Wrapper Schemas (Pd represents Database layer structures)
class PdSurvey(BaseModel):
    """
    Database-agnostic representation of a Survey used when interacting with the database service layer.
    """

    id: Optional[int] = None
    study_id: Optional[int] = None
    cnt_id: int = 0
    survey_description: str
    survey_title: Optional[str] = None
    llm: LLMEnum = LLMEnum("chatgpt")
    language: str = "English"
    add_context: bool = False
    config: SurveyConfig = Field(default_factory=SurveyConfig)


class PdSurveyQuestion(BaseModel):
    """
    Database-agnostic representation of a Survey Question used with the database service layer.
    """

    id: Optional[int] = None
    qs_id: Optional[int] = None
    su_id: Optional[int] = None  # this will correspond to survey.id
    cnt_id: int = 0
    question: str = "<question>"
    description: str = "<description>"
    seq_num: int = Field(
        default_factory=int, description="Sequence position of the question"
    )
    config: QuestionConfig = Field(default_factory=QuestionConfig)


# -- API Response / Auth Schemas
class GetSurveyResponse(BaseModel):
    """
    Schema representing a standardized API response when fetching Surveys.
    """

    code: int
    error: bool
    message: str
    response: List[PySurvey]


class AccessRequest(BaseModel):
    """
    Schema representing a user request for platform access.
    """

    firstName: str
    lastName: str
    email: str
    organization: str
    department: str
    jobTitle: str
    requestType: str
    message: str


class get_token(BaseModel):
    """
    Schema representing the payload to request an authentication token.
    """

    client_id: str
    client_secret: str
    code: str
    redirect_uri: str
    grant_type: str
