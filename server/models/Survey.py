from enum import Enum
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from .payload import LLMEnum, PyObjectId


# -- Survey
class SurveyConfig(BaseModel):
    language: str = "English"
    metrics: bool = True
    llm: LLMEnum = LLMEnum("chatgpt")
    metrics: bool = True
    llm_role: str = "survey assistant"
    content_type: str = "Movie Trailer"
    mediaAI: bool = False  # overall check for the feature
    add_context: bool = False
    adaptive_probing: bool = False


class SurveyMedia(BaseModel):
    media_url: str = ""
    media_filename: str = ""
    media_content_type: str = "video/mp4"
    media_description: str = ""
    media_name: str = ""
    media_movie_name: str = ""
    media_upload_timestamp: datetime = datetime.now()


class status(str, Enum):
    active = "active"
    draft = "draft"


class Survey(BaseModel):
    title: str
    description: str
    media: SurveyMedia = SurveyMedia()
    config: SurveyConfig = SurveyConfig()
    createdAt: Optional[datetime]
    status: status
    tags: list[str] = []
    display: bool


# -- Question and Response
class Strategy(str, Enum):
    presence = "presence"
    absence = "absence"
    avoid_on = "avoid_on"
    canned = "canned"


class TargetConfig(BaseModel):
    target: str
    priority: int = 1
    strategy: Strategy


class MediaConfig(BaseModel):
    available: bool = False
    audio: bool = False
    video: bool = False


class QuestionConfig(BaseModel):
    probes: int = 0
    max_probes: int = 0
    targets: List[TargetConfig] = []
    description: str = ""
    media: MediaConfig = (
        MediaConfig()
    )  # survey config media should be true in order for this to work
    add_context: bool = False
    allow_pasting: bool = False
    quality_threshold: int = 4
    gibberish_score: int = 7


class SurveyQuestion(BaseModel):
    su_id: Optional[str]
    question: str
    description: str = ""
    seq_num: int = Field(..., description="Sequence position of the question")
    config: QuestionConfig = QuestionConfig()


class PySurveyQuestion(SurveyQuestion):
    su_id: PyObjectId = Field(default_factory=PyObjectId, alias="su_id")
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")


class SurveyResponse(BaseModel):
    su_id: str
    mo_id: str
    qs_id: str
    question: str
    response: str
    comment: str | None = None
    relevant: bool = True


class PySurveyResponse(SurveyResponse):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")


# Payloads
class PySurvey(Survey):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    questions: List[PySurveyQuestion] = []


class GetSurveyResponse(BaseModel):
    code: int
    error: bool
    message: str
    response: List[PySurvey]


class CreateSurvey(Survey):
    id: Optional[PyObjectId] = None
    questions: list[SurveyQuestion]


class mailType(str, Enum):
    contact_admin = "contact_admin"
    reset_password = "reset_password"


class AccessRequest(BaseModel):
    firstName: str
    lastName: str
    email: str
    organization: str
    department: str
    jobTitle: str
    requestType: str
    message: str

class get_token(BaseModel):
    client_id: str
    client_secret: str
    code : str
    redirect_uri: str
    grant_type: str

class PdSurvey(BaseModel):
    id: Optional[int] = None
    study_id: Optional[int] = None
    cnt_id: int = 0
    survey_description: str
    survey_title: Optional[str] = None
    # service: str = "monadic"
    llm: LLMEnum = LLMEnum("chatgpt")
    language: str = "English"
    add_context: bool = False
    config: SurveyConfig = SurveyConfig()

class PdSurveyQuestion(BaseModel):
    id: Optional[int] = None
    qs_id: Optional[int] = None
    su_id: Optional[int] = None  # this will correspond to survey.id
    cnt_id: int = 0
    question: str = "<question>"
    description: str = "<description>"
    seq_num: int = Field(default_factory=int, description="Sequence position of the question")
    config: QuestionConfig = QuestionConfig()
