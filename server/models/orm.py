from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    Enum as SQLEnum,
    ForeignKey,
    Text,
    JSON,
)
from sqlalchemy.orm import relationship, declarative_base

from .enums import StatusEnum

Base = declarative_base()


# ----- STUDY SUMMARY -----
class StudySummary(Base):
    """
    SQLAlchemy model representing the summary of a study.
    Tracks overall statistics and processing methods for a specific study.
    """

    __tablename__ = "probe_study_summary"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    study_id = Column(Integer, index=True)
    cnt_id = Column(Integer, nullable=True)
    cnt_name = Column(String(255), nullable=True)  # MySQL requires length
    overall_summary = Column(JSON, default={})
    processing_method = Column(String(255), nullable=True)
    response_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, insert_default=datetime.now, onupdate=datetime.now)

    question_summaries = relationship(
        "QuestionSummary",
        back_populates="study_summary_id",
        # cascade="all, delete-orphan",
    )


class QuestionSummary(Base):
    """
    SQLAlchemy model representing the summary of a specific question within a study.
    Linked to a StudySummary record.
    """

    __tablename__ = "probe_question_summary"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    study_id = Column(Integer, ForeignKey("probe_study_summary.id"))
    qs_id = Column(Integer)
    question = Column(Text)
    summary = Column(JSON, default={})
    SPSS = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, insert_default=datetime.now, onupdate=datetime.now)

    study_summary_id = relationship("StudySummary", back_populates="question_summaries")


# ----- SURVEY -----
class Survey(Base):
    """
    SQLAlchemy model representing a Survey configuration.
    Contains metadata such as title, LLM settings, and display language.
    """

    __tablename__ = "probe_survey"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    cnt_id = Column(Integer, nullable=False)
    study_id = Column(Integer, nullable=False)
    survey_title = Column(String(255), nullable=False)
    survey_description = Column(Text, nullable=True)
    service = Column(String(255), default="monadic")
    llm = Column(String(255), default="chatgpt")
    language = Column(String(50), default="English")
    add_context = Column(Boolean, default=False)
    config = Column(JSON, default={})
    status = Column(SQLEnum(StatusEnum), default=StatusEnum.draft)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, insert_default=datetime.now, onupdate=datetime.now)


# ----- SURVEY QUESTION -----
class SurveyQuestion(Base):
    """
    SQLAlchemy model representing a specific question in a Survey.
    Includes the actual question text, sequence position, and specific configurations.
    """

    __tablename__ = "probe_survey_question"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    su_id = Column(Integer)
    qs_id = Column(Integer)
    cnt_id = Column(Integer)
    question = Column(Text, nullable=False)
    description = Column(Text)
    seq_num = Column(Integer, default=0)
    config = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, insert_default=datetime.now, onupdate=datetime.now)


# ----- SURVEY RESPONSE -----
class SurveyResponse(Base):
    """
    SQLAlchemy model representing a user's response to a SurveyQuestion.
    Stores the response text, associated quality metrics, and session information.
    """

    __tablename__ = "probe_survey_response"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    su_id = Column(Integer)
    mo_id = Column(Integer)
    qs_id = Column(Integer)
    cnt_id = Column(Integer)
    question = Column(Text)
    response = Column(Text)
    reason = Column(Text)
    keywords = Column(JSON)
    quality = Column(Integer, default=0)
    relevance = Column(Integer, default=0)
    confusion = Column(Integer, default=0)
    negativity = Column(Integer, default=0)
    consistency = Column(Integer, default=0)
    confidence = Column(Integer, default=0)
    qs_no = Column(Integer)
    session_no = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, insert_default=datetime.now, onupdate=datetime.now)


class SurveyResponseTest(Base):
    """
    SQLAlchemy model acting as a test version of SurveyResponse.
    Has identical structure to SurveyResponse for testing without modifying production data.
    """

    __tablename__ = "probe_survey_response_test"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    su_id = Column(Integer)
    mo_id = Column(Integer)
    qs_id = Column(Integer)
    cnt_id = Column(Integer)
    question = Column(Text)
    response = Column(Text)
    reason = Column(Text)
    keywords = Column(JSON)
    quality = Column(Integer, default=0)
    relevance = Column(Integer, default=0)
    confusion = Column(Integer, default=0)
    negativity = Column(Integer, default=0)
    consistency = Column(Integer, default=0)
    confidence = Column(Integer, default=0)
    qs_no = Column(Integer)
    session_no = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, insert_default=datetime.now, onupdate=datetime.now)
