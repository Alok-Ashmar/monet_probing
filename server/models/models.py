from datetime import datetime
from enum import Enum as PyEnum

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Enum,
    ForeignKey, Text, JSON
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

# ----- ENUMS -----
class StatusEnum(str, PyEnum):
    active = "active"
    draft = "draft"

class StrategyEnum(str, PyEnum):
    presence = "presence"
    absence = "absence"
    avoid_on = "avoid_on"
    canned = "canned"

# ----- STUDY SUMMARY -----
class StudySummary(Base):
    __tablename__ = "probe_study_summary"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    study_id = Column(Integer, index=True)
    cnt_id = Column(Integer, nullable=True)
    cnt_name = Column(String(255), nullable=True)  # MySQL requires length
    overall_summary = Column(JSON, default={})
    processing_method = Column(String(255), nullable=True)
    response_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.now)
    question_summaries = relationship(
        "QuestionSummary",
        back_populates="study_summary_id",
        # cascade="all, delete-orphan",
    )
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, insert_default=datetime.now, onupdate=datetime.now)

class QuestionSummary(Base):
    __tablename__ = "probe_question_summary"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    study_id = Column(Integer, ForeignKey("probe_study_summary.id"))
    qs_id = Column(Integer)
    question = Column(Text)
    summary = Column(JSON, default={})
    SPSS = Column(JSON, nullable=True)
    study_summary_id = relationship("StudySummary", back_populates="question_summaries")
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, insert_default=datetime.now, onupdate=datetime.now)

# ----- SURVEY -----
class Survey(Base):
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
    created_at = Column(DateTime, default=datetime.now)
    status = Column(Enum(StatusEnum), default=StatusEnum.draft)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, insert_default=datetime.utcnow, onupdate=datetime.now)

# ----- SURVEY QUESTION -----
class SurveyQuestion(Base):
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
    updated_at = Column(DateTime, insert_default=datetime.utcnow, onupdate=datetime.now)

# ----- SURVEY RESPONSE -----
class SurveyResponse(Base):
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
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, insert_default=datetime.now, onupdate=datetime.now)
    session_no = Column(Integer)

class SurveyResponseTest(Base):
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
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, insert_default=datetime.now, onupdate=datetime.now)
    session_no = Column(Integer)