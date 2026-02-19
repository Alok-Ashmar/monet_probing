from sqlalchemy import Column, Integer, String, JSON, ForeignKey, Text
from sqlalchemy.orm import relationship
from db.base import Base
from datetime import datetime

class StudySummary(Base):
    __tablename__ = "study_summaries"

    id = Column(Integer, primary_key=True, index=True)
    study_id = Column(Integer, index=True)
    cnt_id = Column(Integer, nullable=True)
    cnt_name = Column(String, nullable=True)
    overall_summary = Column(JSON, default={})
    processing_method = Column(String, nullable=True)
    response_count = Column(Integer, default=0)
    created_at = Column(Text, default=str(datetime.utcnow()))

    question_summaries = relationship("QuestionSummary", back_populates="study", cascade="all, delete-orphan")

class QuestionSummary(Base):
    __tablename__ = "question_summaries"

    id = Column(Integer, primary_key=True, index=True)
    study_summary_id = Column(Integer, ForeignKey("study_summaries.id"))
    qs_id = Column(Integer, nullable=True)
    question = Column(Text)
    summary = Column(JSON, default={})
    SPSS = Column(JSON, nullable=True)

    study = relationship("StudySummary", back_populates="question_summaries")
