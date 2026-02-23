from typing import List
from pydantic import BaseModel, Field
from services.LLMAdapter import LLMAdapter
from langchain_core.prompts import PromptTemplate

class NSIGHT(BaseModel):
    """Metrics for evaluating LLM response quality and characteristics"""
    model_config = {"extra": "allow"}
    
    relevance: int = Field(
        default=0, ge=0, le=10,
        description="Relevance to original question (0-10): 0-3=Irrelevant, 4-5=Tangential, 6-7=Relevant, 8-10=Highly Relevant"
    )
    quality: int = Field(
        default=1, ge=1, le=10,
        description="Score the quality (1-10): factors in relevance, depth, descriptiveness, and value."
    )
    detail: int = Field(
        default=0, ge=0, le=10,
        description="Level of elaboration (0-10): Penalizes repetition/rephrasing. Rewards unique insights."
    )
    confusion: int = Field(
        default=0, ge=0, le=10,
        description="Confusion/Uncertainty detected (0-10): 0=Confident, 10=Completely confused"
    )
    negativity: int = Field(
        default=0, ge=0, le=10,
        description="Negative sentiment strength (0-10): 0=Positive/Neutral, 10=Hostile/Sarcastic"
    )
    consistency: int = Field(
        default=10, ge=0, le=10,
        description="Internal consistency (0-10): 0=Self-contradictory, 10=Fully coherent"
    )
    confidence: int = Field(
        default=5, ge=0, le=10,
        description="Response confidence (0-10): 0=Hesitant/Uncertain, 10=Absolutely certain"
    )
    keywords: List[str] = Field(
        default_factory=list,
        description="Unique keywords/phrases capturing core insights"
    )
    reason: str = Field(
        default="",
        description="Reason for awarding the quality score."
    )
    gibberish_score: int = Field(
        default=0, ge=0, le=10,
        description="Gibberish Likelihood Score (0-10): 0=clearly meaningful, 10=almost certainly gibberish/noise."
    )

class NSIGHT_v2(NSIGHT):
    question: str = Field(
        ...,
        description="Follow up question to insitigate more elaborate and insightful responses"
    )

    response: str = Field(
        ...,
        description="The original response text being evaluated"
    )