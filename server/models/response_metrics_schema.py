from typing import List
from pydantic import BaseModel, Field

class ImmediateEvaluation(BaseModel):
    """Metrics for immediate evaluation like relevance and gibberish"""

    relevance: int = Field(
        ...,
        ge=0,
        le=10,
        description="Relevance to original question (0-10): 0-3=Irrelevant, 4-5=Tangential, 6-7=Relevant, 8-10=Highly Relevant"
    )

    gibberish_score: int = Field(
        ...,
        ge=0,
        le=10,
        description="Compute a Gibberish Likelihood Score from 0 to 10 (inclusive) where 0 = meaningful natural language and 10 = garbled noise/corrupted text. Penalize high character randomness/entropy and meaningless repetition."
    )


class DetailedMetrics(BaseModel):
    """Detailed metrics for evaluating LLM response quality and characteristics"""
    
    quality: int = Field(
        ...,
        ge=1, 
        le=10,
        description="""
            Score the quality of the responses on a scale of (1 - 10) with 1 being a poor answer and 10 being an excellent answer.

            When scoring, you should factor in the following criteria:
            1. Relevance - how relevant are the respondent's answers to the questions?
            2. Depth of response - how much detail does the respondent provide? the more detail the better.
            3. Descriptiveness - how descriptive is the response? This can include emotional resonance.
            4. Value - how helpful is their response in providing actionable recommendations?
            5. Substance over surface - don't rate high just because the user has used any character name or proper nouns. Evaluate the actual content and insights provided.
            6. Semantic quality - don't rate high just because the user has used some/many words that are present in context, prompt, question, or any other reference material. Go with meaning and genuine understanding; don't let keyword matching or superficial alignment influence the score. Focus on whether the response demonstrates true comprehension and adds value.
        """
    )
    
    detail: int = Field(
        ...,
        ge=0,
        le=10,
        description="Level of elaboration (0-10): Penalizes repetition/rephrasing. Rewards unique insights and contextual depth"
    )
    
    confusion: int = Field(
        ...,
        ge=0,
        le=10,
        description="Confusion/Uncertainty detected (0-10): 0=Confident, 5=Moderately uncertain, 10=Completely confused"
    )
    
    negativity: int = Field(
        ...,
        ge=0,
        le=10,
        description="Negative sentiment strength (0-10): 0=Positive/Neutral, 5=Mild frustration, 10=Hostile/Sarcastic"
    )
    
    consistency: int = Field(
        ...,
        ge=0,
        le=10,
        description="Internal consistency (0-10): 0=Self-contradictory, 5=Partially consistent, 10=Fully coherent"
    )
    
    confidence: int = Field(
        ...,
        ge=0,
        le=10,
        description="Response confidence (0-10): 0=Hesitant/Uncertain, 5=Moderately confident, 10=Absolutely certain"
    )
    
    keywords: List[str] = Field(
        ...,
        min_items=1,
        description="Unique keywords/phrases capturing core insights (minimum 1 required)"
    )

    reason: str = Field(
        ...,
        description="Reason for awarding the quality score."
    )


class NSIGHT(DetailedMetrics, ImmediateEvaluation):
    """Combined Metrics for evaluating LLM response quality and characteristics"""
    pass

class NSIGHT_v2(NSIGHT):
    question: str = Field(
        ...,
        description="Follow up question to insitigate more elaborate and insightful responses"
    )

    response: str = Field(
        ...,
        description="The original response text being evaluated"
    )