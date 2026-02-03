from typing import List
from pydantic import BaseModel, Field
from modules.LLMAdapter import LLMAdapter
from langchain_core.prompts import PromptTemplate

class NSIGHT(BaseModel):
    """Metrics for evaluating LLM response quality and characteristics"""
    
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
    
    relevance: int = Field(
        ...,
        ge=0,
        le=10,
        description="Relevance to original question (0-10): 0-3=Irrelevant, 4-5=Tangential, 6-7=Relevant, 8-10=Highly Relevant"
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
    
    negativity: int = Field(  # Renamed from "annoyed"
        ...,
        ge=0,
        le=10,
        description="Negative sentiment strength (0-10): 0=Positive/Neutral, 5=Mild frustration, 10=Hostile/Sarcastic"
    )
    
    consistency: int = Field(  # Renamed and inverted from "contradicting"
        ...,
        ge=0,
        le=10,
        description="Internal consistency (0-10): 0=Self-contradictory, 5=Partially consistent, 10=Fully coherent"
    )
    
    confidence: int = Field(  # Renamed and inverted from "hesitation"
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

    gibberish_score: int = Field(
        ...,
        ge=0,
        le=10,
        description="""
            Task:
            Compute a Gibberish Likelihood Score from 0 to 10 (inclusive).

            Scale definition:
                - 0 = clearly meaningful natural language or clearly intentional/valid code or logs.
                - 10 = almost certainly gibberish, garbled noise, or corrupted text.
                Do NOT use a 0–100 scale.

            Method:
            Compute the score as a calibrated confidence estimate by combining multiple independent cues.
            Do NOT rely on a single clue.

            Primary scoring cues (increase score when present):
                1. High character-pattern randomness / entropy indicating corruption or noise.
                2. Very low alphabetic-character ratio relative to total content.
                3. Very low valid-word ratio (dictionary-like validity).
                4. Implausible character transitions or n-gram sequences.
                5. Repeated symbols, words, or phrases without semantic progression.

            Anchors (select the band first, then fine-tune within it):
                - 0–1: Fully coherent text or clearly intentional code/logs; typos or slang allowed.
                - 1–3: Mostly coherent with minor noise or small corruption.
                - 3–6: Mixed or ambiguous; intent partially recoverable; many malformed words.
                - 6–8: Largely nonsensical; few valid words; heavy noise patterns or repetition.
                - 8–10: Near-certain gibberish; random characters, encoding artifacts, or meaningless repetition.

            Hard constraints:
                - If valid-word ratio < 30% AND entropy is high → score MUST be ≥ 6.
                - Repeated words, phrases, or symbol runs without semantic progression → score MUST be ≥ 6.
                - Isolated valid words do NOT imply meaningful text.
                - When cues conflict, prioritize entropy and sequence plausibility over the presence of real words.

            Exclusions (do NOT lower the score solely due to these):
                - Normal spelling mistakes
                - Code-switching
                - Domain-specific jargon
                - Short or concise replies
            These exclusions apply ONLY if sentence-level meaning is clearly recoverable.

            Output:
            Return ONLY a single numeric score from 0 to 10. No explanation.
        """
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