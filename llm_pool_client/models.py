"""Shared Pydantic response models for the LLM pool.

Import these in your processor code to ensure the server can resolve them.
"""

from pydantic import BaseModel, Field


class AnswerResponse(BaseModel):
    answer: str


class NSEApplicabilityResponse(BaseModel):
    applicable: bool = Field(description="YES if applicable, NO if not")
    reason: str | None = Field(default=None, description="Brief explanation")
    confidence: float = Field(description="Confidence score between 0 and 1")


class MarketAnalysisResponse(BaseModel):
    sentiment: str = Field(description="bullish, bearish, or neutral")
    key_factors: list[str] = Field(description="3-5 key observations")
    risk_level: str = Field(description="low, medium, or high")
    confidence: float = Field(description="Confidence score between 0 and 1")
