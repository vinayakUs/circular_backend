from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_serializer


class CircularReferenceDTO(BaseModel):
    """DTO for circular reference response in API."""

    model_config = ConfigDict(str_strip_whitespace=True)

    id: UUID
    source_circular_id: UUID
    referenced_circular_id: str
    referenced_source: str  # 'NSE' or 'SEBI'
    referenced_full_ref: str
    relationship_nature: Optional[str] = None
    confidence_score: float
    extraction_method: str
    matched_text: Optional[str] = None
    referenced_circular_exists: bool = False
    created_at: datetime
    updated_at: datetime

    @field_serializer("created_at")
    def serialize_created_at(self, value: datetime) -> str:
        return value.isoformat()

    @field_serializer("updated_at")
    def serialize_updated_at(self, value: datetime) -> str:
        return value.isoformat()


class ExtractedReference(BaseModel):
    """A reference detected in circular text during extraction."""
    referenced_id: str
    referenced_source: str  # 'NSE' or 'SEBI'
    referenced_full_ref: str
    matched_text: str
    extraction_method: str = "regex"


class ReferenceRelationship(BaseModel):
    """LLM classification of reference relationship nature."""
    relationship_nature: str
    confidence: float


class ReferenceWithNature(BaseModel):
    """A reference paired with its LLM-classified relationship."""
    reference: ExtractedReference
    relationship: Optional[ReferenceRelationship] = None