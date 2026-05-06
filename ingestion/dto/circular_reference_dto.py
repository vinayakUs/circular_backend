from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_serializer


class CircularReferenceDTO(BaseModel):
    """DTO for circular reference response in API."""

    model_config = ConfigDict(str_strip_whitespace=True)

    id: UUID
    source_circular_id: UUID
    reference_circular_no: str
    reference_circular_id: Optional[UUID] = None
    relationship_nature: str
    ref_circular_exist: bool = False
    created_at: datetime
    updated_at: datetime

    @field_serializer("created_at")
    def serialize_created_at(self, value: datetime) -> str:
        return value.isoformat()

    @field_serializer("updated_at")
    def serialize_updated_at(self, value: datetime) -> str:
        return value.isoformat()