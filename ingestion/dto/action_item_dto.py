from datetime import date, datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_serializer


class ActionItemDTO(BaseModel):
    """DTO for action item response in API."""

    model_config = ConfigDict(str_strip_whitespace=True)

    id: UUID
    circular_id: UUID
    action_item: str
    deadline: Optional[date] = None
    priority: Optional[str] = None
    persona: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    @field_serializer("created_at")
    def serialize_created_at(self, value: datetime) -> str:
        return value.isoformat()

    @field_serializer("updated_at")
    def serialize_updated_at(self, value: datetime) -> str:
        return value.isoformat()

    @field_serializer("deadline")
    def serialize_deadline(self, value: Optional[date]) -> Optional[str]:
        return value.isoformat() if value else None


class ActionItemListResponseDTO(BaseModel):
    """DTO for paginated action items list response."""

    action_items: List[ActionItemDTO]
    total: int
    limit: int
    offset: int