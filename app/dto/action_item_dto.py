"""API-layer DTOs for action items. Enriches the repository DTO with
backend-computed presentation fields (relative deadline).
"""
from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_serializer

from ingestion.repository.action_item_repository import ActionItemDTO


class EnrichedActionItemDTO(BaseModel):
    """Action item with backend-computed relative-deadline fields."""

    model_config = ConfigDict(str_strip_whitespace=True)

    id: UUID
    circular_id: UUID
    action_item: str
    deadline: Optional[date] = None
    priority: Optional[str] = None
    days_until_deadline: Optional[int] = None
    relative_deadline: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_repository(
        cls,
        item: ActionItemDTO,
        days_until_deadline: Optional[int],
        relative_deadline: Optional[str],
    ) -> "EnrichedActionItemDTO":
        return cls(
            id=item.id,
            circular_id=item.circular_id,
            action_item=item.action_item,
            deadline=item.deadline,
            priority=item.priority,
            days_until_deadline=days_until_deadline,
            relative_deadline=relative_deadline,
            created_at=item.created_at,
            updated_at=item.updated_at,
        )

    @field_serializer("created_at")
    def _ser_created_at(self, v: datetime) -> str:
        return v.isoformat()

    @field_serializer("updated_at")
    def _ser_updated_at(self, v: datetime) -> str:
        return v.isoformat()

    @field_serializer("deadline")
    def _ser_deadline(self, v: Optional[date]) -> Optional[str]:
        return v.isoformat() if v else None


class EnrichedActionItemListResponseDTO(BaseModel):
    """Paginated list response with enriched action items."""

    action_items: List[EnrichedActionItemDTO]
    total: int
    limit: int
    offset: int