from datetime import date, datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_serializer


class CircularSummaryDTO(BaseModel):
    """Summary DTO for circular list items — basic fields only."""

    model_config = ConfigDict(str_strip_whitespace=True)

    id: UUID
    source: str
    circular_id: str
    full_reference: str
    department: Optional[str] = None
    title: str
    issue_date: date
    effective_date: Optional[date] = None
    status: str
    url: Optional[str] = None

    @field_serializer("issue_date")
    def serialize_issue_date(self, value: date) -> str:
        return value.isoformat()

    @field_serializer("effective_date")
    def serialize_effective_date(self, value: Optional[date]) -> Optional[str]:
        return value.isoformat() if value else None


class CircularListDataDTO(BaseModel):
    """Inner data container for circular list response."""
    circulars: List[CircularSummaryDTO]


class PaginationDTO(BaseModel):
    """Pagination metadata."""
    limit: int
    offset: int
    total: int
    hasNext: bool
    hasPrev: bool


class CircularListResponseDTO(BaseModel):
    """Paginated response for circular list endpoint."""
    data: CircularListDataDTO
    pagination: PaginationDTO