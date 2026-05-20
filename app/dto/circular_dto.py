from datetime import date, datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_serializer


class SignatoryDTO(BaseModel):
    name: str
    designation: str


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
    applicable_to_nse: bool = False
    status: str
    url: Optional[str] = None
    signatories: List[SignatoryDTO] = []

    @field_serializer("issue_date")
    def serialize_issue_date(self, value: date) -> str:
        return value.isoformat()


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