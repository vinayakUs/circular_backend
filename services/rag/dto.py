from pydantic import BaseModel
from datetime import date

class RAGAnswer(BaseModel):
    """A generated answer with circular references."""

    answer: str = ""
    references: list[str] = []

class CircularGroupSummary(BaseModel):
    circular_db_id: str
    circular_id: str        # human-readable id, e.g. "NSE/INSP/56789"
    full_reference: str
    title: str
    source: str
    department: str | None = None
    issue_date: date
    url: str | None = None
    applicable_to_nse: bool | None = None
    summary: str            # brief LLM-generated summary (≤3 sentences)

class RAGGroupedAnswer(BaseModel):
    results: list[CircularGroupSummary] = []

class _PerGroupSummary(BaseModel):           # only used as response_model for instructor
    summary: str