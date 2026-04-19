from typing import Any
from pydantic import BaseModel, Field


class Citation(BaseModel):
    """A citation to a circular referenced in the answer."""

    circular_id: str = Field(..., description="The circular ID (e.g., NSE/CML/73791)")
    title: str = Field(..., description="Title of the circular")
    source: str = Field(..., description="Source of the circular (NSE or SEBI)")
    url: str = Field(..., description="URL to the circular")
    relevance_score: float = Field(..., description="Relevance score from search (0.0-1.0)")

    def to_dict(self) -> dict[str, Any]:
        return {
            "circular_id": self.circular_id,
            "title": self.title,
            "source": self.source,
            "url": self.url,
            "relevance_score": self.relevance_score,
        }


class RAGAnswer(BaseModel):
    """A generated answer with citations and snippets."""

    answer: str = Field(..., description="The AI-generated answer to the query")
    references: list[Citation] = Field(..., description="List of circulars referenced in the answer")
    snippets: list[str] = Field(..., description="Relevant text snippets from the circulars")
