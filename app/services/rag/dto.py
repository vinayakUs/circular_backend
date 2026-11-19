from pydantic import BaseModel


class Citation(BaseModel):
    id: int
    circular_id: str
    circular_db_id: str | None = None
    full_reference: str | None = None
    title: str
    source: str | None = None
    url: str | None = None
    chunk_id: str | None = None
    chunk_index: int | None = None
    text: str = ""
    score: float | None = None


class RAGAnswer(BaseModel):
    """A generated answer with inline [n] markers and matching sources."""

    answer: str = ""
    citations: list[Citation] = []
