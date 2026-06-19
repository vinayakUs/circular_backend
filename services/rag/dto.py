from pydantic import BaseModel


class RAGAnswer(BaseModel):
    """A generated answer with circular references."""

    answer: str = ""
    references: list[str] = []