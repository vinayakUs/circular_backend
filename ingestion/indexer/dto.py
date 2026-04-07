from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


@dataclass(slots=True)
class TextChunk:
    chunk_id: str
    chunk_index: int
    text: str


@dataclass(slots=True)
class IndexDocument:
    chunk_id: str
    circular_db_id: str
    circular_id: str
    source: str
    title: str
    department: str
    issue_date: date
    effective_date: date | None
    full_reference: str
    url: str
    pdf_url: str
    file_path: str
    content_hash: str | None
    chunk_index: int
    chunk_text: str
    indexed_at: datetime

    def to_es_body(self) -> dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "circular_db_id": self.circular_db_id,
            "circular_id": self.circular_id,
            "source": self.source,
            "title": self.title,
            "department": self.department,
            "issue_date": self.issue_date.isoformat(),
            "effective_date": (
                self.effective_date.isoformat() if self.effective_date is not None else None
            ),
            "full_reference": self.full_reference,
            "url": self.url,
            "pdf_url": self.pdf_url,
            "file_path": self.file_path,
            "content_hash": self.content_hash,
            "chunk_index": self.chunk_index,
            "chunk_text": self.chunk_text,
            "indexed_at": self.indexed_at.isoformat(),
        }
