from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from email.mime import text
from email.mime import text
from typing import Any

from utils import multi_snippet, highlight


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

    @classmethod
    def from_es_source(cls, source: dict[str, Any]) -> IndexDocument:
        return cls(
            chunk_id=source["chunk_id"],
            circular_db_id=source["circular_db_id"],
            circular_id=source["circular_id"],
            source=source["source"],
            title=source["title"],
            department=source["department"],
            issue_date=date.fromisoformat(source["issue_date"]),
            effective_date=(
                date.fromisoformat(source["effective_date"])
                if source.get("effective_date") is not None
                else None
            ),
            full_reference=source["full_reference"],
            url=source["url"],
            pdf_url=source["pdf_url"],
            file_path=source["file_path"],
            content_hash=source.get("content_hash"),
            chunk_index=source["chunk_index"],
            chunk_text=source["chunk_text"],
            indexed_at=datetime.fromisoformat(source["indexed_at"]),
        )

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


@dataclass(slots=True)
class SearchHit:
    es_id: str | None
    score: float | None
    document: IndexDocument

    def to_dict(self, query: str) -> dict[str, Any]:
        return {
            "id": self.es_id,
            "score": self.score,
            "preview": self.build_preview(query),
            "document": self.document.to_es_body(),
        }
    
    def build_preview(self, query: str) -> str:
        text = self.document.chunk_text or ""
        if not text:
            return ""

        snippets = multi_snippet(text, query)
        if snippets:
            # highlight the query terms in the snippets
            highlighted = [highlight(s, query) for s in snippets]
            return f"<div class='preview'>{' ... '.join(highlighted)}</div>"

        fallback = text[:200] + ("..." if len(text) > 200 else "")
        return f"<div class='preview'>{highlight(fallback, query)}</div>"