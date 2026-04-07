from __future__ import annotations

import hashlib

from ingestion.indexer.dto import TextChunk


class FixedSizeChunker:
    """Simple deterministic text chunking for PDF content."""

    def __init__(self, chunk_size: int = 1200, overlap: int = 200) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if overlap < 0:
            raise ValueError("overlap must be non-negative")
        if overlap >= chunk_size:
            raise ValueError("overlap must be smaller than chunk_size")
        self.chunk_size = chunk_size
        self.overlap = overlap

    def chunk(self, text: str, *, circular_key: str) -> list[TextChunk]:
        normalized = " ".join(text.split()).strip()
        if not normalized:
            return []

        chunks: list[TextChunk] = []
        start = 0
        step = self.chunk_size - self.overlap
        while start < len(normalized):
            end = min(len(normalized), start + self.chunk_size)
            chunk_text = normalized[start:end].strip()
            if chunk_text:
                digest = hashlib.sha1(
                    f"{circular_key}:{len(chunks)}:{chunk_text}".encode("utf-8")
                ).hexdigest()
                chunks.append(
                    TextChunk(
                        chunk_id=f"{circular_key}:{len(chunks)}:{digest[:12]}",
                        chunk_index=len(chunks),
                        text=chunk_text,
                    )
                )
            if end >= len(normalized):
                break
            start += step
        return chunks
