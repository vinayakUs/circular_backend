from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod
from typing import List
from ingestion.indexer.dto import TextChunk
from ingestion.indexer.pdf_extractor import Block

# Interface for chunking strategies that can be swapped out as needed. For example, we could implement a more advanced semantic chunker in the future.
class ChunkingStrategy(ABC):
    """Abstract contract for text chunking strategies."""

    @abstractmethod
    def chunk(self, text: str, *, circular_key: str) -> list[TextChunk]:
        """
        Split text into semantic chunks.

        Args:
            text: Raw text content to chunk
            circular_key: Unique identifier for the source document

        Returns:
            List of TextChunk objects with chunk_id, chunk_index, and text
        """
        pass

    @property
    @abstractmethod
    def chunk_size(self) -> int:
        """Target size for each chunk in characters."""
        pass

    @property
    @abstractmethod
    def overlap(self) -> int:
        """Overlap between chunks in characters."""
        pass


# Early iteration of a simple chunker that splits text into fixed-size overlapping chunks.
class FixedSizeChunker(ChunkingStrategy):
    """Simple deterministic text chunking for PDF content."""

    def __init__(self, chunk_size: int = 1200, overlap: int = 200) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if overlap < 0:
            raise ValueError("overlap must be non-negative")
        if overlap >= chunk_size:
            raise ValueError("overlap must be smaller than chunk_size")
        self._chunk_size = chunk_size
        self._overlap = overlap

    @property
    def chunk_size(self) -> int:
        return self._chunk_size 
    @property
    def overlap(self) -> int:
        return self._overlap   

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


class ParagraphSentenceChunker(ChunkingStrategy):
    """Sentence and paragraph-aware text chunking for regulatory documents."""

    def __init__(
        self,
        chunk_size: int = 600,
        overlap: int = 80,
        min_chunk_size: int = 200,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")

        if overlap < 0:
            raise ValueError("overlap must be non-negative")

        if overlap >= chunk_size:
            raise ValueError("overlap must be smaller than chunk_size")

        if min_chunk_size <= 0:
            raise ValueError("min_chunk_size must be positive")

        self._chunk_size = chunk_size
        self._overlap = overlap
        self.min_chunk_size = min_chunk_size

    @property
    def chunk_size(self) -> int:
        return self._chunk_size
    
    @property
    def overlap(self) -> int:
        return self._overlap

    def chunk(self, text: str, *, circular_key: str) -> list[TextChunk]:
        if not text or not text.strip():
            return []

        normalized = text.strip()
        chunks: list[TextChunk] = []
        current_chunk = ""

        # Split by paragraph boundaries first
        paragraphs = re.split(r"\n\s*\n+", normalized)

        for para in paragraphs:
            para = para.strip()

            if not para:
                continue

            # If paragraph itself exceeds chunk_size, split into sentences
            if len(para) > self.chunk_size:

                # Save current chunk before processing long paragraph
                if current_chunk:
                    chunks.append(self._create_chunk(current_chunk, circular_key, len(chunks)))

                    # Apply overlap from end of previous chunk
                    overlap_len = min(self.overlap, len(current_chunk) // 2)
                    current_chunk = current_chunk[-overlap_len:] if overlap_len > 0 else ""

                # Split long paragraph into sentences
                sentences = self._split_into_sentences(para)

                for sentence in sentences:
                    sentence = sentence.strip()

                    if not sentence:
                        continue

                    # Very long sentence - split by words as last resort
                    if len(sentence) > self.chunk_size:

                        if current_chunk:
                            chunks.append(self._create_chunk(current_chunk, circular_key, len(chunks)))

                            overlap_len = min(self.overlap, len(current_chunk) // 2)
                            current_chunk = current_chunk[-overlap_len:] if overlap_len > 0 else ""

                        words = sentence.split()
                        temp = ""

                        for word in words:
                            if len(temp) + len(word) + 1 > self.chunk_size:
                                if temp:
                                    chunks.append(self._create_chunk(temp, circular_key, len(chunks)))

                                temp = word
                            else:
                                temp = (temp + " " + word).strip()

                        current_chunk = temp

                    elif len(current_chunk) + len(sentence) + 1 > self.chunk_size:
                        chunks.append(self._create_chunk(current_chunk, circular_key, len(chunks)))

                        overlap_len = min(self.overlap, len(current_chunk) // 2)
                        current_chunk = current_chunk[-overlap_len:] if overlap_len > 0 else ""

                        current_chunk = (
                            (current_chunk + " " + sentence).strip()
                            if current_chunk
                            else sentence
                        )

                    else:
                        current_chunk = (
                            (current_chunk + " " + sentence).strip()
                            if current_chunk
                            else sentence
                        )

            elif len(current_chunk) + len(para) + 1 > self.chunk_size:
                chunks.append(self._create_chunk(current_chunk, circular_key, len(chunks)))

                overlap_len = min(self.overlap, len(current_chunk) // 2)
                current_chunk = current_chunk[-overlap_len:] if overlap_len > 0 else ""

                current_chunk = (
                    (current_chunk + " " + para).strip()
                    if current_chunk
                    else para
                )

            else:
                current_chunk = (
                    (current_chunk + " " + para).strip()
                    if current_chunk
                    else para
                )

        # Don't forget remaining content
        if current_chunk:
            chunks.append(self._create_chunk(current_chunk, circular_key, len(chunks)))

        return chunks

    def _split_into_sentences(self, text: str) -> list[str]:
        """Split text by sentence-ending punctuation followed by a capital letter.

        Only splits on . ! ? when followed by whitespace + capital letter (new sentence).
        This avoids splitting on abbreviations like "ANNEXURE 1." or "E.g."
        """
        # Only split on sentence endings when followed by capital letter (new sentence)
        sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text)
        return [s.strip() for s in sentences if s.strip()]

    def _create_chunk(
        self,
        text: str,
        circular_key: str,
        index: int,
    ) -> TextChunk:
        """Create a TextChunk with unique ID."""

        digest = hashlib.sha1(
            f"{circular_key}:{index}:{text}".encode("utf-8")
        ).hexdigest()

        return TextChunk(
            chunk_id=f"{circular_key}:{index}:{digest[:12]}",
            chunk_index=index,
            text=text.strip(),
        )


# ---------------------------------------------------------------------------
# NSE Circular PDF Chunker
# ---------------------------------------------------------------------------

class NSEPdfChunkingStrategy:
    """
    PDF-aware chunker for NSE circulars.

    Works with pre-extracted blocks from PDFPlumberExtractor.
    Tables are kept atomic — never split mid-table.
    """

    def __init__(self, chunk_size: int = 1000, overlap: int = 100):
        self._chunk_size = chunk_size
        self._overlap = overlap

    @property
    def chunk_size(self) -> int:
        return self._chunk_size

    @property
    def overlap(self) -> int:
        return self._overlap

    def chunk(self, blocks: list[Block], *, circular_key: str) -> list[TextChunk]:
        """Pack pre-extracted blocks into chunks."""
        raw_chunks = self._pack_into_chunks(blocks)
        overlapped = self._apply_overlap(raw_chunks)

        return [
            TextChunk(
                chunk_id=f"{circular_key}_{i}",
                chunk_index=i,
                text=chunk.strip(),
            )
            for i, chunk in enumerate(overlapped)
            if chunk.strip()
        ]

    def _pack_into_chunks(self, blocks: list[Block]) -> list[str]:
        chunks: list[str] = []
        buffer: list[str] = []
        buf_len = 0

        def flush():
            nonlocal buffer, buf_len
            if buffer:
                chunks.append("\n\n".join(buffer))
                buffer, buf_len = [], 0

        for block in blocks:
            content = block.content
            clen = len(content)

            if block.kind == "table":
                if buf_len + clen > self._chunk_size and buffer:
                    flush()
                if clen > self._chunk_size:
                    flush()
                    chunks.append(content)
                else:
                    buffer.append(content)
                    buf_len += clen
            else:
                if clen > self._chunk_size:
                    flush()
                    for sub in self._split_prose(content):
                        chunks.append(sub)
                elif buf_len + clen > self._chunk_size:
                    flush()
                    buffer.append(content)
                    buf_len = clen
                else:
                    buffer.append(content)
                    buf_len += clen

        flush()
        return chunks

    def _split_prose(self, text: str) -> list[str]:
        paragraphs = re.split(r'\n\s*\n', text)
        chunks: list[str] = []
        buffer: list[str] = []
        buf_len = 0

        def flush_buf():
            nonlocal buffer, buf_len
            if buffer:
                chunks.append(" ".join(buffer))
                buffer, buf_len = [], 0

        for para in paragraphs:
            sentences = re.split(r'(?<=[.!?])\s+', para.strip())
            for sentence in sentences:
                if len(sentence) > self._chunk_size:
                    flush_buf()
                    for word in sentence.split():
                        if buf_len + len(word) + 1 > self._chunk_size:
                            flush_buf()
                        buffer.append(word)
                        buf_len += len(word) + 1
                    flush_buf()
                elif buf_len + len(sentence) > self._chunk_size:
                    flush_buf()
                    buffer.append(sentence)
                    buf_len = len(sentence)
                else:
                    buffer.append(sentence)
                    buf_len += len(sentence)

        flush_buf()
        return chunks

    def _apply_overlap(self, chunks: list[str]) -> list[str]:
        if self._overlap == 0 or len(chunks) < 2:
            return chunks

        result = [chunks[0]]
        for i in range(1, len(chunks)):
            prev = chunks[i - 1]
            tail = prev[-self._overlap:]
            boundary = tail.find(" ")
            tail = tail[boundary + 1:] if boundary != -1 else tail
            result.append(tail + "\n\n" + chunks[i])

        return result
