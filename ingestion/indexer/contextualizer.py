from __future__ import annotations

import logging
from typing import Any
from pydantic import BaseModel

from config import Config
from utils.llm_client import get_llm_client


logger = logging.getLogger(__name__)


class ContextResponse(BaseModel):
    """Response model for context generation."""

    context: str


class ChunkContext:
    """Contextual information for a chunk."""

    def __init__(
        self,
        chunk_index: int,
        chunk_text: str,
        context: str,
    ) -> None:
        self.chunk_index = chunk_index
        self.chunk_text = chunk_text
        self.context = context

    def get_contextualized_text(self) -> str:
        """Return the chunk text with contextual prefix."""
        return f"{self.context}\n\n{self.chunk_text}"


class Contextualizer:
    """Generates contextual information for document chunks."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.model = Config.ES_CONTEXTUAL_MODEL
        self.max_tokens = Config.ES_CONTEXTUAL_MAX_TOKENS

    def contextualize_chunks(
        self,
        chunks: list[str],
        circular_title: str,
        full_reference: str,
    ) -> list[ChunkContext]:
        """Generate contextual information for each chunk.

        Args:
            chunks: List of chunk texts
            circular_title: Title of the circular
            full_reference: Full reference of the circular

        Returns:
            List of ChunkContext with contextual information
        """
        if not chunks:
            return []

        total_chunks = len(chunks)
        contexts = []

        for i, chunk_text in enumerate(chunks):
            try:
                context = self._generate_context(
                    chunk_text=chunk_text,
                    chunk_index=i,
                    total_chunks=total_chunks,
                    circular_title=circular_title,
                    full_reference=full_reference,
                )
                contexts.append(
                    ChunkContext(
                        chunk_index=i,
                        chunk_text=chunk_text,
                        context=context,
                    )
                )
            except Exception as e:
                self.logger.error(
                    "Failed to generate context for chunk %d: %s", i, e
                )
                # Fall back to empty context
                contexts.append(
                    ChunkContext(
                        chunk_index=i,
                        chunk_text=chunk_text,
                        context="",
                    )
                )

        return contexts

    def _generate_context(
        self,
        chunk_text: str,
        chunk_index: int,
        total_chunks: int,
        circular_title: str,
        full_reference: str,
    ) -> str:
        """Generate contextual prefix for a single chunk.

        Args:
            chunk_text: The chunk text
            chunk_index: Index of the chunk
            total_chunks: Total number of chunks
            circular_title: Title of the circular
            full_reference: Full reference of the circular

        Returns:
            Contextual prefix (50-100 tokens)
        """
        prompt = self._build_prompt(
            chunk_text=chunk_text,
            chunk_index=chunk_index,
            total_chunks=total_chunks,
            circular_title=circular_title,
            full_reference=full_reference,
        )

        try:
            llm_client = get_llm_client()
            response = llm_client.chat.completions.create(
                model=self.model,
                response_model=ContextResponse,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
            )
            return response.context
        except Exception as e:
            self.logger.error("Context generation failed: %s", e)
            raise

    def _build_prompt(
        self,
        chunk_text: str,
        chunk_index: int,
        total_chunks: int,
        circular_title: str,
        full_reference: str,
    ) -> str:
        """Build the prompt for context generation.

        Args:
            chunk_text: The chunk text
            chunk_index: Index of the chunk
            total_chunks: Total number of chunks
            circular_title: Title of the circular
            full_reference: Full reference of the circular

        Returns:
            Prompt string
        """
        return f"""You are a regulatory compliance expert. Provide a short, succinct context (50-100 tokens)
to situate this chunk within the overall circular for the purposes of improving search retrieval.

Circular: {circular_title}
Full Reference: {full_reference}
Chunk {chunk_index + 1} of {total_chunks}: {chunk_text}

Context:"""


def get_contextualizer() -> Contextualizer:
    """Get the shared contextualizer instance."""
    return Contextualizer()
