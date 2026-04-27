from __future__ import annotations

import logging
from typing import Any
from pydantic import BaseModel

from config import Config
from utils.llm_providers import get_llm_provider


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

    def contextualize_chunks_wrt_full_doc(
        self,
        chunks: list[str],
        full_doc_text: str,
        circular_title: str,
        full_reference: str,
        max_doc_chars: int = 12000,
    ) -> list[ChunkContext]:
        """Generate contextual information for each chunk given the full document.

        Uses the full document text to provide better context for where each chunk
        fits within the overall document. Calls LLM in parallel for all chunks.

        Args:
            chunks: List of chunk texts
            full_doc_text: Full document text (used for context only)
            circular_title: Title of the circular
            full_reference: Full reference of the circular
            max_doc_chars: Maximum characters from full doc to include in prompt

        Returns:
            List of ChunkContext with contextual information
        """
        if not chunks:
            return []

        truncated_doc = self._truncate_text(full_doc_text, max_doc_chars)
        total_chunks = len(chunks)
        self.logger.info(
            "Generating contextual text for %d chunks in parallel (doc chars: %d, truncated to: %d)",
            total_chunks,
            len(full_doc_text),
            max_doc_chars,
        )

        # Build all prompts upfront
        prompts = [
            self._build_prompt_with_doc(
                chunk_text=chunk_text,
                chunk_index=i,
                total_chunks=total_chunks,
                full_doc_text=truncated_doc,
                circular_title=circular_title,
                full_reference=full_reference,
            )
            for i, chunk_text in enumerate(chunks)
        ]

        # Call LLM in parallel
        llm_client = get_llm_provider(Config.LLM_PROVIDER)
        responses = llm_client.create_completions_parallel(
            prompts=prompts,
            model=self.model,
            response_model=ContextResponse,
            max_workers=min(total_chunks, 8),
        )

        # Build ChunkContext list, handling failures
        contexts = []
        for i, chunk_text in enumerate(chunks):
            response = responses[i]
            if response is None:
                self.logger.warning(
                    "LLM call failed for chunk %d/%d, falling back to empty context",
                    i + 1,
                    total_chunks,
                )
                context = ""
            else:
                context = response.context
            contexts.append(ChunkContext(
                chunk_index=i,
                chunk_text=chunk_text,
                context=context,
            ))

        return contexts

    def _truncate_text(self, text: str, max_chars: int) -> str:
        """Truncate text to max characters, keeping start and end."""
        if len(text) <= max_chars:
            return text
        # Keep first 2/3 and last 1/3 to capture structure and end
        keep_start = int(max_chars * 2 / 3)
        keep_end = max_chars - keep_start
        return text[:keep_start] + "\n\n[... document continues ...]\n\n" + text[-keep_end:]

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
            llm_client = get_llm_provider(Config.LLM_PROVIDER)
            response = llm_client.get_client().chat.completions.create(
                model=self.model,
                response_model=ContextResponse,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
            )
            return response.context
        except Exception as e:
            self.logger.error("Context generation failed: %s", e)
            raise

    def _generate_context_with_doc(
        self,
        chunk_text: str,
        chunk_index: int,
        total_chunks: int,
        full_doc_text: str,
        circular_title: str,
        full_reference: str,
    ) -> str:
        """Generate contextual prefix for a chunk using the full document for context.

        Args:
            chunk_text: The chunk text
            chunk_index: Index of the chunk
            total_chunks: Total number of chunks
            full_doc_text: Full document text (truncated)
            circular_title: Title of the circular
            full_reference: Full reference of the circular

        Returns:
            Contextual prefix (50-100 tokens)
        """
        prompt = self._build_prompt_with_doc(
            chunk_text=chunk_text,
            chunk_index=chunk_index,
            total_chunks=total_chunks,
            full_doc_text=full_doc_text,
            circular_title=circular_title,
            full_reference=full_reference,
        )

        try:
            llm_client = get_llm_provider(Config.LLM_PROVIDER)
            self.logger.debug("Calling LLM for chunk %d, model=%s", chunk_index, self.model)
            response = llm_client.chat.completions.create(
                model=self.model,
                response_model=ContextResponse,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
            )
            self.logger.debug("LLM response received for chunk %d", chunk_index)
            return response.context
        except Exception as e:
            self.logger.error("Context generation with doc failed for chunk %d: %s", chunk_index, e)
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

    def _build_prompt_with_doc(
        self,
        chunk_text: str,
        chunk_index: int,
        total_chunks: int,
        full_doc_text: str,
        circular_title: str,
        full_reference: str,
    ) -> str:
        """Build the prompt for context generation using full document.

        Args:
            chunk_text: The chunk text
            chunk_index: Index of the chunk
            total_chunks: Total number of chunks
            full_doc_text: Full document text (truncated)
            circular_title: Title of the circular
            full_reference: Full reference of the circular

        Returns:
            Prompt string
        """
        return f"""<document>
{full_doc_text}
</document>

Here is the chunk we want to situate within the whole document:
<chunk>
{chunk_text}
</chunk>

You are a regulatory compliance expert. Please give a short succinct context to situate this chunk within the overall document for the purposes of improving search retrieval of the chunk.
Answer only with the succinct context and nothing else."""


def get_contextualizer() -> Contextualizer:
    """Get the shared contextualizer instance."""
    return Contextualizer()
