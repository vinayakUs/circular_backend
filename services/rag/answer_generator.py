import logging

import openai

from config import Config
from ingestion.indexer.dto import SearchHit
from services.rag.citation_engine import (
    build_citation_query_engine,
    citation_nodes_to_citations,
    hits_to_nodes,
)
from services.rag.dto import RAGAnswer

# Network/infra failures worth another attempt. Auth, bad-request and
# validation errors are not — retrying them just delays the same verdict.
_TRANSIENT_LLM_ERRORS = (
    openai.APITimeoutError,
    openai.APIConnectionError,
    openai.RateLimitError,
    openai.InternalServerError,
)


class RAGAnswerGenerator:
    """Generates answers using LLM based on retrieved chunks."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.max_chunks = Config.RAG_MAX_CHUNKS
        self.max_tokens = Config.RAG_MAX_TOKENS

    def generate_answer(
        self,
        query: str,
        hits: list[SearchHit],
    ) -> RAGAnswer:
        """Generate an answer whose [n] markers map onto ``citations``."""
        if not hits:
            return RAGAnswer(
                answer="No relevant information found in the circulars.",
                citations=[],
            )

        # Limit chunks to avoid token overflow
        limited_hits = hits[: self.max_chunks]

        engine = build_citation_query_engine(hits_to_nodes(limited_hits))

        max_retries = 3
        last_error: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                self.logger.info(
                    "RAG: attempt=%d/%d calling LLM model=%s chunks=%d",
                    attempt + 1,
                    max_retries + 1,
                    Config.RAG_MODEL,
                    len(limited_hits),
                )
                response = engine.query(query)
                self.logger.info(
                    "RAG: raw LLM output (attempt=%d/%d):\n%s\n---",
                    attempt + 1,
                    max_retries + 1,
                    response.response or "",
                )
                citations = citation_nodes_to_citations(response.source_nodes)
                self.logger.info(
                    "RAG: attempt=%d/%d success answer_chars=%d citations=%d",
                    attempt + 1,
                    max_retries + 1,
                    len(response.response or ""),
                    len(citations),
                )
                return RAGAnswer(answer=response.response or "", citations=citations)
            except _TRANSIENT_LLM_ERRORS as e:
                last_error = e
                if attempt < max_retries:
                    self.logger.warning(
                        "RAG: attempt=%d/%d retryable error type=%s msg=%s",
                        attempt + 1,
                        max_retries + 1,
                        type(e).__name__,
                        str(e)[:300],
                    )
                else:
                    self.logger.error(
                        "RAG: attempt=%d/%d exhausted all retries type=%s msg=%s",
                        attempt + 1,
                        max_retries + 1,
                        type(e).__name__,
                        str(e)[:300],
                    )
                    raise
            except Exception as e:
                self.logger.error(
                    "RAG: attempt=%d/%d non-retryable error type=%s msg=%s",
                    attempt + 1,
                    max_retries + 1,
                    type(e).__name__,
                    str(e)[:300],
                )
                raise

        # Should not reach here, but raise last error if we do
        if last_error:
            raise last_error
