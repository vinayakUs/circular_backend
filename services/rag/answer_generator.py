import logging
from typing import Any

from config import Config
from ingestion.indexer.dto import SearchHit
from services.rag.dto import Citation, RAGAnswer
from utils.llm_client import get_llm_client


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
        """Generate an answer with citations from retrieved chunks.

        Args:
            query: The user's search query
            hits: List of search hits from Elasticsearch

        Returns:
            RAGAnswer with answer, references, and snippets
        """
        if not hits:
            return RAGAnswer(
                answer="No relevant information found in the circulars.",
                references=[],
                snippets=[],
            )

        # Limit chunks to avoid token overflow
        limited_hits = hits[: self.max_chunks]

        # Build context from chunks
        context_chunks = self._build_context_chunks(limited_hits)

        # Build prompt
        prompt = self._build_prompt(query, context_chunks)

        try:
            llm_client = get_llm_client()
            response = llm_client.get_client().chat.completions.create(
                model=Config.RAG_MODEL,
                response_model=RAGAnswer,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
            )
            return response
        except Exception as e:
            self.logger.error("RAG generation failed: %s", e)
            raise

    def _build_context_chunks(self, hits: list[SearchHit]) -> list[dict[str, Any]]:
        """Build context chunks from search hits."""
        chunks = []
        for hit in hits:
            doc = hit.document
            chunks.append(
                {
                    "circular_id": doc.circular_id,
                    "title": doc.title,
                    "source": doc.source,
                    "url": doc.url,
                    "full_reference": doc.full_reference,
                    "chunk_text": doc.chunk_text,
                    "score": hit.score or 0.0,
                }
            )
        return chunks

    def _build_prompt(self, query: str, context_chunks: list[dict[str, Any]]) -> str:
        """Build the RAG prompt for the LLM."""
        context_text = "\n\n".join(
            [
                f"--- Circular: {chunk['circular_id']} ({chunk['source']}) ---\n"
                f"Title: {chunk['title']}\n"
                f"Reference: {chunk['full_reference']}\n"
                f"Content: {chunk['chunk_text']}"
                for chunk in context_chunks
            ]
        )

        return f"""You are a regulatory compliance assistant. Answer the user's question based ONLY on the provided circular excerpts.

Guidelines:
1. Answer the question comprehensively using information from the circulars
2. Cite specific circulars using their circular_id (e.g., NSE/CML/73791)
3. Extract relevant snippets that support your answer
4. Be factual and reference-based - do not hallucinate
5. If the information is not available in the provided context, say so
6. Format your answer clearly with bullet points or numbered lists where appropriate

User Question:
{query}

Relevant Circular Excerpts:
{context_text}

Provide your answer with:
- A clear, comprehensive response
- List of referenced circulars with their IDs, titles, sources, and URLs
- Relevant text snippets that support your answer
"""
