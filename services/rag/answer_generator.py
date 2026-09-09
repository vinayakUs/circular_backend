import logging
from typing import Any

from config import Config
from ingestion.indexer.dto import SearchHit
from services.rag.dto import RAGAnswer, RAGGroupedAnswer
from utils.llm_providers import get_llm_provider

# Per-circular grouped answer constants (hardcoded — not in config.py)
MAX_CHUNKS_PER_GROUP = 5
GROUP_SUMMARY_MAX_TOKENS = 1900
MAX_PARALLEL_WORKERS = 8
PER_GROUP_PROMPT_MAX_CHARS = 22000


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

        max_retries = 3
        last_error: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                llm_client = get_llm_provider(Config.LLM_PROVIDER)
                self.logger.info(
                    "RAG: attempt=%d/%d calling LLM model=%s chunks=%d prompt_chars=%d",
                    attempt + 1,
                    max_retries + 1,
                    Config.RAG_MODEL,
                    len(limited_hits),
                    len(prompt),
                )
                response = llm_client.get_client().chat.completions.create(
                    model=Config.RAG_MODEL,
                    response_model=RAGAnswer,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=self.max_tokens,
                    max_retries=0,  # we handle retries ourselves
                )
                self.logger.info(
                    "RAG: attempt=%d/%d success answer_chars=%d refs=%d",
                    attempt + 1,
                    max_retries + 1,
                    len(response.answer) if response.answer else 0,
                    len(response.references),
                )
                self.logger.info("logger info ------ %s" , response)
                return response
            except Exception as e:
                last_error = e
                error_type = type(e).__name__
                error_msg = str(e)[:300]
                is_instructor_retry = "InstructorRetryException" in error_type or "incomplete" in error_msg.lower()
                if is_instructor_retry and attempt < max_retries:
                    self.logger.warning(
                        "RAG: attempt=%d/%d retryable error type=%s msg=%s",
                        attempt + 1,
                        max_retries + 1,
                        error_type,
                        error_msg,
                    )
                elif is_instructor_retry and attempt == max_retries:
                    self.logger.error(
                        "RAG: attempt=%d/%d exhausted all retries type=%s msg=%s",
                        attempt + 1,
                        max_retries + 1,
                        error_type,
                        error_msg,
                    )
                    raise
                else:
                    # Non-retryable error (e.g. auth, timeout, connection)
                    self.logger.error(
                        "RAG: attempt=%d/%d non-retryable error type=%s msg=%s",
                        attempt + 1,
                        max_retries + 1,
                        error_type,
                        error_msg,
                    )
                    raise

        # Should not reach here, but raise last error if we do
        if last_error:
            raise last_error

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
                    "chunk_text": doc.chunk_text,
                }
            )
        return chunks

    def _build_prompt(self, query: str, context_chunks: list[dict[str, Any]]) -> str:
        """Build the RAG prompt for the LLM."""
        context_text = "\n\n".join(
            [
                f"--- Circular: {chunk['circular_id']} ({chunk['source']}) ---\n"
                f"Title: {chunk['title']}\n"
                f"Content: {chunk['chunk_text']}"
                for chunk in context_chunks
            ]
        )

        return f"""
You are a regulatory compliance assistant.

Answer the user's question using ONLY the information provided in the circular excerpts below.

STRICT RULES:
1. Do not use outside knowledge.
2. Do not invent or assume facts.
3. Do not add information that is not present in the provided excerpts.
4. If the answer is not present in the excerpts, say:
   "The provided circular excerpts do not contain this information."
5. If only part of the answer is available, answer that part and clearly state what information is missing.
6. Preserve the exact meaning of the circulars. Do not change, broaden, or reinterpret requirements.
7. If circulars contain conflicting information, mention the conflict and identify the relevant circular IDs.
8. Use only circular IDs that actually support your answer.
9. Write in clear, professional English.
10. Do not make the answer unnecessarily verbose.
11. Prefer using points, bullets or numbered lists for clarity . Prefer using bold, Italics etc.
12. Do not give Duplicate circular no refrences.

IMPORTANT:
The circular excerpts are the ONLY source of truth.
Anything not explicitly supported by the excerpts must be treated as unknown.

USER QUESTION:
{query}

CIRCULAR EXCERPTS:
{context_text}

Return ONLY this format:

ANSWER:
<answer to the question>

REFERENCES:
<circular IDs supporting the answer>
"""


    # New Code for grouped answer generation


    # ── Per-circular grouped answer (parallel fan-out) ──────────────
    def generate_grouped_answer(
        self,
        query: str,
        hits: list[SearchHit],
    ) -> "RAGGroupedAnswer":
        """Group hits by circular and synthesize one brief summary per circular in parallel.
          Returns one entry per distinct circular, sorted by issue_date desc (newest first).
          Each entry carries only the LLM-generated per-circular summary.
        """

        if not hits:
            self.logger.info("RAG grouped: no hits, returning empty answer.")
            return RAGGroupedAnswer(results=[])
        
        from services.rag.dto import (
            CircularGroupSummary,
            RAGGroupedAnswer,
            _PerGroupSummary,
        )

        groups: dict[str, list[SearchHit]] = {}
        repr_by_group: dict[str, SearchHit] = {}

        for hit in hits:
            cid = hit.document.circular_db_id
            groups.setdefault(cid, []).append(hit)
            existing = repr_by_group.get(cid)
            if existing is None or (hit.score or 0.0) > (existing.score or 0.0):
                repr_by_group[cid] = hit

        ordered_cids = sorted(
            groups.keys(),
            key=lambda c: repr_by_group[c].document.issue_date,
            reverse=True,
        )


        # 2) Build one prompt per group, capped per-group chunks + char budget.
        prompts: list[str] = []
        for cid in ordered_cids:
            rep = repr_by_group[cid]
            doc = rep.document
            group_hits = sorted(
                groups[cid],
                key=lambda h: (h.score or 0.0),
                reverse=True,
            )[:MAX_CHUNKS_PER_GROUP]
            prompts.append(
                self._build_grouped_prompt(
                    query=query, doc=doc, group_hits=group_hits,
                )
            )

        # 3) Issue all group prompts in parallel.
        self.logger.info(
            "RAG grouped: groups=%d prompt_chars=%d max_workers=%d",
            len(prompts),
            sum(len(p) for p in prompts),
            MAX_PARALLEL_WORKERS,
        )
        parallel_results = self._call_parallel_grouped(prompts)


        # 4) Map index-aligned results back into CircularGroupSummary entries.
        results: list[CircularGroupSummary] = []
        for idx, cid in enumerate(ordered_cids):
            doc = repr_by_group[cid].document
            raw = parallel_results[idx] if idx < len(parallel_results) else None
            text = ""
            if raw is not None and getattr(raw, "summary", None):
                text = raw.summary.strip()
            if not text:
                text = "Summary unavailable for this circular."
            results.append(
                CircularGroupSummary(
                    circular_db_id=doc.circular_db_id,
                    circular_id=doc.circular_id,
                    full_reference=doc.full_reference,
                    title=doc.title,
                    source=doc.source,
                    department=doc.department or None,
                    issue_date=doc.issue_date,
                    url=doc.url or None,
                    applicable_to_nse=doc.applicable_to_nse,
                    summary=text,
                )
            )

        return RAGGroupedAnswer(results=results)


    def _call_parallel_grouped(self, prompts: list[str]) -> list[Any]:
        """Thin wrapper around the LLM provider's parallel helper (patched in tests)."""
        from services.rag.dto import _PerGroupSummary

        llm_provider = get_llm_provider(Config.LLM_PROVIDER)
        return llm_provider.create_completions_parallel(
            prompts=prompts,
            model=Config.RAG_MODEL,
            response_model=_PerGroupSummary,
            max_workers=min(MAX_PARALLEL_WORKERS, len(prompts)) if prompts else 1,
            max_retries=2,
            max_tokens=GROUP_SUMMARY_MAX_TOKENS,
        )
    
    def _build_grouped_prompt(
        self,
        query: str,
        doc: Any,
        group_hits: list[SearchHit],
    ) -> str:
        """Build a per-circular summary prompt. Excerpts are truncated by char budget."""
        excerpt_blocks: list[str] = []
        running = 0
        for i, hit in enumerate(group_hits, start=1):
            block = f"--- Excerpt {i} ---\n{hit.document.chunk_text or ''}"
            if running + len(block) > PER_GROUP_PROMPT_MAX_CHARS:
                break
            excerpt_blocks.append(block)
            running += len(block) + 2  # +2 for "\n\n" separator

        excerpts_text = "\n\n".join(excerpt_blocks)
        issue_d = (
            doc.issue_date.isoformat()
            if hasattr(doc.issue_date, "isoformat")
            else str(doc.issue_date)
        )

        return (
            f'You are a regulatory compliance assistant.\n\n'
            f'The user asked: "{query}"\n\n'
            f'Below are {len(excerpt_blocks)} excerpt(s) from circular '
            f'{doc.circular_id} ({doc.full_reference}), titled "{doc.title}", '
            f'issued on {issue_d} by {doc.source}.\n\n'
            f"Write a brief 1-3 sentence summary (max 80 words) explaining how "
            f"THIS circular addresses the user's question. Use ONLY information in the excerpts.\n\n"
            f"Strict rules:\n"
            f"- Do not invent facts.\n"
            f'- If the excerpts do not address the question, say "This circular does not '
            f'directly address the question." in one sentence.\n'
            f"- Preserve the exact meaning of the circular; do not broaden requirements.\n"
            f"- Write in clear, professional English; prefer short sentences or bullets.\n\n"
            f"CIRCULAR EXCERPTS:\n{excerpts_text}\n"
        )