"""
  /root/circular_backend/ingestion/processor/document_summarizer.py
  Document summarization processor using LangChain.
  python -m ingestion.processor.document_summarizer --circular-id CMPT72078
"""

import logging
import sys
from functools import lru_cache
from typing import Any

try:
    from instructor.core import InstructorRetryException
except ImportError:  # pragma: no cover — fallback for older instructor installs
    from instructor.exceptions import InstructorRetryException

from langchain_classic.chains.summarize import load_summarize_chain
from langchain_core.documents import Document
from langchain_core.prompts import PromptTemplate
from transformers import AutoTokenizer

from config import Config
from db.postgres_client import get_postgres_client
from ingestion.indexer.pdf_extractor import PDFTextExtractor
from ingestion.processor.base import BaseProcessor
from ingestion.repository.asset_repository import AssetRepository
from ingestion.repository.circular_repository import CircularRecord, CircularRepository
from ingestion.repository.summary_repository import SummaryRepository
from utils.llm_providers.langchain_adapter import LangChainLLMAdapter

# Max chars per chunk before splitting (leave room for prompt overhead)
MAX_CHUNK_CHARS = 5000

# Map prompt for map_reduce
MAP_PROMPT = PromptTemplate(
    input_variables=["text"],
    template="""Write a concise paragraph summarizing this section. Focus on: factual information about what the regulation requires, who it applies to, and key obligations or deadlines. For tables: extract only aggregate counts or totals (e.g., "the list contains 45 securities across 6 categories"). Do not list individual table entries. Ignore UI navigation steps, procedural instructions, and SOPs. Do not include any contact information, email addresses, phone numbers, or references to help desk/FAQ/exchange contacts. Write 2-4 sentences in plain paragraph format without headers, bullet points, or tables.

IMPORTANT: You MUST wrap the following in **bold** in your output: key entities (like names of regulations, frameworks, segments), obligations, deadlines (dates), and important values or thresholds. Example: "**minimum 100% margin**", "**January 6, 2026**", "**Trade-for-Trade**"

Text:
{text}""",
)

# Reduce prompt to combine chunk summaries
REDUCE_PROMPT = PromptTemplate(
    input_variables=["text"],
    template="""Combine these section summaries into a single cohesive paragraph. Write in plain paragraph format without headers, bullet points, or tables. Focus on: what the regulation requires, who it applies to, key compliance obligations, and important deadlines. For any tables or lists mentioned, include only aggregate counts or totals (e.g., "covers 30 securities across 4 categories"). Do not include individual entries. Do not include any contact information, email addresses, phone numbers, or references to help desk/FAQ/exchange contacts. Ignore procedural steps and UI instructions.

IMPORTANT: You MUST wrap the following in **bold** in your output: key entities (like names of regulations, frameworks, segments), obligations, deadlines (dates), and important values or thresholds. Example: "**minimum 100% margin**", "**January 6, 2026**", "**Trade-for-Trade**"

Summaries:
{text}""",
)

# Stuff prompt for small documents
SUMMARY_PROMPT = PromptTemplate(
    input_variables=["text"],
    template="""You are a regulatory compliance assistant. Summarize the following document as a single cohesive paragraph. Write in plain prose without headers, bullet points, tables, or lists. Focus on: what the regulation requires, who it applies to, key compliance obligations and deadlines, and important entities or thresholds. For any tables or data sets, include only aggregate statistics (e.g., "covers 45 securities across 6 categories" or "lists 12 items with values ranging from X to Y"). Do not enumerate individual table entries. Do not include any contact information, email addresses, phone numbers, or references to help desk/FAQ/exchange contacts. Ignore UI steps, navigation instructions, SOPs, and procedural how-to guides. Be precise and factual.

IMPORTANT: You MUST wrap the following in **bold** in your output: key entities (like names of regulations, frameworks, segments), obligations, deadlines (dates), and important values or thresholds. Example: "**minimum 100% margin**", "**January 6, 2026**", "**Trade-for-Trade**"

Document:
{text}""",
)


@lru_cache(maxsize=1)
def _load_bge_tokenizer():
    """Load the BGE tokenizer from the local HF cache (offline)."""
    return AutoTokenizer.from_pretrained(
        Config.ES_EMBEDDING_MODEL_NAME,
        local_files_only=True,
    )


def _bge_token_ids(text: str) -> list[int]:
    """Token counter backed by the BGE tokenizer for LangChain chain sizing."""
    if not text:
        return []
    return _load_bge_tokenizer().encode(text, add_special_tokens=False)


class DocumentSummarizerProcessor(BaseProcessor):
    """Processor to generate concise summaries of regulatory documents using LangChain."""

    def __init__(self, db_pool: Any):
        super().__init__(db_pool)
        self.asset_repo = AssetRepository(db_pool)
        self.summary_repo = SummaryRepository(db_pool)
        self.circular_repo = CircularRepository(db_pool)

    @property
    def name(self) -> str:
        return "document_summarizer"

    def run(self, record: CircularRecord) -> bool:
        """Override to restrict to last-24h circulars only."""
        from datetime import datetime, timezone, timedelta

        cutoff = datetime.now(timezone.utc).date()
        yesterday = cutoff - timedelta(days=1)
        if record.issue_date and record.issue_date < yesterday:
            self.logger.info(
                "Skipping circular_id=%s issue_date=%s (older than 24h)",
                record.id, record.issue_date
            )
            # Mark as completed so it's not retried
            self.processor_repo.mark_task_completed(record.id, self.name)
            return True

        return super().run(record)

    def process(self, record: CircularRecord) -> None:
        # Get PDF path from circular_assets table (new architecture)
        file_path = None
        assets = self.asset_repo.list_assets(record.id)
        for role in ["original_pdf","extracted_pdf"]:
            for asset in assets:
                if asset.asset_role == role and asset.file_path and asset.file_path.lower().endswith(".pdf"):
                    file_path = asset.file_path
                    break
            if file_path:
                break
        
        self.logger.info("Using PDF file: %s", file_path)


        # Fallback to pdf_url from circulars table
        # if not file_path and record.pdf_url:
        #     file_path = record.pdf_url

        if not file_path:
            raise ValueError(f"No PDF file path found for circular: {record.circular_id}")

        # Extract text from PDF
        extractor = PDFTextExtractor()
        try:
            text = extractor.extract(file_path)
        except Exception as e:
            raise RuntimeError(f"Failed to extract text from PDF at {file_path}: {e}")

        if not text.strip():
            raise ValueError("Extracted text from PDF is empty.")

        # Build LangChain adapter. We supply a custom token counter backed by
        # the locally-cached BGE embedding tokenizer so LangChain's internal
        # token counting (used by map_reduce's collapse pre-check) never tries
        # to download GPT-2 from HuggingFace at runtime.
        llm_adapter = LangChainLLMAdapter(
            provider_name=Config.LLM_PROVIDER,
            model_name=Config.SUMMARIZATION_MODEL,
            custom_get_token_ids=_bge_token_ids,
        )

        # Use map_reduce for large documents, stuff for small ones
        if len(text) > MAX_CHUNK_CHARS:
            # Split into chunks for map_reduce
            chunks = [text[i : i + MAX_CHUNK_CHARS] for i in range(0, len(text), MAX_CHUNK_CHARS)]
            self.logger.info(
                "Using map_reduce for large document: circular_id=%s total_chars=%d max_chunk_chars=%d num_chunks=%d",
                record.circular_id,
                len(text),
                MAX_CHUNK_CHARS,
                len(chunks),
            )
            chain = load_summarize_chain(
                llm=llm_adapter,
                chain_type="map_reduce",
                map_prompt=MAP_PROMPT,
                combine_prompt=REDUCE_PROMPT,
                verbose=False,
            )
            docs = [Document(page_content=chunk) for chunk in chunks]
        else:
            self.logger.info(
                "Using stuff chain for small document: circular_id=%s total_chars=%d",
                record.circular_id,
                len(text),
            )
            chain = load_summarize_chain(
                llm=llm_adapter,
                chain_type="stuff",
                prompt=SUMMARY_PROMPT,
                verbose=False,
            )
            docs = [Document(page_content=text)]

        try:
            summary = chain.invoke(docs)
            summary_text = summary.get("output_text", str(summary))
        except InstructorRetryException:
            # All LLM retries exhausted — propagate so BaseProcessor.run records
            # FAILED in processing_tasks and the next pipeline run retries once
            # the LLM service recovers. No summary is persisted in this case.
            self.logger.error(
                "metric=document_summarizer_llm_failure circular_id=%s — LLM retries exhausted",
                record.id,
            )
            raise
        except Exception as e:
            self.logger.error(
                "metric=document_summarizer_unknown_error circular_id=%s error=%s",
                record.id, e,
            )
            raise RuntimeError(f"LangChain summarization failed: {e}") from e

        # Defense-in-depth: reject placeholder strings that the adapter or chain
        # could produce from a None response (e.g. str(None) == "None").
        stripped = summary_text.strip() if isinstance(summary_text, str) else ""
        if not stripped or stripped.lower() in ("none", "null", "nil", "{}"):
            self.logger.error(
                "metric=document_summarizer_empty_summary circular_id=%s summary_text=%r",
                record.id, summary_text,
            )
            raise RuntimeError(f"Summarization returned empty/invalid result: {summary_text!r}")

        summary_text = stripped
        self.logger.info("Generated summary for circular_id=%s, length=%s", record.id, len(summary_text))
        self.logger.info("Summary text: %s", summary_text)
        # Idempotent: delete old summary and upload new one
        self.summary_repo.delete_summary_for_circular(record.id)
        self.summary_repo.upload_and_store_summary(
            circular_id=record.id,
            source=record.source,
            source_item_key=record.source_item_key,
            issue_date=record.issue_date,
            summary_text=summary_text,
        )

        self.logger.info("Stored summary for circular_id=%s", record.id)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run the document summarizer for a specific circular.")
    parser.add_argument(
        "--circular-id",
        type=str,
        required=True,
        help="Process a specific circular by its circular_id (e.g. CMPT72078)"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    pool = get_postgres_client().get_pool()

    repo = CircularRepository(pool)
    record = repo.get_record_by_circular_id(args.circular_id)
    if not record:
        logger.error("Circular not found: %s", args.circular_id)
        sys.exit(1)

    processor = DocumentSummarizerProcessor(pool)
    success = processor.run(record)
    logger.info("Result: %s", "SUCCESS" if success else "FAILED")
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
