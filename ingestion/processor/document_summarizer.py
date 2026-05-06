"""
  Document summarization processor using LangChain.
  python -m ingestion.processor.document_summarizer --circular-id CMPT72078
"""

import logging
import sys
from typing import Any

from langchain.chains.summarize import load_summarize_chain
from langchain_core.documents import Document
from langchain_core.prompts import PromptTemplate

from config import Config
from ingestion.indexer.pdf_extractor import PDFTextExtractor
from ingestion.processor.base import BaseProcessor
from ingestion.repository.circular_repository import CircularRecord, CircularRepository
from ingestion.repository.summary_repository import SummaryRepository
from utils.llm_providers.langchain_adapter import LangChainLLMAdapter

# Max chars per chunk before splitting (leave room for prompt overhead)
MAX_CHUNK_CHARS = 5000

# Map prompt for map_reduce
MAP_PROMPT = PromptTemplate(
    input_variables=["text"],
    template="""Extract key factual information and regulatory entities from this section. Ignore UI navigation steps, SOPs for process completion, and procedural instructions.

For tables or large data: summarize as counts, totals, or statistics (e.g., "Table lists 47 items across 6 categories").

Output 2-3 sentences max covering:
- Factual entities and their attributes
- Regulatory requirements and obligations
- Key statistics or aggregated data

Use **bold** for key entities, obligations, and important values.

Text:
{text}""",
)

# Reduce prompt to combine chunk summaries
REDUCE_PROMPT = PromptTemplate(
    input_variables=["text"],
    template="""Combine these section summaries into a cohesive 2-3 paragraph regulatory summary.

Focus on:
- What the regulation requires and who it applies to
- Key compliance obligations and deadlines
- Important entities, thresholds, or statistics mentioned

Ignore: UI steps, navigation instructions, SOPs, and procedural how-to guides.

Use **bold** for key entities, obligations, deadlines, and important values.

Summaries:
{text}""",
)

# Stuff prompt for small documents
SUMMARY_PROMPT = PromptTemplate(
    input_variables=["text"],
    template="""You are a regulatory compliance assistant. Summarize the following document focusing on factual content.

Extract:
- What the regulation requires and who it applies to
- Key compliance obligations, deadlines, and thresholds
- Important entities and their attributes
- Aggregated statistics from tables (ignore individual entries)

Skip:
- UI navigation steps or screen instructions
- SOPs and process completion procedures
- Procedural how-to guides

Use **bold** for key entities, obligations, deadlines, and important values.
Be precise and factual. Do not add commentary or opinions.

Document:
{text}""",
)


class DocumentSummarizerProcessor(BaseProcessor):
    """Processor to generate concise summaries of regulatory documents using LangChain."""

    def __init__(self, db_pool: Any):
        super().__init__(db_pool)
        self.summary_repo = SummaryRepository(db_pool)
        self.circular_repo = CircularRepository(db_pool)

    @property
    def name(self) -> str:
        return "document_summarizer"

    def process(self, record: CircularRecord) -> None:
        # Get PDF path from record assets
        file_path = None
        assets = self.circular_repo.list_assets(record.id)
        for role in ["extracted_pdf", "original_pdf"]:
            for asset in assets:
                if asset.asset_role == role and asset.file_path and asset.file_path.lower().endswith(".pdf"):
                    file_path = asset.file_path
                    break
            if file_path:
                break

        # Fallback to main file path if it's a PDF
        if not file_path and record.file_path and record.file_path.lower().endswith(".pdf"):
            file_path = record.file_path

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

        # Build LangChain adapter
        llm_adapter = LangChainLLMAdapter(
            provider_name=Config.LLM_PROVIDER,
            model_name=Config.RAG_MODEL,
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
        except Exception as e:
            raise RuntimeError(f"LangChain summarization failed: {e}")

        if not summary_text or not summary_text.strip():
            raise RuntimeError("Summarization returned empty result.")

        summary_text = summary_text.strip()
        self.logger.info("Generated summary for circular_id=%s, length=%s", record.id, len(summary_text))

        # Idempotent: delete old summary and upload new one
        self.summary_repo.delete_summary_for_circular(record.id)
        self.summary_repo.upload_and_store_summary(
            circular_id=record.id,
            source=record.source,
            circular_ref=record.circular_id,
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

    from db.client import get_db_client
    db_client = get_db_client()
    pool = db_client.get_pool()

    from ingestion.repository.circular_repository import CircularRepository
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