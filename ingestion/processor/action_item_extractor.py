"""
python -m ingestion.processor.action_item_extractor --circular_id "SEBI/HO/CFD/..."
"""

import argparse
import logging
import sys
from typing import Any, Optional

from pydantic import BaseModel, Field

from config import Config
from db.postgres_client import get_postgres_client
from ingestion.indexer.pdf_extractor import PDFTextExtractor
from ingestion.processor.base import BaseProcessor
from ingestion.repository.action_item_repository import ActionItemRepository
from ingestion.repository.circular_repository import CircularRepository, CircularRecord
from ingestion.repository.processor_repository import ProcessorRepository
from utils.llm_providers import get_llm_provider

class ActionItemSingle(BaseModel):
    """Single action item extracted from a circular."""

    action_item: str = Field(
        ...,
        description=(
            "Full action item in natural language format including entity, action, and deadline. "
            "Example: 'Trading in HDFC Bank Limited's Non-Convertible Securities will be "
            "suspended from April 17, 2026.'"
        ),
    )
    deadline: Optional[str] = Field(
        None,
        description="Deadline in YYYY-MM-DD format. Copy directly from text without modification. Use null if no specific date is mentioned.",
    )
    priority: Optional[str] = Field(
        None,
        description="Priority level: critical, high, medium, or low. Assess based on regulatory consequence and urgency.",
    )


class ActionItemProcessor(BaseProcessor):
    """Processor to extract and persist action items from circulars."""

    name = "action_item_extractor"  # class attribute (matches siblings)

    def __init__(self, db_pool: Any):
        super().__init__(db_pool)
        self.action_repo = ActionItemRepository(db_pool)
        self.pdf_extractor = PDFTextExtractor()


    def process(self, record: CircularRecord) -> None:

        # Step 1: locate PDF (inherited helper, raises ValueError if missing)
        pdf_path = self._get_pdf_path(record)

        # Step 2: extract text (PDFTextExtractor handles S3, %PDF check, FileNotFound)
        try:
            text = self.pdf_extractor.extract(pdf_path)
        except Exception as e:
            raise RuntimeError(f"Failed to extract text from PDF at {pdf_path}: {e}") from e

        if not text.strip():
            raise ValueError("Extracted text from PDF is empty.")

        # Step 3: call LLM
        llm_client = get_llm_provider(Config.LLM_PROVIDER)
        model = Config.ACTION_ITEM_MODEL


        prompt = f"""You are a regulatory compliance assistant. Extract the highest-level summarized action items, obligations, and deadlines from the following circular text.

Guidelines:
1. Each action_item should be in natural action item format starting with a verb
   (e.g., "Extend...", "Reduce...", "Trading members must..."). Keep it brief — aim for 10-20 words maximum.
2. The deadline field must be in YYYY-MM-DD format. Extract dates directly from the text
   without modification. If no specific date is mentioned, use null.
3. Also extract priority: critical, high, medium, or low. Assess based on regulatory
   consequence and urgency.
4. Output EXACTLY 1 action item that combines all key changes into a single concise summary.

Examples of good action items:
- action_item: "Trading in HDFC Bank Limited's Non-Convertible Securities will be suspended from April 17, 2026."
  deadline: "2026-04-17"
  priority: "critical"
- action_item: "ZCZP minimum subscription reduced to 50% for Social Stock Exchange."
  deadline: null
  priority: "medium"

Text to extract from:
{text}
"""

        try:
            response = llm_client.create_completions_parallel(
                prompts=[prompt],
                model=model,
                response_model=ActionItemSingle,
            )[0]
        except Exception as e:
            raise RuntimeError(f"LLM extraction failed: {e}")

        # Step 4: persist (idempotent — supports 1 or N items later)
        self.action_repo.delete_action_items_for_circular(record.id)
        self.action_repo.insert_action_items(record.id, [response])
        self.logger.info("Inserted action item for circular_id=%s", record.id)

def main() -> None:
    parser = argparse.ArgumentParser(description="Extract action items from a Circular using LLM.")
    parser.add_argument("--circular_id", type=str, help="Process a specific circular by ID")
    parser.add_argument("--limit", type=int, default=100, help="Max pending circulars to process")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    db_client = get_postgres_client()
    pool = db_client.get_pool()
    processor = ActionItemProcessor(pool)

    if args.circular_id:
        logger.info("Extracting action items for: %s", args.circular_id)
        repo = CircularRepository(pool)
        record = repo.get_record_by_circular_id(args.circular_id)
        if not record:
            logger.error("No circular found with ID: %s", args.circular_id)
            sys.exit(1)
        success = processor.run(record)
        sys.exit(0 if success else 1)
    else:
        processor_repo = ProcessorRepository(pool)
        pending = processor_repo.get_pending_circulars_for_processor(processor.name, limit=args.limit)
        logger.info("Found %d pending circulars for '%s'", len(pending), processor.name)
        for record in pending:
            logger.info("Processing: %s", record.circular_id)
            processor.run(record)
        logger.info("Completed %d circulars.", len(pending))


if __name__ == "__main__":
    main()