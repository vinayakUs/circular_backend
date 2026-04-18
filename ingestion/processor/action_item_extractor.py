import argparse
import logging
import sys
from typing import List, Optional, Any

from pydantic import BaseModel, Field

from config import Config
from db.client import get_db_client
from ingestion.indexer.pdf_extractor import PDFTextExtractor
from ingestion.processor.base import BaseProcessor
from ingestion.repository.action_item_repository import ActionItemRepository
from ingestion.repository.circular_repository import CircularRepository, CircularRecord
from utils.llm_client import get_llm_client


class ActionItem(BaseModel):
    """Represents a single extracted action item from a circular."""

    action_item: str = Field(
        ...,
        description="Full action item in natural language format including entity, action, and deadline. Examples: 'Trading in HDFC Bank Limited's Non-Convertible Securities (ISIN: INE040A08468) will be suspended from April 17, 2026.' or 'Promoters of Creative Merchants Ltd must purchase shares from public shareholders as per fair value by May 11, 2026.'",
    )
    deadline: Optional[str] = Field(
        None,
        description=" deadline in YYYY-MM-DD format. Copy directly from the text without modification.",
    )
    priority: Optional[str] = Field(
        None,
        description="Priority level: critical, high, medium, or low. Assess based on regulatory consequence and urgency.",
    )


class ActionItemList(BaseModel):
    """A list of extracted action items."""

    items: List[ActionItem]


class ActionItemProcessor(BaseProcessor):
    """Processor to extract and persist action items from circulars."""

    def __init__(self, db_pool: Any):
        super().__init__(db_pool)
        self.action_repo = ActionItemRepository(db_pool)
        self.circular_repo = CircularRepository(db_pool)

    @property
    def name(self) -> str:
        return "action_item_extractor"

    def process(self, record: CircularRecord) -> None:
        file_path = None
        
        # Prefer an explicitly extracted PDF or original PDF
        assets = self.circular_repo.list_assets(record.id)
        for role in ['extracted_pdf', 'original_pdf']:
            for asset in assets:
                if asset.asset_role == role and asset.file_path and asset.file_path.lower().endswith('.pdf'):
                    file_path = asset.file_path
                    break
            if file_path:
                break
        
        # Fallback to the main record file path if it's a PDF
        if not file_path and record.file_path and record.file_path.lower().endswith('.pdf'):
            file_path = record.file_path

        if not file_path:
            raise ValueError(f"No PDF file path found for circular: {record.circular_id}")

        # Extract text from the PDF
        extractor = PDFTextExtractor()
        try:
            text = extractor.extract(file_path)
        except Exception as e:
            raise RuntimeError(f"Failed to extract text from PDF at {file_path}: {e}")

        if not text.strip():
            raise ValueError("Extracted text from PDF is empty.")

        # Call LLM
        llm_client = get_llm_client()
        model = Config.ACTION_ITEM_MODEL

        prompt = f"""
        You are a regulatory compliance assistant. Extract the highest-level summarized action items, obligations, and deadlines from the following circular text.

        Guidelines:
        1. Each action_item should be in natural action item format starting with a verb (e.g., "Extend...", "Reduce...", "Trading members must..."). Keep it brief - aim for 10-20 words maximum.
        2. The deadline field must be in YYYY-MM-DD format. Extract dates directly from the text without modification. If no specific date is mentioned, use null.
        3. Also extract priority: critical, high, medium, or low. Assess based on regulatory consequence and urgency.
        4. Output EXACTLY 1 action item that combines all key changes into a single concise summary.

        Examples of good action items:
        - action_item: "Trading in HDFC Bank Limited's Non-Convertible Securities will be suspended from April 17, 2026."
          deadline: "2026-04-17"
          priority: "critical"
        - action_item: "ZCZP minimum subscription reduced to 50% for Social Stock Exchange."
          deadline: null
          priority: "medium"
        - action_item: "Trading members must ensure all Client and PRO UCCs are compliant with KYC attributes, custodian details, PAN verification, and PAN-Aadhaar seeding by April 01, 2022."
          deadline: "2022-04-01"
          priority: "high"

        Text to extract from:
        {text}
        """

        try:
            response = llm_client.chat.completions.create(
                model=model,
                response_model=ActionItemList,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:
            raise RuntimeError(f"LLM extraction failed: {e}")

        # Persist action items idempotently
        self.action_repo.delete_action_items_for_circular(record.id)
        if response.items:
            self.action_repo.insert_action_items(record.id, response.items)
            self.logger.info("Inserted %d action items for circular_id=%s", len(response.items), record.id)
        else:
            self.logger.info("No action items extracted for circular_id=%s", record.id)


def main():
    parser = argparse.ArgumentParser(description="Extract action items from a Circular using LLM.")
    parser.add_argument("--circular_id", type=str, required=True, help="The circular ID to process (e.g. SEBI/HO/CFD/... )")
    
    args = parser.parse_args()

    print(f"Extracting action items for: {args.circular_id}")
    try:
        db_client = get_db_client()
        pool = db_client.get_pool()
        repo = CircularRepository(pool)
        
        record = repo.get_record_by_circular_id(args.circular_id)
        if not record:
            print(f"No circular found with ID: {args.circular_id}", file=sys.stderr)
            sys.exit(1)

        processor = ActionItemProcessor(pool)
        success = processor.run(record)
        
        if success:
            print("Successfully processed and saved action items.")
            # Verify and print
            action_repo = ActionItemRepository(pool)
            # Add a small helper query to see them
            with pool.connection() as conn:
                rows = conn.execute("SELECT action_item, deadline, priority FROM action_items WHERE circular_id = %s", (record.id,)).fetchall()
                print("\n=== Saved Action Items ===")
                for r in rows:
                    print(f"- {r[0]} (Deadline: {r[1]}, Priority: {r[2]})")
        else:
            print("Failed to process circular.", file=sys.stderr)
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    # Setup basic logging for CLI
    logging.basicConfig(level=logging.INFO)
    main()
