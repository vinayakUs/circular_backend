'''
  python -m ingestion.processor.nse_applicability_processor --circular_id "SEBI/HO/CFD/..."
  python -m ingestion.processor.nse_applicability_processor --limit 100
'''
import argparse
import logging
import re
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from config import Config
from db.postgres_client import get_postgres_client
from ingestion.indexer.es_provider import get_es_client
from ingestion.indexer.pdf_extractor import PDFTextExtractor
from ingestion.processor.base import BaseProcessor
from ingestion.repository.asset_repository import AssetRepository
from ingestion.repository.circular_repository import CircularRecord, CircularRepository
from pydantic import BaseModel, Field
from storage.s3_client import S3StorageClient
from utils.llm_providers import get_llm_provider


class NSEApplicabilityProcessor(BaseProcessor):
    """Processor to determine if a SEBI circular is applicable to NSE based on first page text."""

    NSE_APPLICABLE_PATTERNS = [
        re.compile(r"To\s+All\s+Stock\s+Exchange[sz]", re.IGNORECASE),
        re.compile(r"To\s+All\s+Recognised\s+Stock\s+Exchange[sz]", re.IGNORECASE),
        re.compile(r"To\s+All\s+Recognized\s+Stock\s+Exchange[sz]", re.IGNORECASE),
        re.compile(r"All\s+Stock\s+Exchange[sz]\b", re.IGNORECASE),
        re.compile(r"All\s+Recognised\s+Stock\s+Exchange[sz]\b", re.IGNORECASE),
        re.compile(r"All\s+Recognized\s+Stock\s+Exchange[sz]\b", re.IGNORECASE),
    ]

    def __init__(self, db_pool: Any):
        super().__init__(db_pool)
        self.circular_repo = CircularRepository(db_pool)
        self.asset_repo = AssetRepository(db_pool)

    name = "nse_applicability_processor"

    def process(self, record: CircularRecord) -> None:
        self.logger.info('nse applicability process start %s', record.applicable_to_nse)
        if record.source == 'NSE':
            self.logger.info('nse applicability process inside nse blocks')

            self.circular_repo.update_applicable_to_nse(record.id, True)
            if record.es_indexed_at is not None:
                get_es_client().update_applicable_to_nse(str(record.id), True)
            self.logger.info("Source is NSE, set applicable_to_nse=True for %s", record.id)
            return

        file_path = self._get_pdf_path(record)
        if not file_path:
            raise ValueError(f"No PDF file path found for circular: {record.circular_id}")

        first_page_text = self._extract_first_page(file_path)
        if not first_page_text:
            raise ValueError("First page is empty")

        self.logger.info("=== NSE Check START ===")
        self.logger.info("circular_id=%s current_applicable_to_nse=%s", record.id, record.applicable_to_nse)

        is_applicable = self._check_nse_applicable(first_page_text)
        self.logger.info("Pattern match result: is_applicable=%s", is_applicable)

        # If pattern not found for SEBI circulars, use LLM to determine
        if not is_applicable:
            is_applicable = self._check_with_llm(first_page_text, record.title)
            self.logger.info("LLM result: is_applicable=%s", is_applicable)
        else:
            self.logger.info("Skipping LLM (pattern already matched)")

        self.logger.info("Final is_applicable=%s for %s", is_applicable, record.id)

        self.circular_repo.update_applicable_to_nse(record.id, is_applicable)
        if record.es_indexed_at is not None:
            get_es_client().update_applicable_to_nse(str(record.id), is_applicable)
        self.logger.info("Updated applicable_to_nse=%s for %s", is_applicable, record.id)

        self.logger.info("=== NSE Check END ===")

    def _get_pdf_path(self, record: CircularRecord) -> str | None:
        assets = self.asset_repo.list_assets(record.id)
        for role in ['extracted_pdf', 'original_pdf']:
            for asset in assets:
                if asset.asset_role == role and asset.file_path and asset.file_path.lower().endswith('.pdf'):
                    return asset.file_path

        if record.file_path and record.file_path.lower().endswith('.pdf'):
            return record.file_path
        return None

    def _extract_first_page(self, path: str | Path) -> str:
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise RuntimeError(
                "pypdf is not installed. Install dependencies before running the processor."
            ) from exc

        target = Path(path)

        if str(path).startswith("s3://"):
            s3_client = S3StorageClient()
            pdf_bytes = s3_client.download_bytes(str(path))
            with TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir) / "doc.pdf"
                tmp_path.write_bytes(pdf_bytes)
                reader = PdfReader(str(tmp_path))
        else:
            if not target.exists():
                raise FileNotFoundError(f"PDF file not found: {path}")
            reader = PdfReader(str(target))

        if reader.pages:
            return reader.pages[0].extract_text() or ""
        return ""

    def _check_nse_applicable(self, text: str) -> bool:
        for pattern in self.NSE_APPLICABLE_PATTERNS:
            if pattern.search(text):
                self.logger.info("Pattern match found for NSE applicability: pattern=%s", pattern.pattern)
                return True
        return False

    def _check_with_llm(self, text: str, title: str) -> bool:
        """Use LLM to determine if circular is applicable to NSE when pattern match fails."""

        class RecipientExtractionResponse(BaseModel):
            recipients: list[str] = Field(description="List of entities this circular is addressed to")

        llm_client = get_llm_provider(Config.LLM_PROVIDER)
        model = Config.ACTION_ITEM_MODEL

        prompt = f"""Extract the list of entities this SEBI circular is addressed to.

First Page Text:
{text[:2000]}

Only list the entities that appear in the "To:" field. For example:
- If "To: All Stock Exchanges" → extract ["All Stock Exchanges"]
- If "To: All AIFs, All Merchant Bankers" → extract ["All AIFs", "All Merchant Bankers"]
- If "To: All Alternative Investment Funds" → extract ["All Alternative Investment Funds"]

Only extract entities from the "To" field, not from the body of the circular."""

        try:
            # Step 1: Extract recipients
            extraction_response = llm_client.create_completions_parallel(
                prompts=[prompt],
                model=model,
                response_model=RecipientExtractionResponse,
            )[0]

            recipients = extraction_response.recipients

            self.logger.info("LLM extracted recipients: %s", recipients)

            # Empty recipients = no specific recipient = likely applicable
            if not recipients:
                self.logger.info("No recipients extracted, treating as applicable to NSE")
                return True

            # Step 2: Classify based on recipients
            stock_exchange_keywords = ["stock exchange", "nse", "bse", "national stock exchange", "bombay stock exchange", "exchanges"]
            for recipient in recipients:
                recipient_lower = recipient.lower()
                matched_keyword = None
                for keyword in stock_exchange_keywords:
                    if keyword in recipient_lower:
                        matched_keyword = keyword
                        break
                if matched_keyword:
                    self.logger.info("Recipient '%s' matched keyword '%s' → applicable", recipient, matched_keyword)
                    return True
                if not any(word in recipient_lower for word in ["stock", "exchange", "nse", "bse"]):
                    # If no stock exchange keyword found, it's not applicable
                    self.logger.info("Recipient '%s' has no stock exchange keywords → not applicable", recipient)
                    continue
            self.logger.info("No recipients matched stock exchange criteria → not applicable")
            return False
        except Exception as e:
            self.logger.warning("LLM check failed: %s", e)
            return True  # Changed from False


def main():
    parser = argparse.ArgumentParser(description="Check NSE applicability for a circular.")
    parser.add_argument("--circular_id", type=str, help="The circular ID to process (e.g. SEBI/HO/CFD/...)")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of pending circulars to process")

    args = parser.parse_args()

    db_client = get_postgres_client()
    pool = db_client.get_pool()
    processor = NSEApplicabilityProcessor(pool)

    if args.circular_id:
        logging.info("Checking NSE applicability for: %s", args.circular_id)
        repo = CircularRepository(pool)

        record = repo.get_record_by_circular_id(args.circular_id)
        if not record:
            logging.error("No circular found with ID: %s", args.circular_id)
            sys.exit(1)

        success = processor.run(record)

        if success:
            # Fetch updated record to show result
            updated = repo.get_record_by_circular_id(args.circular_id)
            logging.info("Processing complete. applicable_to_nse: %s", updated.applicable_to_nse)
        else:
            logging.error("Failed to process circular: %s", args.circular_id)
            sys.exit(1)
    else:
        from ingestion.repository.processor_repository import ProcessorRepository
        processor_repo = ProcessorRepository(pool)
        pending = processor_repo.get_pending_circulars_for_processor(processor.name, limit=args.limit)
        logging.info("Found %d pending circulars for '%s'", len(pending), processor.name)
        for record in pending:
            logging.info("Processing: %s", record.circular_id)
            processor.run(record)
        logging.info("Completed %d circulars.", len(pending))


if __name__ == "__main__":
    main()
