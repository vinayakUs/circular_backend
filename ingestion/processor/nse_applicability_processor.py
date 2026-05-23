'''
  python -m ingestion.processor.nse_applicability_processor --circular_id "SEBI/HO/CFD/..."             
  python -m ingestion.processor.runner --limit 100                                                      
  python -m ingestion.processor.nse_applicability_processor --circular_id "SEBI/HO/CFD/..."
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
        if record.source == 'NSE':
            return

        file_path = self._get_pdf_path(record)
        if not file_path:
            raise ValueError(f"No PDF file path found for circular: {record.circular_id}")

        first_page_text = self._extract_first_page(file_path)
        if not first_page_text:
            raise ValueError("First page is empty")

        is_applicable = self._check_nse_applicable(first_page_text)

        # If pattern not found for SEBI circulars, use LLM to determine
        if not is_applicable:
            is_applicable = self._check_with_llm(first_page_text, record.title)

        if is_applicable:
            self.circular_repo.update_applicable_to_nse(record.id, True)
            if record.es_indexed_at is not None:
                get_es_client().update_applicable_to_nse(str(record.id), True)

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

        class NSEApplicabilityResponse(BaseModel):
            answer: str = Field(description="YES or NO")

        llm_client = get_llm_provider(Config.LLM_PROVIDER)
        model = Config.ACTION_ITEM_MODEL

        prompt = f"""You are a regulatory compliance assistant. Determine if the following SEBI circular is applicable to NSE (National Stock Exchange of India).

Circular Title: {title}

First Page Text:
{text[:2000]}

Answer YES if:
1. This circular is addressed to NSE or any stock exchange (e.g., "All Stock Exchanges", "Recognised Stock Exchanges", "NSE", etc.)
2. OR there is NO recipient listed after "To" (i.e., the To field is blank/empty)

Answer NO if the circular is addressed to specific entities only (like specific banks, brokers, etc.) and does not mention stock exchanges."""

        try:
            response = llm_client.create_completions_parallel(
                prompts=[prompt],
                model=model,
                response_model=NSEApplicabilityResponse,
            )[0]
            return response.answer.upper() == "YES"
        except Exception as e:
            self.logger.warning("LLM check failed: %s", e)
            return False


def main():
    parser = argparse.ArgumentParser(description="Check NSE applicability for a circular.")
    parser.add_argument("--circular_id", type=str, required=True, help="The circular ID to process (e.g. SEBI/HO/CFD/...)")

    args = parser.parse_args()

    print(f"Checking NSE applicability for: {args.circular_id}")
    try:
        db_client = get_postgres_client()
        pool = db_client.get_pool()
        repo = CircularRepository(pool)

        record = repo.get_record_by_circular_id(args.circular_id)
        if not record:
            print(f"No circular found with ID: {args.circular_id}", file=sys.stderr)
            sys.exit(1)

        processor = NSEApplicabilityProcessor(pool)
        success = processor.run(record)

        if success:
            # Fetch updated record to show result
            updated = repo.get_record_by_circular_id(args.circular_id)
            print(f"\nProcessing complete.")
            print(f"  applicable_to_nse: {updated.applicable_to_nse}")
        else:
            print("Failed to process circular.", file=sys.stderr)
            sys.exit(1)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
