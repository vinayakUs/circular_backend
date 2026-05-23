'''
  python -m ingestion.processor.designation_extractor_processor --circular_id "SEBI/HO/CFD/..."
  python -m ingestion.processor.designation_extractor_processor --circular_id "NSE/CIR/..."
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
from ingestion.indexer.pdf_extractor import PDFTextExtractor
from ingestion.processor.base import BaseProcessor
from ingestion.repository.asset_repository import AssetRepository
from ingestion.repository.circular_repository import CircularRecord, CircularRepository
from ingestion.repository.circular_signatory_repository import CircularSignatoryRepository, Signatory
from pydantic import BaseModel, Field
from storage.s3_client import S3StorageClient
from utils.llm_providers import get_llm_provider


class DesignationExtractorProcessor(BaseProcessor):
    """Processor to extract signatory name and designation from circulars."""

    SEBI_SIGNATORY_PATTERN = re.compile(r"Yours\s+(?:faithfully|sincerely)", re.IGNORECASE)
    NSE_SIGNATORY_PATTERN = re.compile(r"For\s+and\s+on\s+behalf\s+of", re.IGNORECASE)

    def __init__(self, db_pool: Any):
        super().__init__(db_pool)
        self.circular_repo = CircularRepository(db_pool)
        self.asset_repo = AssetRepository(db_pool)
        self.signatory_repo = CircularSignatoryRepository(db_pool)

    name = "designation_extractor_processor"

    def process(self, record: CircularRecord) -> None:
        file_path = self._get_pdf_path(record)
        if not file_path:
            raise ValueError(f"No PDF file path found for circular: {record.circular_id}")

        full_text = self._extract_all_text(file_path)
        if not full_text:
            raise ValueError("PDF is empty")

        signatory_text = self._extract_signatory_block(record.source, full_text)
        if signatory_text:
            result = self._extract_with_llm(signatory_text, record.source)
            if result and result.signatories:
                signatories = [
                    Signatory(name=s.name, designation=s.designation) for s in result.signatories
                ]
                self.signatory_repo.upsert_signatories(record.id, signatories)
                for s in result.signatories:
                    self.logger.info(
                        "Signatory extracted and persisted: name=%s, designation=%s, circular_id=%s",
                        s.name,
                        s.designation,
                        record.circular_id,
                    )
        else:
            self.logger.info("No signatory block found for circular_id=%s", record.circular_id)

    def _get_pdf_path(self, record: CircularRecord) -> str | None:
        assets = self.asset_repo.list_assets(record.id)
        for role in ['extracted_pdf', 'original_pdf']:
            for asset in assets:
                if asset.asset_role == role and asset.file_path and asset.file_path.lower().endswith('.pdf'):
                    return asset.file_path
        if record.file_path and record.file_path.lower().endswith('.pdf'):
            return record.file_path
        return None

    def _extract_all_text(self, path: str | Path) -> str:
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

        text_parts = []
        for page in reader.pages:
            page_text = page.extract_text() or ""
            text_parts.append(page_text)
        return "\n".join(text_parts)

    def _extract_signatory_block(self, source: str, text: str) -> str | None:
        if source == "NSE":
            pattern = self.NSE_SIGNATORY_PATTERN
        else:
            # Default to SEBI pattern for SEBI or unknown sources
            pattern = self.SEBI_SIGNATORY_PATTERN

        match = pattern.search(text)
        if not match:
            return None

        # Get the position after the match
        start_pos = match.end()
        # Extract next 5-6 lines (approximately 500 characters to be safe)
        block = text[start_pos:start_pos + 600]

        # Take only the first few lines (up to newline or 5 newlines)
        lines = block.split('\n')
        # Take first 6 non-empty lines max
        signatory_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped:
                signatory_lines.append(stripped)
            if len(signatory_lines) >= 6:
                break

        if signatory_lines:
            return "\n".join(signatory_lines)
        return None

    def _extract_with_llm(self, signatory_text: str, source: str) -> Any | None:
        """Use LLM to extract name and designation from the signatory block."""

        class SignatoryEntry(BaseModel):
            name: str = Field(description="The full official name of the signatory")
            designation: str = Field(description="The official title/role of the signatory (e.g., Managing Director, Chief General Manager)")

        class DesignationResponse(BaseModel):
            signatories: list[SignatoryEntry] = Field(description="List of all signatories found in the block")

        llm_client = get_llm_provider(Config.LLM_PROVIDER)
        model = Config.ACTION_ITEM_MODEL

        prompt = f"""Extract ALL signatories (name and designation) from the following text taken from a {source} circular. There may be one or multiple signatories. Return every signatory you find.

Signatory Block:
{signatory_text}

Return all officials and their roles/designations. Be precise - use the exact text as it appears."""

        try:
            response = llm_client.create_completions_parallel(
                prompts=[prompt],
                model=model,
                response_model=DesignationResponse,
            )[0]
            return response
        except Exception as e:
            self.logger.warning("LLM extraction failed: %s", e)
            return None


def main():
    parser = argparse.ArgumentParser(description="Extract signatory designation from a circular.")
    parser.add_argument("--circular_id", type=str, required=True, help="The circular ID to process")

    args = parser.parse_args()

    print(f"Extracting signatory for: {args.circular_id}")
    try:
        db_client = get_postgres_client()
        pool = db_client.get_pool()
        repo = CircularRepository(pool)

        record = repo.get_record_by_circular_id(args.circular_id)
        if not record:
            print(f"No circular found with ID: {args.circular_id}", file=sys.stderr)
            sys.exit(1)

        processor = DesignationExtractorProcessor(pool)
        success = processor.run(record)

        if success:
            print("Processing complete.")
        else:
            print("Failed to process circular.", file=sys.stderr)
            sys.exit(1)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()