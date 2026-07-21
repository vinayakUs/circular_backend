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
from uuid import UUID

from config import Config
from db.postgres_client import get_postgres_client
from ingestion.indexer.pdf_extractor import PDFTextExtractor
from ingestion.processor.base import BaseProcessor
from ingestion.repository.asset_repository import AssetRepository
from ingestion.repository.circular_repository import CircularRecord, CircularRepository
from ingestion.repository.circular_signatory_repository import CircularSignatoryRepository, Signatory
from pydantic import BaseModel, Field
from instructor.core import InstructorRetryException
from storage.s3_client import S3StorageClient
from utils.llm_providers import get_llm_provider


class DesignationExtractorProcessor(BaseProcessor):
    """Processor to extract signatory name and designation from circulars."""

    NSE_SIGNATORY_PATTERNS = [
        re.compile(r"For\s+and\s+on\s+behalf\s+of", re.IGNORECASE),
        re.compile(r"For\s+&\s+on\s+behalf\s+of", re.IGNORECASE),
        re.compile(r"For\s+National\s+Stock\s+Exchange\s+of\s+India\s+Limited", re.IGNORECASE),
        re.compile(r"for\s+and\s+on\s+behalf\s+of\s+national\s+stock\s+exchange", re.IGNORECASE),
        re.compile(r"for\s+national\s+stock\s+exchange\s+of\s+india\s+limited", re.IGNORECASE),
        re.compile(r"For\s+and\s+on\s+behalf\s+of\s+National\s+Stock\s+Exchange\s+of\s+India\s+Ltd", re.IGNORECASE),
        re.compile(r"For\s+and\s+on\s+behalf\s+of\s+National\s+Stock\s+Exchange\s+of\s+India\s+Limited", re.IGNORECASE),
    ]
    SEBI_SIGNATORY_PATTERN = re.compile(r"Yours\s+(?:faithfully|sincerely)", re.IGNORECASE)

    def __init__(self, db_pool: Any):
        super().__init__(db_pool)
        self.circular_repo = CircularRepository(db_pool)
        self.asset_repo = AssetRepository(db_pool)
        self.signatory_repo = CircularSignatoryRepository(db_pool)

    name = "designation_extractor_processor"

    def process(self, record: CircularRecord) -> None:
        self.logger.info('designation extractor process start %s', record.applicable_to_nse)

        file_path = self._get_pdf_path(record)
        if not file_path:
            raise ValueError(f"No PDF file path found for circular: {record.circular_id}")

        full_text = self._extract_all_text(file_path)
        if not full_text:
            raise ValueError("PDF is empty")

        signatory_text = self._extract_signatory_block(record.source, full_text)
        if signatory_text:
            result = self._extract_with_llm(signatory_text, record.source, record.id)
            if result and result.signatories:
                signatories = [
                    Signatory(
                        name=s.name.title(),
                        designation=s.designation,
                    )
                    for s in result.signatories
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
            if not pdf_bytes.startswith(b"%PDF"):
                raise ValueError(
                    f"Downloaded content from {path} is not a valid PDF "
                    f"(starts with: {pdf_bytes[:50]!r}). "
                    f"Check that the S3 URL is correct and the object exists."
                )
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
            patterns = self.NSE_SIGNATORY_PATTERNS
        else:
            # Default to SEBI pattern for SEBI or unknown sources
            patterns = [self.SEBI_SIGNATORY_PATTERN]

        match = None
        for pattern in patterns:
            match = pattern.search(text)
            if match:
                break
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

    def _extract_with_llm(self, signatory_text: str, source: str, circular_id: UUID) -> Any | None:
        """Use LLM to extract name and designation from the signatory block.

        Returns the parsed DesignationResponse on success, or None if the LLM
        returned an empty signatories list (valid "no persons found" result).
        Raises InstructorRetryException when the LLM call fails after all retries —
        this propagates up to BaseProcessor.run which marks the task FAILED so the
        next pipeline run retries once the LLM service recovers.
        """

        class SignatoryEntry(BaseModel):
            name: str = Field(description="The full official name of the signatory")
            designation: str = Field(description="The official title/role of the signatory (e.g., Managing Director, Chief General Manager)")

        class DesignationResponse(BaseModel):
            signatories: list[SignatoryEntry] = Field(description="List of all signatories found in the block")

        llm_client = get_llm_provider(Config.LLM_PROVIDER)
        model = Config.ACTION_ITEM_MODEL

        prompt = f"""Extract ONLY the INDIVIDUAL human signatories (name and designation) from the following text taken from a {source} circular.

IMPORTANT:
- Extract ONLY named persons with their personal designations (e.g., "Meghna Chavan, Senior Manager")
- DO NOT include company names, organization names, or entity names (e.g., "National Stock Exchange of India Limited")
- DO NOT include "For and on behalf of" phrases
- Skip any line that is just an organization/company name without a personal name

Signatory Block:
{signatory_text}

Return only the actual human signatories with their roles. Ignore organizations and entity names."""

        response = llm_client.create_completions_parallel(
            prompts=[prompt],
            model=model,
            response_model=DesignationResponse,
        )[0]

        if response is None:
            # The LLM client swallowed the underlying InstructorRetryException
            # after exhausting retries. Re-raise so the pipeline records a FAILED
            # row in processing_tasks instead of silently marking COMPLETED with
            # no signatories persisted.
            self.logger.error(
                "metric=designation_extractor_llm_failure "
                "circular_id=%s source=%s — LLM returned no response after retries",
                circular_id, source,
            )
            raise InstructorRetryException(
                "LLM call returned no response after retries",
                n_attempts=3,
                total_usage=0,
            )

        return response


def main():
    parser = argparse.ArgumentParser(description="Extract signatory designation from a circular.")
    parser.add_argument("--circular_id", type=str, help="Process a specific circular by ID")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of pending circulars to process")
    args = parser.parse_args()

    db_client = get_postgres_client()
    pool = db_client.get_pool()
    processor = DesignationExtractorProcessor(pool)

    if args.circular_id:
        logging.info("Extracting signatory for: %s", args.circular_id)
        repo = CircularRepository(pool)
        record = repo.get_record_by_circular_id(args.circular_id)
        if not record:
            logging.error("No circular found with ID: %s", args.circular_id)
            sys.exit(1)
        success = processor.run(record)
        if success:
            logging.info("Processing complete for: %s", args.circular_id)
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