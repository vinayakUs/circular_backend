import logging
from abc import ABC, abstractmethod
from typing import Any
from ingestion.repository.asset_repository import AssetRepository
from ingestion.repository.asset_repository import AssetRepository
from ingestion.repository.circular_repository import CircularRecord
from ingestion.repository.processor_repository import ProcessorRepository


class BaseProcessor(ABC):
    """Abstract base class for all circular processors."""

    def __init__(self, db_pool: Any):
        self.db_pool = db_pool
        self.processor_repo = ProcessorRepository(db_pool)
        self.asset_repo = AssetRepository(db_pool)
        self.logger = logging.getLogger(self.__class__.__name__)

    name: str  # Class attribute - subclasses must define (e.g., "nse_applicability_processor")

    @abstractmethod
    def process(self, record: CircularRecord) -> None:
        """The core processing logic for a single circular record.
        
        This method should be idempotent. It should clear any previous 
        extracted data for this circular before inserting new data.
        """
        pass

    def run(self, record: CircularRecord) -> bool:
        """Template method for running the processor on a record.
        
        Handles database state updates (COMPLETED/FAILED).
        Returns True if successful, False otherwise.
        """
        self.logger.info("Running processor '%s' for circular_id=%s", self.name, record.id)
        try:
            self.process(record)
            self.processor_repo.mark_task_completed(record.id, self.name)
            self.logger.info("Successfully processed circular_id=%s with '%s'", record.id, self.name)
            return True
        except Exception as e:
            self.logger.exception("Failed to process circular_id=%s with '%s'", record.id, self.name)
            self.processor_repo.mark_task_failed(record.id, self.name, str(e))
            return False

    def _get_pdf_path(self, record: CircularRecord) -> str:
        """Locate the PDF for a circular.

        Prefers extracted_pdf, then original_pdf.
        Raises ValueError if no PDF exists.
        """
        assets = self.asset_repo.list_assets(record.id)
        for role in ("extracted_pdf", "original_pdf"):
            for asset in assets:
                if (
                    asset.asset_role == role
                    and asset.file_path
                    and asset.file_path.lower().endswith(".pdf")
                ):
                    return asset.file_path

        raise ValueError(f"No PDF file path found for circular: {record.circular_id}")