import logging
from abc import ABC, abstractmethod
from typing import Any

from ingestion.repository.circular_repository import CircularRecord
from ingestion.repository.processor_repository import ProcessorRepository


class BaseProcessor(ABC):
    """Abstract base class for all circular processors."""

    def __init__(self, db_pool: Any):
        self.db_pool = db_pool
        self.processor_repo = ProcessorRepository(db_pool)
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    @abstractmethod
    def name(self) -> str:
        """The unique name of the processor. Used for tracking status in the database."""
        pass

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
