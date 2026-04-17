import logging
from typing import Any, List

from ingestion.processor.base import BaseProcessor
from ingestion.repository.processor_repository import ProcessorRepository


class ProcessorPipeline:
    """Orchestrates the execution of multiple processors on pending circulars."""

    def __init__(self, db_pool: Any):
        self.db_pool = db_pool
        self.processor_repo = ProcessorRepository(db_pool)
        self.logger = logging.getLogger(__name__)
        self._processors: List[BaseProcessor] = []

    def register_processor(self, processor: BaseProcessor) -> None:
        """Registers a processor to be run in the pipeline."""
        self._processors.append(processor)
        self.logger.info("Registered processor: %s", processor.name)

    def run(self, limit_per_processor: int = 100) -> None:
        """Runs all registered processors on their respective pending circulars."""
        self.logger.info("Starting ProcessorPipeline run with %d processors", len(self._processors))

        for processor in self._processors:
            self.logger.info("Processing pending circulars for '%s'", processor.name)
            pending_records = self.processor_repo.get_pending_circulars_for_processor(
                processor.name, limit=limit_per_processor
            )
            
            if not pending_records:
                self.logger.info("No pending circulars for '%s'", processor.name)
                continue

            self.logger.info("Found %d pending circulars for '%s'", len(pending_records), processor.name)

            success_count = 0
            for record in pending_records:
                if processor.run(record):
                    success_count += 1

            self.logger.info(
                "Completed run for '%s'. Success: %d/%d", 
                processor.name, success_count, len(pending_records)
            )

        self.logger.info("Finished ProcessorPipeline run")
