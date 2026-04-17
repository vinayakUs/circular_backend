import logging
from typing import Any
from uuid import UUID

from ingestion.repository.circular_repository import CircularRecord, CircularRepository


class ProcessorRepository:
    """Repository for managing processing tasks state."""

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("ProcessorRepository requires db_pool")
        self.logger = logging.getLogger(__name__)
        self.db_pool = db_pool
        # Use CircularRepository to parse CircularRecord and ensure schema
        self.circular_repo = CircularRepository(db_pool)

    def get_pending_circulars_for_processor(self, processor_name: str, limit: int = 100) -> list[CircularRecord]:
        """Finds circulars that need to be processed by a specific processor."""
        self.circular_repo._ensure_schema()
        with self.db_pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT c.id, c.source, c.circular_id, c.source_item_key, c.full_reference,
                       c.department, c.title, c.issue_date, c.effective_date, c.url, c.pdf_url,
                       c.status, c.file_path, c.content_hash, c.error_message, c.detected_at,
                       c.created_at, c.updated_at, c.es_indexed_at, c.es_chunk_count,
                       c.es_index_name
                FROM circulars c
                LEFT JOIN processing_tasks pt 
                  ON c.id = pt.circular_id AND pt.processor_name = %s
                WHERE c.file_path IS NOT NULL
                  AND (pt.id IS NULL OR pt.status = 'FAILED' OR pt.status = 'PENDING')
                ORDER BY c.issue_date DESC, c.created_at ASC
                LIMIT %s
                """,
                (processor_name, limit),
            ).fetchall()
        return [record for row in rows if (record := self.circular_repo._row_to_record(row))]

    def mark_task_completed(self, circular_id: UUID, processor_name: str) -> None:
        self.circular_repo._ensure_schema()
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO processing_tasks (circular_id, processor_name, status)
                VALUES (%s, %s, 'COMPLETED')
                ON CONFLICT (circular_id, processor_name) 
                DO UPDATE SET status = 'COMPLETED', error_message = NULL, updated_at = NOW()
                """,
                (circular_id, processor_name),
            )

    def mark_task_failed(self, circular_id: UUID, processor_name: str, error_message: str) -> None:
        self.circular_repo._ensure_schema()
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO processing_tasks (circular_id, processor_name, status, error_message)
                VALUES (%s, %s, 'FAILED', %s)
                ON CONFLICT (circular_id, processor_name) 
                DO UPDATE SET status = 'FAILED', error_message = EXCLUDED.error_message, updated_at = NOW()
                """,
                (circular_id, processor_name, error_message),
            )
