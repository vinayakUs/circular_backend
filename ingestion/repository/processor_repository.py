import logging
from typing import Any
from uuid import UUID

from ingestion.repository.circular_repository import CircularRecord, CircularRepository, _uuid_to_raw


class ProcessorRepository:
    """Repository for managing processing tasks state."""

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("ProcessorRepository requires db_pool")
        self.logger = logging.getLogger(__name__)
        self.db_pool = db_pool
        self.circular_repo = CircularRepository(db_pool)

    def get_pending_circulars_for_processor(self, processor_name: str, limit: int = 100) -> list[CircularRecord]:
        """Finds circulars that need to be processed by a specific processor."""
        self.circular_repo._ensure_schema()
        with self.db_pool.acquire_connection() as conn:
            rows = conn.execute(
                """
                SELECT c.id, c.source, c.circular_id, c.source_item_key, c.full_reference,
                       c.department, c.title, c.issue_date, c.applicable_to_nse, c.url, c.pdf_url,
                       c.status, c.file_path, c.content_hash, c.error_message, c.detected_at,
                       c.created_at, c.updated_at, c.es_indexed_at, c.es_chunk_count,
                       c.es_index_name
                FROM circulars c
                LEFT JOIN processing_tasks pt
                  ON c.id = pt.circular_id AND pt.processor_name = :1
                WHERE c.status = 'FETCHED'
                  AND c.file_path IS NOT NULL
                  AND (pt.id IS NULL OR pt.status = 'FAILED' OR pt.status = 'PENDING')
                ORDER BY c.issue_date DESC, c.created_at ASC
                OFFSET 0 ROWS FETCH NEXT :2 ROWS ONLY
                """,
                (processor_name, limit),
            ).fetchall()
        return [record for row in rows if (record := self.circular_repo._row_to_record(row))]

    def mark_task_completed(self, circular_id: UUID, processor_name: str) -> None:
        self.circular_repo._ensure_schema()
        circular_id_raw = _uuid_to_raw(circular_id)
        with self.db_pool.acquire_connection() as conn:
            conn.execute(
                """
                MERGE INTO processing_tasks pt
                USING (SELECT :1 AS circular_id, :2 AS processor_name FROM DUAL) src
                ON (pt.circular_id = src.circular_id AND pt.processor_name = src.processor_name)
                WHEN MATCHED THEN
                    UPDATE SET pt.status = 'COMPLETED', pt.error_message = NULL, pt.updated_at = SYSDATE
                WHEN NOT MATCHED THEN
                    INSERT (circular_id, processor_name, status) VALUES (src.circular_id, src.processor_name, 'COMPLETED')
                """,
                (circular_id_raw, processor_name),
            )
            conn.commit()

    def mark_task_failed(self, circular_id: UUID, processor_name: str, error_message: str) -> None:
        self.circular_repo._ensure_schema()
        circular_id_raw = _uuid_to_raw(circular_id)
        with self.db_pool.acquire_connection() as conn:
            conn.execute(
                """
                MERGE INTO processing_tasks pt
                USING (SELECT :1 AS circular_id, :2 AS processor_name, :3 AS error_message FROM DUAL) src
                ON (pt.circular_id = src.circular_id AND pt.processor_name = src.processor_name)
                WHEN MATCHED THEN
                    UPDATE SET pt.status = 'FAILED', pt.error_message = src.error_message, pt.updated_at = SYSDATE
                WHEN NOT MATCHED THEN
                    INSERT (circular_id, processor_name, status, error_message)
                    VALUES (src.circular_id, src.processor_name, 'FAILED', src.error_message)
                """,
                (circular_id_raw, processor_name, error_message),
            )
            conn.commit()