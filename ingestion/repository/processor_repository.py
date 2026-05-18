from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from ingestion.repository.circular_repository import (
    CircularRecord,
    CircularRepository,
    _raw_to_uuid,
    _uuid_to_raw,
)


class ProcessorRepository:

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("ProcessorRepository requires db_pool")
        self.logger = __import__("logging").getLogger(__name__)
        self.db_pool = db_pool
        self.circular_repo = CircularRepository(db_pool)

    def get_pending_circulars_for_processor(self, processor_name: str, limit: int = 100) -> list[CircularRecord]:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT c.id, c.source, c.circular_id, c.source_item_key, c.full_reference,
                       c.department, c.title, c.issue_date, c.applicable_to_nse, c.url, c.pdf_url,
                       c.status, c.content_hash, c.error_message, c.detected_at,
                       c.created_at, c.updated_at, c.es_indexed_at, c.es_chunk_count,
                       c.es_index_name
                FROM circulars c
                LEFT JOIN processing_tasks pt ON c.id = pt.circular_id AND pt.processor_name = :1
                WHERE c.status = 'FETCHED'
                  AND c.pdf_url IS NOT NULL
                  AND (pt.id IS NULL OR pt.status = 'FAILED' OR pt.status = 'PENDING')
                ORDER BY c.issue_date DESC, c.created_at ASC
                FETCH FIRST :2 ROWS ONLY
                """,
                (processor_name, limit),
            )
            rows = cursor.fetchall()
        return [r for row in rows if (r := self.circular_repo._row_to_record(row))]

    def mark_task_completed(self, circular_id: UUID, processor_name: str) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                MERGE INTO processing_tasks dst
                USING (SELECT :1 AS cid, :2 AS pname, 'COMPLETED' AS status FROM DUAL) src
                ON (dst.circular_id = src.cid AND dst.processor_name = src.pname)
                WHEN MATCHED THEN UPDATE SET status = 'COMPLETED', error_message = NULL, updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN INSERT (circular_id, processor_name, status)
                    VALUES (src.cid, src.pname, 'COMPLETED')
                """,
                (_uuid_to_raw(circular_id), processor_name),
            )
            conn.commit()

    def mark_task_failed(self, circular_id: UUID, processor_name: str, error_message: str) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                MERGE INTO processing_tasks dst
                USING (SELECT :1 AS cid, :2 AS pname, 'FAILED' AS status, :3 AS err FROM DUAL) src
                ON (dst.circular_id = src.cid AND dst.processor_name = src.pname)
                WHEN MATCHED THEN UPDATE SET status = 'FAILED', error_message = src.err, updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN INSERT (circular_id, processor_name, status, error_message)
                    VALUES (src.cid, src.pname, 'FAILED', src.err)
                """,
                (_uuid_to_raw(circular_id), processor_name, error_message),
            )
            conn.commit()

    def get_completed_by_processor(self, processor_name: str) -> list[tuple[bytes, str]]:
        """Get circular_id (raw) for completed tasks by processor name."""
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT pt.circular_id, c.circular_id
                FROM processing_tasks pt
                JOIN circulars c ON pt.circular_id = c.id
                WHERE pt.status = 'COMPLETED'
                AND pt.processor_name = :1
                ORDER BY pt.updated_at ASC
                """,
                (processor_name,),
            )
            return [(row[0], row[1]) for row in cursor.fetchall()]