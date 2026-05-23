from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from uuid import UUID

from ingestion.repository.circular_repository import (
    CircularRecord,
)


class ProcessorRepository:

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("ProcessorRepository requires db_pool")
        self.logger = __import__("logging").getLogger(__name__)
        self.db_pool = db_pool

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
                LEFT JOIN processing_tasks pt ON c.id = pt.circular_id AND pt.processor_name = %s
                WHERE c.status = 'FETCHED'
                  AND c.pdf_url IS NOT NULL
                  AND (pt.id IS NULL OR pt.status = 'FAILED' OR pt.status = 'PENDING')
                ORDER BY c.issue_date DESC, c.created_at ASC
                LIMIT %s
                """,
                (processor_name, limit),
            )
            rows = cursor.fetchall()
        return [r for row in rows if (r := self._row_to_record(row))]

    def mark_task_completed(self, circular_id: UUID, processor_name: str) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO processing_tasks (circular_id, processor_name, status, error_message, updated_at)
                VALUES (%s, %s, 'COMPLETED', NULL, NOW())
                ON CONFLICT (circular_id, processor_name) DO UPDATE SET
                    status = 'COMPLETED',
                    error_message = NULL,
                    updated_at = NOW()
                """,
                (str(circular_id), processor_name),
            )
            conn.commit()

    def mark_task_failed(self, circular_id: UUID, processor_name: str, error_message: str) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO processing_tasks (circular_id, processor_name, status, error_message, updated_at)
                VALUES (%s, %s, 'FAILED', %s, NOW())
                ON CONFLICT (circular_id, processor_name) DO UPDATE SET
                    status = 'FAILED',
                    error_message = %s,
                    updated_at = NOW()
                """,
                (str(circular_id), processor_name, error_message, error_message),
            )
            conn.commit()

    def get_completed_by_processor(self, processor_name: str) -> list[tuple[UUID, str]]:
        """Get circular_id (UUID) for completed tasks by processor name."""
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT pt.circular_id, c.circular_id
                FROM processing_tasks pt
                JOIN circulars c ON pt.circular_id = c.id
                WHERE pt.status = 'COMPLETED'
                AND pt.processor_name = %s
                ORDER BY pt.updated_at ASC
                """,
                (processor_name,),
            )
            return [(row[0], row[1]) for row in cursor.fetchall()]

    def _row_to_record(self, row: Any) -> CircularRecord | None:
        """Convert a task query row (20 columns) to CircularRecord."""
        if row is None:
            return None
        return CircularRecord(
            id=row[0],
            source=row[1],
            circular_id=row[2],
            source_item_key=row[3] or "",
            full_reference=row[4],
            department=row[5] or "",
            title=row[6],
            issue_date=row[7],
            effective_date=None,
            url=row[9] or "",
            pdf_url=row[10] or "",
            content_hash=row[12],
            status=row[11],
            error_message=row[13],
            detected_at=row[14],
            created_at=row[15],
            updated_at=row[16],
            es_indexed_at=row[17] if row[17] is not None else None,
            es_chunk_count=int(row[18]) if row[18] is not None else None,
            es_index_name=row[19] if row[19] is not None else None,
            applicable_to_nse=bool(row[8]) if row[8] is not None else False,
            signatory=[],
        )