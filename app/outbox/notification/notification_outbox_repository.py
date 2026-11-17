
"""
Notification outbox repository for managing notification delivery jobs.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(slots=True, frozen=True)
class NotificationOutboxJob:
    """Notification outbox record."""
    id: UUID
    user_id: UUID | None
    recipient_email: str
    channel: str                 # 'EMAIL' (SMS/PUSH later)
    template: str
    params: dict[str, Any]
    scheduled_at: datetime
    status: str                  # PENDING | PROCESSING | SENT | FAILED
    attempt_count: int
    max_attempts: int
    locked_by: str | None
    locked_at: datetime | None
    idempotency_key: str
    created_at: datetime
    updated_at: datetime
    sent_at: datetime | None
    last_error: str | None


class NotificationOutboxRepository:
    """Repository for notification outbox operations."""

    def __init__(self, db_pool: Any = None):
        self.logger = logging.getLogger(__name__)
        self._db_pool = db_pool

    @property
    def db_pool(self) -> Any:
        if self._db_pool is None:
            from db.postgres_client import get_postgres_client
            self._db_pool = get_postgres_client().get_pool()
        return self._db_pool

    def enqueue(
        self,
        cursor,
        *,
        recipient_email: str,
        channel: str,
        template: str,
        idempotency_key: str,
        user_id: UUID | None = None,
        params: dict[str, Any] | None = None,
        scheduled_at: datetime | None = None,
        max_attempts: int = 3,
    ) -> NotificationOutboxJob | None:
        """Insert a notification job. Returns None if the idempotency key
        already exists. The caller manages the transaction."""
        cursor.execute(
            """
            INSERT INTO notification_outbox
                (user_id, recipient_email, channel, template, params,
                 scheduled_at, max_attempts, idempotency_key)
            VALUES (%s, %s, %s, %s, %s, COALESCE(%s, NOW()), %s, %s)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id, user_id, recipient_email, channel, template, params,
                      scheduled_at, status, attempt_count, max_attempts,
                      locked_by, locked_at, idempotency_key, created_at,
                      updated_at, sent_at, last_error
            """,
            (str(user_id) if user_id else None, recipient_email, channel,
             template, json.dumps(params or {}), scheduled_at,
             max_attempts, idempotency_key),
        )
        return self._row_to_record(cursor.fetchone())

    def claim(
        self,
        worker_id: str,
        *,
        limit: int = 10,
    ) -> list[NotificationOutboxJob]:
        """Claim due PENDING jobs and mark them as PROCESSING."""
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE notification_outbox
                SET status = 'PROCESSING',
                    locked_by = %s,
                    locked_at = NOW(),
                    attempt_count = attempt_count + 1,
                    updated_at = NOW()
                WHERE id IN (
                    SELECT id FROM notification_outbox
                    WHERE status = 'PENDING'
                      AND scheduled_at <= NOW()
                    ORDER BY scheduled_at, created_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, user_id, recipient_email, channel, template,
                          params, scheduled_at, status, attempt_count,
                          max_attempts, locked_by, locked_at, idempotency_key,
                          created_at, updated_at, sent_at, last_error
                """,
                (worker_id, limit),
            )
            rows = cur.fetchall()
            conn.commit()
            return [self._row_to_record(row) for row in rows]

    def mark_sent(self, job_id: UUID) -> bool:
        """Mark a job as SENT."""
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE notification_outbox
                SET status = 'SENT', sent_at = NOW(),
                    locked_by = NULL, locked_at = NULL, updated_at = NOW()
                WHERE id = %s AND status = 'PROCESSING'
                """,
                (str(job_id),),
            )
            conn.commit()
            return cur.rowcount > 0

    def mark_failed(
        self, job_id: UUID, error: str, *, retry_delay_seconds: int = 60
    ) -> str | None:
        """Record a delivery failure. Retry if attempts remain, otherwise
        mark the job as FAILED. Returns the new status."""
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE notification_outbox
                SET status = CASE WHEN attempt_count >= max_attempts
                                  THEN 'FAILED' ELSE 'PENDING' END,
                    last_error = %s,
                    scheduled_at = CASE
                        WHEN attempt_count >= max_attempts THEN scheduled_at
                        ELSE NOW() + make_interval(secs => attempt_count * %s)
                    END,
                    locked_by = NULL, locked_at = NULL, updated_at = NOW()
                WHERE id = %s
                RETURNING status
                """,
                (error, retry_delay_seconds, str(job_id)),
            )
            row = cur.fetchone()
            conn.commit()
            return row[0] if row else None

    def release_stale(self, *, stale_before: datetime) -> int:
        """Release PROCESSING jobs with expired worker locks."""
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE notification_outbox
                SET status = 'PENDING',
                    locked_by = NULL, locked_at = NULL, updated_at = NOW()
                WHERE status = 'PROCESSING' AND locked_at < %s
                """,
                (stale_before,),
            )
            conn.commit()
            return cur.rowcount

    def get_by_id(self, job_id: UUID) -> NotificationOutboxJob | None:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, user_id, recipient_email, channel, template, params,
                       scheduled_at, status, attempt_count, max_attempts,
                       locked_by, locked_at, idempotency_key, created_at,
                       updated_at, sent_at, last_error
                FROM notification_outbox
                WHERE id = %s
                """,
                (str(job_id),),
            )
            return self._row_to_record(cur.fetchone())

    @staticmethod
    def _row_to_record(row):
        if row is None:
            return None
        params = row[5]
        return NotificationOutboxJob(
            id=row[0],
            user_id=row[1],
            recipient_email=row[2],
            channel=row[3],
            template=row[4],
            params=json.loads(params) if isinstance(params, str) else (params or {}),
            scheduled_at=row[6],
            status=row[7],
            attempt_count=row[8],
            max_attempts=row[9],
            locked_by=row[10],
            locked_at=row[11],
            idempotency_key=row[12],
            created_at=row[13],
            updated_at=row[14],
            sent_at=row[15],
            last_error=row[16],
        )
