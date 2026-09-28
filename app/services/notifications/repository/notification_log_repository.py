from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any
from uuid import UUID

from db.postgres_client import get_postgres_client


class NotificationLogRepository:
    def __init__(self, db_pool: Any = None):
        self.logger = logging.getLogger(__name__)
        self._db_pool = db_pool

    @property
    def db_pool(self) -> Any:
        if self._db_pool is None:
            self._db_pool = get_postgres_client().get_pool()
        return self._db_pool

    def create_log(
        self,
        template_name: str,
        recipient_email: str,
        subject: str,
        variables: dict[str, Any],
    ) -> UUID:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            variables_json = json.dumps(variables)
            cursor.execute(
                """
                INSERT INTO notification_logs (template_name, recipient_email, subject, variables, status)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (template_name, recipient_email, subject, variables_json, "PENDING"),
            )
            log_id = cursor.fetchone()[0]
            conn.commit()
            self.logger.info("Notification log created id=%s template=%s recipient=%s", log_id, template_name, recipient_email)
            return log_id

    def record_outcome(
        self,
        *,
        template_name: str | None,
        recipient_email: str,
        subject: str | None,
        variables: dict[str, Any],
        status: str,
        error_message: str | None,
        sent_at: datetime | None,
        correlation_id: str | None,
    ) -> UUID:
        """Insert a single audit row reflecting the final delivery outcome.

        Unlike create_log() + mark_sent()/mark_failed() (the 3-step flow
        the old EmailService used), this captures the outcome in one shot
        — fits the new pipeline where the logger only sees the final
        DeliveryResult, not the in-flight state.

        Args:
            template_name: name of the rendered template.
            recipient_email: comma-joined list of recipient addresses.
            subject: email subject line.
            variables: dict passed to the renderer.
            status: final status — "SENT" or "FAILED".
            error_message: populated on failure (code + detail).
            sent_at: delivery completion timestamp (success only).
            correlation_id: per-call tracing token from the request.

        Returns:
            UUID of the inserted row.
        """
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            variables_json = json.dumps(variables)
            cursor.execute(
                """
                INSERT INTO notification_logs (
                    template_name, recipient_email, subject, variables,
                    status, error_message, sent_at, correlation_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    template_name,
                    recipient_email,
                    subject,
                    variables_json,
                    status,
                    error_message,
                    sent_at,
                    correlation_id,
                ),
            )
            log_id = cursor.fetchone()[0]
            conn.commit()
            self.logger.info(
                "Notification log recorded id=%s status=%s recipient=%s correlation=%s",
                log_id, status, recipient_email, correlation_id,
            )
            return log_id

    def mark_sent(self, log_id: UUID) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE notification_logs
                SET status = 'SENT', sent_at = NOW()
                WHERE id = %s
                """,
                (str(log_id),),
            )
            conn.commit()
        self.logger.info("Notification log marked sent id=%s", log_id)

    def mark_failed(self, log_id: UUID, error_message: str) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE notification_logs
                SET status = 'FAILED', error_message = %s
                WHERE id = %s
                """,
                (error_message, str(log_id)),
            )
            conn.commit()
        self.logger.warning("Notification log marked failed id=%s error=%s", log_id, error_message)

    def get_logs_by_status(self, status: str, limit: int = 100) -> list[dict[str, Any]]:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, template_name, recipient_email, subject, variables, status, error_message, sent_at, created_at
                FROM notification_logs
                WHERE status = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (status, limit),
            )
            rows = cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "template_name": row[1],
                    "recipient_email": row[2],
                    "subject": row[3],
                    "variables": json.loads(row[4]) if isinstance(row[4], str) else (row[4] or {}),
                    "status": row[5],
                    "error_message": row[6],
                    "sent_at": row[7],
                    "created_at": row[8],
                }
                for row in cursor.fetchall()
            ]

    def is_circular_notified(self, circular_id: str) -> bool:
        """Check if a circular has already been notified (SENT notification exists)."""
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM notification_logs
                WHERE status = 'SENT'
                AND template_name = %s
                AND (variables->>'circular_id' = %s OR variables->>'circular_uuid' = %s)
                """,
                ("circular_notification.html", circular_id, circular_id),
            )
            count = cursor.fetchone()[0]
            return count > 0

    def get_notified_circular_ids(self, circular_ids: list[str]) -> set[str]:
        """Given a list of circular_ids (UUIDs), return those that have already been notified."""
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"[NOTIFICATION_LOG_REPO] get_notified_circular_ids called with: {circular_ids}")

        if not circular_ids:
            return set()
        placeholders = ",".join(["%s"] * len(circular_ids))
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT DISTINCT variables->>'circular_uuid'
                FROM notification_logs
                WHERE status = 'SENT'
                AND template_name = 'circular_notification.html'
                AND variables->>'circular_uuid' IN ({placeholders})
                """,
                circular_ids,
            )
            result = {row[0] for row in cursor.fetchall() if row[0]}
            logger.info(f"[NOTIFICATION_LOG_REPO] Query returned notified IDs: {result}")
            return result