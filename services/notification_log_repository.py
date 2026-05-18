from __future__ import annotations

import json
import logging
from typing import Any
from uuid import UUID

from db.client import get_db_client
from ingestion.repository._uuid_utils import _raw_to_uuid, _uuid_to_raw


class NotificationLogRepository:
    def __init__(self, db_pool: Any = None):
        self.logger = logging.getLogger(__name__)
        self._db_pool = db_pool

    @property
    def db_pool(self) -> Any:
        if self._db_pool is None:
            self._db_pool = get_db_client().get_pool()
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
            out_id = cursor.var(bytes)
            variables_json = json.dumps(variables)
            cursor.execute(
                """
                INSERT INTO notification_logs (template_name, recipient_email, subject, variables, status)
                VALUES (:1, :2, :3, :4, :5)
                RETURNING id INTO :6
                """,
                (template_name, recipient_email, subject, variables_json, "PENDING", out_id),
            )
            conn.commit()
            log_id = _raw_to_uuid(out_id.getvalue()[0])
            self.logger.info("Notification log created id=%s template=%s recipient=%s", log_id, template_name, recipient_email)
            return log_id

    def mark_sent(self, log_id: UUID) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE notification_logs
                SET status = 'SENT', sent_at = SYSTIMESTAMP
                WHERE id = :1
                """,
                (_uuid_to_raw(log_id),),
            )
            conn.commit()
        self.logger.info("Notification log marked sent id=%s", log_id)

    def mark_failed(self, log_id: UUID, error_message: str) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE notification_logs
                SET status = 'FAILED', error_message = :1
                WHERE id = :2
                """,
                (error_message, _uuid_to_raw(log_id)),
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
                WHERE status = :1
                ORDER BY created_at DESC
                FETCH FIRST :2 ROWS ONLY
                """,
                (status, limit),
            )
            rows = cursor.fetchall()
            return [
                {
                    "id": _raw_to_uuid(row[0]),
                    "template_name": row[1],
                    "recipient_email": row[2],
                    "subject": row[3],
                    "variables": json.loads(row[4]) if isinstance(row[4], str) else (row[4] or {}),
                    "status": row[5],
                    "error_message": row[6],
                    "sent_at": row[7],
                    "created_at": row[8],
                }
                for row in rows
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
                AND template_name = :1
                AND json_value(variables, '$.circular_id') = :2
                """,
                ("circular_notification.html", circular_id),
            )
            count = cursor.fetchone()[0]
            return count > 0

    def get_notified_circular_ids(self, circular_ids: list[str]) -> set[str]:
        """Given a list of circular_ids, return those that have already been notified."""
        if not circular_ids:
            return set()
        placeholders = ",".join([f":{i+1}" for i in range(len(circular_ids))])
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT DISTINCT json_value(variables, '$.circular_id')
                FROM notification_logs
                WHERE status = 'SENT'
                AND template_name = 'circular_notification.html'
                AND json_value(variables, '$.circular_id') IN ({placeholders})
                """,
                circular_ids,
            )
            return {row[0] for row in cursor.fetchall()}