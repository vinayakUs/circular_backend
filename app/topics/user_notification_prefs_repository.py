"""
Notification preferences: one row per user for the global email digest.

`digest_frequency` is DAILY / WEEKLY / NONE — it drives the scheduled digest
producer only; per-topic alert levels live in user_topics.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

DIGEST_FREQUENCIES = {"DAILY", "WEEKLY", "NONE"}


@dataclass(slots=True, frozen=True)
class UserNotificationPrefs:
    user_id: UUID
    digest_frequency: str  # DAILY | WEEKLY | NONE
    updated_at: datetime


class UserNotificationPrefsRepository:
    """db_pool may be None at construction time — lazy-loaded from the
    singleton postgres client on first use (matches UserTopicRepository)."""

    def __init__(self, db_pool: Any = None):
        self.logger = logging.getLogger(__name__)
        self._db_pool = db_pool

    @property
    def db_pool(self) -> Any:
        if self._db_pool is None:
            from db.postgres_client import get_postgres_client
            self._db_pool = get_postgres_client().get_pool()
        return self._db_pool

    def get(self, user_id: UUID) -> UserNotificationPrefs | None:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT user_id, digest_frequency, updated_at
                FROM user_notification_prefs
                WHERE user_id = %s
                """,
                (str(user_id),),
            )
            return self._row_to_record(cur.fetchone())

    def upsert(self, user_id: UUID, digest_frequency: str) -> UserNotificationPrefs:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO user_notification_prefs (user_id, digest_frequency)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE
                    SET digest_frequency = EXCLUDED.digest_frequency,
                        updated_at = NOW()
                RETURNING user_id, digest_frequency, updated_at
                """,
                (str(user_id), digest_frequency),
            )
            row = cur.fetchone()
            conn.commit()
            return self._row_to_record(row)

    @staticmethod
    def _row_to_record(row):
        if row is None:
            return None
        return UserNotificationPrefs(
            user_id=row[0],
            digest_frequency=row[1],
            updated_at=row[2],
        )