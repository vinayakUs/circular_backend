"""
User topics: CRUD for the user_topics table.

Methods are user-scoped — every read and write takes user_id so one user
can never touch another's topics. `create()` returns None on a duplicate
topic name (ON CONFLICT DO NOTHING, race-free); `update()` is partial —
only the fields passed are changed.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(slots=True, frozen=True)
class UserTopic:
    """One row of the user_topics table."""
    id: UUID
    user_id: UUID
    topic_name: str
    keywords: list[str]
    sources: list[str]
    alert_level: str        # IMMEDIATE | DAILY | WEEKLY | OFF
    created_at: datetime
    updated_at: datetime


class UserTopicRepository:
    """Persistence layer for the user_topics table.

    db_pool may be None at construction time — lazy-loaded from the
    singleton postgres client on first use (matches
    NotificationOutboxRepository).
    """

    def __init__(self, db_pool: Any = None):
        self.logger = logging.getLogger(__name__)
        self._db_pool = db_pool

    @property
    def db_pool(self) -> Any:
        if self._db_pool is None:
            from db.postgres_client import get_postgres_client
            self._db_pool = get_postgres_client().get_pool()
        return self._db_pool

    def list_by_user(self, user_id: UUID) -> list[UserTopic]:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, user_id, topic_name, keywords, sources,
                       alert_level, created_at, updated_at
                FROM user_topics
                WHERE user_id = %s
                ORDER BY created_at
                """,
                (str(user_id),),
            )
            return [self._row_to_record(row) for row in cur.fetchall()]

    def get(self, user_id: UUID, topic_id: UUID) -> UserTopic | None:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, user_id, topic_name, keywords, sources,
                       alert_level, created_at, updated_at
                FROM user_topics
                WHERE id = %s AND user_id = %s
                """,
                (str(topic_id), str(user_id)),
            )
            return self._row_to_record(cur.fetchone())

    def list_with_owner_email(self, alert_level: str) -> list[tuple[UserTopic, str]]:
        """All topics at a given alert level across users, with owner email.
        Producer-side read (digest fan-out) — not user-scoped."""
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT t.id, t.user_id, t.topic_name, t.keywords, t.sources,
                       t.alert_level, t.created_at, t.updated_at, u.email
                FROM user_topics t
                JOIN users u ON u.id = t.user_id
                WHERE t.alert_level = %s
                ORDER BY t.user_id, t.created_at
                """,
                (alert_level,),
            )
            return [
                (self._row_to_record(row[:8]), row[8])
                for row in cur.fetchall()
            ]

    def create(
        self,
        *,
        user_id: UUID,
        topic_name: str,
        keywords: list[str],
        sources: list[str],
        alert_level: str,
    ) -> UserTopic | None:
        """Insert a topic. Returns None if (user_id, topic_name) already exists."""
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO user_topics
                    (user_id, topic_name, keywords, sources, alert_level)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (user_id, topic_name) DO NOTHING
                RETURNING id, user_id, topic_name, keywords, sources,
                          alert_level, created_at, updated_at
                """,
                (str(user_id), topic_name, json.dumps(keywords),
                 json.dumps(sources), alert_level),
            )
            row = cur.fetchone()
            conn.commit()
            return self._row_to_record(row)

    def update(
        self,
        user_id: UUID,
        topic_id: UUID,
        *,
        topic_name: str | None = None,
        keywords: list[str] | None = None,
        sources: list[str] | None = None,
        alert_level: str | None = None,
    ) -> UserTopic | None:
        """Partial update of a user's topic. Returns None if no such topic.
        Renaming to an existing name raises IntegrityError (unique constraint)."""
        sets = ["updated_at = NOW()"]
        params: list[Any] = []
        if topic_name is not None:
            sets.append("topic_name = %s")
            params.append(topic_name)
        if keywords is not None:
            sets.append("keywords = %s")
            params.append(json.dumps(keywords))
        if sources is not None:
            sets.append("sources = %s")
            params.append(json.dumps(sources))
        if alert_level is not None:
            sets.append("alert_level = %s")
            params.append(alert_level)

        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                UPDATE user_topics
                SET {', '.join(sets)}
                WHERE id = %s AND user_id = %s
                RETURNING id, user_id, topic_name, keywords, sources,
                          alert_level, created_at, updated_at
                """,
                (*params, str(topic_id), str(user_id)),
            )
            row = cur.fetchone()
            conn.commit()
            return self._row_to_record(row)

    def delete(self, user_id: UUID, topic_id: UUID) -> bool:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM user_topics WHERE id = %s AND user_id = %s",
                (str(topic_id), str(user_id)),
            )
            conn.commit()
            return cur.rowcount > 0

    @staticmethod
    def _row_to_record(row):
        if row is None:
            return None
        keywords = row[3]
        sources = row[4]
        return UserTopic(
            id=row[0],
            user_id=row[1],
            topic_name=row[2],
            keywords=json.loads(keywords) if isinstance(keywords, str) else (keywords or []),
            sources=json.loads(sources) if isinstance(sources, str) else (sources or []),
            alert_level=row[5],
            created_at=row[6],
            updated_at=row[7],
        )