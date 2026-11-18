"""
Immediate per-source subscriptions: a subscribed source emails the user
every new circular from it, one per circular — never a digest.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

SOURCES = {"SEBI", "SEBI_MASTER", "NSE"}


@dataclass(slots=True, frozen=True)
class SourceSubscriber:
    user_db_id: UUID
    user_id: str          # LDAP uid
    email: str | None
    source: str


class UserSourceSubscriptionsRepository:
    """db_pool may be None at construction — lazy-loaded from the singleton
    postgres client on first use (matches UserTopicRepository)."""

    def __init__(self, db_pool: Any = None):
        self.logger = logging.getLogger(__name__)
        self._db_pool = db_pool

    @property
    def db_pool(self) -> Any:
        if self._db_pool is None:
            from db.postgres_client import get_postgres_client
            self._db_pool = get_postgres_client().get_pool()
        return self._db_pool

    def list_for_user(self, user_id: UUID) -> list[str]:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT source FROM user_source_subscriptions "
                "WHERE user_id = %s ORDER BY source",
                (str(user_id),),
            )
            return [r[0] for r in cur.fetchall()]

    def list_subscribers_for_source(self, source: str) -> list[SourceSubscriber]:
        """Live users subscribed to `source` — the fan-out join."""
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT u.id, u.user_id, u.email, s.source
                FROM user_source_subscriptions s
                JOIN users u ON u.id = s.user_id
                WHERE s.source = %s AND u.is_deleted = FALSE
                ORDER BY u.user_id
                """,
                (source,),
            )
            return [
                SourceSubscriber(user_db_id=r[0], user_id=r[1], email=r[2], source=r[3])
                for r in cur.fetchall()
            ]

    def set_sources(self, user_id: UUID, sources: list[str]) -> list[str]:
        """Replace the user's subscription set in one transaction."""
        unknown = set(sources) - SOURCES
        if unknown:
            raise ValueError(f"unknown source(s): {sorted(unknown)}")
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM user_source_subscriptions WHERE user_id = %s",
                (str(user_id),),
            )
            for source in sorted(set(sources)):
                cur.execute(
                    "INSERT INTO user_source_subscriptions (user_id, source) "
                    "VALUES (%s, %s) ON CONFLICT (user_id, source) DO NOTHING",
                    (str(user_id), source),
                )
            conn.commit()
        return self.list_for_user(user_id)
