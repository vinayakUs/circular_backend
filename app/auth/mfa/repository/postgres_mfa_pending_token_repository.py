"""PostgresMfaPendingTokenRepository — Postgres impl."""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from app.auth.mfa.repository.mfa_pending_token_repository import (
    MfaPendingToken,
    MfaPendingTokenRepository,
)


logger = logging.getLogger(__name__)


class PostgresMfaPendingTokenRepository(MfaPendingTokenRepository):

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError(
                "PostgresMfaPendingTokenRepository requires db_pool"
            )
        self.db_pool = db_pool

    def create(
        self,
        *,
        user_db_id: UUID,
        token_hash: str,
        issued_ip: str | None,
        issued_user_agent: str | None,
        ttl_seconds: int,
    ) -> MfaPendingToken:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO mfa_pending_tokens
                    (token_hash, user_db_id, issued_ip,
                     issued_user_agent, expires_at)
                VALUES (%s, %s, %s, %s, NOW() + make_interval(secs => %s))
                RETURNING token_id, token_hash, user_db_id, issued_ip,
                          issued_user_agent, consumed, created_at, expires_at
                """,
                (
                    token_hash,
                    str(user_db_id),
                    issued_ip,
                    issued_user_agent,
                    ttl_seconds,
                ),
            )
            row = cur.fetchone()
            conn.commit()
        return self._row_to_record(row)

    def get_active_by_hash(self, token_hash: str) -> MfaPendingToken | None:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT token_id, token_hash, user_db_id, issued_ip,
                       issued_user_agent, consumed, created_at, expires_at
                FROM mfa_pending_tokens
                WHERE token_hash = %s
                  AND consumed = FALSE
                  AND expires_at > NOW()
                LIMIT 1
                """,
                (token_hash,),
            )
            row = cur.fetchone()
        return self._row_to_record(row)

    def consume(self, token_id: UUID) -> bool:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE mfa_pending_tokens
                SET consumed = TRUE
                WHERE token_id = %s AND consumed = FALSE
                RETURNING token_id
                """,
                (str(token_id),),
            )
            row = cur.fetchone()
            conn.commit()
        return row is not None

    def consume_prior_for_user(self, user_db_id: UUID) -> int:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE mfa_pending_tokens
                SET consumed = TRUE
                WHERE user_db_id = %s AND consumed = FALSE
                RETURNING token_id
                """,
                (str(user_db_id),),
            )
            rows = cur.fetchall()
            conn.commit()
        return len(rows)

    @staticmethod
    def _row_to_record(row):
        if row is None:
            return None
        return MfaPendingToken(
            token_id=row[0],
            token_hash=row[1],
            user_db_id=row[2],
            issued_ip=row[3],
            issued_user_agent=row[4],
            consumed=row[5],
            created_at=row[6],
            expires_at=row[7],
        )
