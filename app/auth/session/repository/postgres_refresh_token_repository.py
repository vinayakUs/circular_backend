"""PostgresRefreshTokenRepository — Postgres impl of RefreshTokenRepository."""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from app.auth.session.repository.refresh_token_repository import (
    RefreshToken,
    RefreshTokenRepository,
)


logger = logging.getLogger(__name__)


class PostgresRefreshTokenRepository(RefreshTokenRepository):
    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError(
                "PostgresRefreshTokenRepository requires db_pool"
            )
        self.db_pool = db_pool

    def create(
        self,
        *,
        user_db_id: UUID,
        token_hash: str,
        family_id: UUID,
        issued_ip: str | None,
        issued_user_agent: str | None,
        ttl_seconds: int,
    ) -> RefreshToken:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO refresh_tokens
                    (user_db_id, token_hash, family_id, issued_ip,
                     issued_user_agent, expires_at)
                VALUES (%s, %s, %s, %s, %s,
                        NOW() + make_interval(secs => %s))
                RETURNING id, user_db_id, token_hash, family_id, issued_ip,
                          issued_user_agent, consumed, consumed_at,
                          created_at, expires_at
                """,
                (
                    str(user_db_id),
                    token_hash,
                    str(family_id),
                    issued_ip,
                    issued_user_agent,
                    ttl_seconds,
                ),
            )
            row = cur.fetchone()
            conn.commit()
        return self._row_to_record(row)

    def get_by_hash(self, token_hash: str) -> RefreshToken | None:
        # No consumed/expires filter on purpose: a replayed token must be
        # distinguishable from an unknown one.
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, user_db_id, token_hash, family_id, issued_ip,
                       issued_user_agent, consumed, consumed_at,
                       created_at, expires_at
                FROM refresh_tokens
                WHERE token_hash = %s
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
                UPDATE refresh_tokens
                SET consumed = TRUE, consumed_at = NOW()
                WHERE id = %s AND consumed = FALSE
                RETURNING id
                """,
                (str(token_id),),
            )
            row = cur.fetchone()
            conn.commit()
        return row is not None

    def revoke_family(self, family_id: UUID) -> int:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE refresh_tokens
                SET consumed = TRUE, consumed_at = NOW()
                WHERE family_id = %s AND consumed = FALSE
                RETURNING id
                """,
                (str(family_id),),
            )
            rows = cur.fetchall()
            conn.commit()
        return len(rows)

    def revoke_all_for_user(self, user_db_id: UUID) -> int:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE refresh_tokens
                SET consumed = TRUE, consumed_at = NOW()
                WHERE user_db_id = %s AND consumed = FALSE
                RETURNING id
                """,
                (str(user_db_id),),
            )
            rows = cur.fetchall()
            conn.commit()
        return len(rows)

    def purge_family(self, family_id: UUID) -> int:
        # DELETE, not UPDATE: a consumed row sits inside the reuse grace
        # window, which would let a logged-out token mint a fresh session.
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                DELETE FROM refresh_tokens
                WHERE family_id = %s
                RETURNING id
                """,
                (str(family_id),),
            )
            rows = cur.fetchall()
            conn.commit()
        return len(rows)

    def delete_expired(self) -> int:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                DELETE FROM refresh_tokens
                WHERE expires_at <= NOW()
                RETURNING id
                """,
            )
            rows = cur.fetchall()
            conn.commit()
        return len(rows)

    @staticmethod
    def _row_to_record(row):
        if row is None:
            return None
        return RefreshToken(
            id=row[0],
            user_db_id=row[1],
            token_hash=row[2],
            family_id=row[3],
            issued_ip=row[4],
            issued_user_agent=row[5],
            consumed=row[6],
            consumed_at=row[7],
            created_at=row[8],
            expires_at=row[9],
        )
