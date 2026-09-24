"""PostgresMfaChallengeRepository — Postgres impl of MfaChallengeRepository."""
from __future__ import annotations

import hmac
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from app.auth.mfa.repository.mfa_challenge_repository import (
    ConsumeResult,
    MfaChallenge,
    MfaChallengeRepository,
)


logger = logging.getLogger(__name__)


def _const_time_eq(a: str, b: str) -> bool:
    """Constant-time string comparison (prevents timing attacks)."""
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def _now() -> datetime:
    return datetime.now(timezone.utc)


class PostgresMfaChallengeRepository(MfaChallengeRepository):

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError(
                "PostgresMfaChallengeRepository requires db_pool"
            )
        self.db_pool = db_pool

    def create(
        self,
        *,
        user_db_id: UUID,
        channel: str,
        code_hash: str,
        ttl_seconds: int,
    ) -> MfaChallenge:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO mfa_challenges
                    (user_db_id, channel, code_hash, expires_at)
                VALUES (%s, %s, %s, NOW() + make_interval(secs => %s))
                RETURNING id, user_db_id, channel, code_hash,
                          attempts, consumed, created_at, expires_at
                """,
                (str(user_db_id), channel, code_hash, ttl_seconds),
            )
            row = cur.fetchone()
            conn.commit()
        return self._row_to_record(row)

    def get_active(self, user_db_id: UUID) -> MfaChallenge | None:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, user_db_id, channel, code_hash,
                       attempts, consumed, created_at, expires_at
                FROM mfa_challenges
                WHERE user_db_id = %s
                  AND consumed = FALSE
                  AND expires_at > NOW()
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (str(user_db_id),),
            )
            row = cur.fetchone()
        return self._row_to_record(row)

    def consume_if_match_and_under_attempts(
        self,
        *,
        challenge_id: UUID,
        candidate_hash: str,
        max_attempts: int,
    ) -> ConsumeResult:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            # Step 1: atomically increment attempts if challenge is still
            # valid (not consumed, not expired, attempts under limit).
            # The UPDATE acquires a row-level lock so two concurrent
            # verify calls cannot both win.
            cur.execute(
                """
                UPDATE mfa_challenges
                SET attempts = attempts + 1
                WHERE id = %s
                  AND consumed = FALSE
                  AND expires_at > NOW()
                  AND attempts < %s
                RETURNING attempts, code_hash
                """,
                (str(challenge_id), max_attempts),
            )
            row = cur.fetchone()

            if row is None:
                # The UPDATE filtered us out — figure out why.
                cur.execute(
                    """
                    SELECT consumed, expires_at, attempts
                    FROM mfa_challenges
                    WHERE id = %s
                    """,
                    (str(challenge_id),),
                )
                state = cur.fetchone()
                conn.commit()
                if state is None:
                    return ConsumeResult.EXPIRED
                consumed, expires_at, attempts = state
                if consumed:
                    return ConsumeResult.CONSUMED
                if expires_at <= _now():
                    return ConsumeResult.EXPIRED
                if attempts >= max_attempts:
                    return ConsumeResult.LOCKED
                return ConsumeResult.EXPIRED

            attempts_after, stored_hash = row

            if not _const_time_eq(candidate_hash, stored_hash):
                # Wrong code. If this was the final allowed attempt, lock.
                if attempts_after >= max_attempts:
                    cur.execute(
                        """
                        UPDATE mfa_challenges
                        SET consumed = TRUE
                        WHERE id = %s AND consumed = FALSE
                        """,
                        (str(challenge_id),),
                    )
                    conn.commit()
                    return ConsumeResult.LOCKED
                conn.commit()
                return ConsumeResult.MISMATCH

            # Match: mark consumed atomically.
            cur.execute(
                """
                UPDATE mfa_challenges
                SET consumed = TRUE
                WHERE id = %s AND consumed = FALSE
                """,
                (str(challenge_id),),
            )
            conn.commit()
            return ConsumeResult.MATCH

    def consume_prior_for_user(self, user_db_id: UUID) -> int:
        with self.db_pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE mfa_challenges
                SET consumed = TRUE
                WHERE user_db_id = %s AND consumed = FALSE
                RETURNING id
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
        return MfaChallenge(
            id=row[0],
            user_db_id=row[1],
            channel=row[2],
            code_hash=row[3],
            attempts=row[4],
            consumed=row[5],
            created_at=row[6],
            expires_at=row[7],
        )
