


import logging
from typing import Any, override
from uuid import UUID
from app.auth.captcha.repository.captcha_repository import CaptchaChallenge, CaptchaChallengeRepository


class PostgresCaptchaChallengeRepository(CaptchaChallengeRepository):
    """psycopg-backed implementation of :class:`CaptchaChallengeRepository"""

    def __init__(self, db_pool: Any):
        if db_pool is None:
            raise ValueError(
                "PostgresCaptchaChallengeRepository requires db_pool"
            )
        self.logger = logging.getLogger(__name__)
        self.db_pool = db_pool

    @override
    def create(
        self,
        *,
        answer_hash: str,
        ttl_seconds: int,
    ) -> CaptchaChallenge:

        with self.db_pool.acquire() as conn:
            cur = conn.cursor()

            cur.execute(
                """
                INSERT INTO captcha_challenges
                    (answer_hash, expires_at)
                VALUES (
                    %s,
                    NOW() + make_interval(secs => %s)
                )
                RETURNING
                    id,
                    answer_hash,
                    created_at,
                    expires_at,
                    consumed
                """,
                (
                    answer_hash,
                    ttl_seconds,
                ),
            )

            row = cur.fetchone()
            conn.commit()

        return self._row_to_record(row)


    @override
    def get_unconsumed(
        self,
        challenge_id: UUID,
    ) -> CaptchaChallenge | None:

        with self.db_pool.acquire() as conn:
            cur = conn.cursor()

            cur.execute(
                """
                SELECT
                    id,
                    answer_hash,
                    created_at,
                    expires_at,
                    consumed
                FROM captcha_challenges
                WHERE id = %s
                  AND consumed = FALSE
                  AND expires_at > NOW()
                """,
                (str(challenge_id),),
            )

            row = cur.fetchone()

        return self._row_to_record(row)

    @override
    def consume(
        self,
        challenge_id: UUID,
    ) -> bool:

        with self.db_pool.acquire() as conn:
            cur = conn.cursor()

            cur.execute(
                """
                UPDATE captcha_challenges
                SET consumed = TRUE
                WHERE id = %s
                  AND consumed = FALSE
                RETURNING id
                """,
                (str(challenge_id),),
            )

            row = cur.fetchone()
            conn.commit()

        return row is not None

    @override
    def purge_expired(self) -> int:

        with self.db_pool.acquire() as conn:
            cur = conn.cursor()

            cur.execute(
                """
                DELETE FROM captcha_challenges
                WHERE expires_at < NOW() - INTERVAL '1 day'
                """
            )

            count = cur.rowcount
            conn.commit()

        return count



    @staticmethod
    def _row_to_record(row: Any) -> CaptchaChallenge | None:

        if row is None:
            return None
        return CaptchaChallenge(
            id=row[0],
            answer_hash=row[1],
            created_at=row[2],
            expires_at=row[3],
            consumed=row[4],
        )