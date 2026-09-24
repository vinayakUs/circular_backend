"""psycopg implementation of :class:`CaptchaImageRepository`."""

import logging
from typing import Any, override
from uuid import UUID
from app.auth.captcha.repository.captcha_image_repository import CaptchaImageRepository

class PostgresCaptchaImageRepository(CaptchaImageRepository):

    def __init__(self, db_pool: Any) -> None:
        """
        Build the repository against an existing
        psycopg connection pool.
        """

        if db_pool is None:
            raise ValueError(
                "PostgresCaptchaImageRepository requires db_pool"
            )

        self.logger = logging.getLogger(__name__)
        self.db_pool = db_pool

    @override            
    def create(
        self,
        *,
        challenge_id: UUID,
        png_bytes: bytes,
        content_type: str,
    ) -> None:
        """
        Insert one CAPTCHA image into the database.
        """

        with self.db_pool.acquire() as conn:
            cur = conn.cursor()

            cur.execute(
                """
                INSERT INTO captcha_images
                    (challenge_id, png_bytes, content_type, byte_len)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    str(challenge_id),
                    png_bytes,
                    content_type,
                    len(png_bytes),
                ),
            )

            conn.commit()

    @override            
    def get(
        self,
        challenge_id: UUID,
    ) -> tuple[bytes, str] | None:
        """
        Return (png_bytes, content_type) for the challenge.

        Returns None if no image exists.
        """

        with self.db_pool.acquire() as conn:
            cur = conn.cursor()

            cur.execute(
                """
                SELECT png_bytes, content_type
                FROM captcha_images
                WHERE challenge_id = %s
                """,
                (str(challenge_id),),
            )

            row = cur.fetchone()

        if row is None:
            return None

        return row[0], row[1]

    @override            
    def delete(self, challenge_id: UUID) -> bool:
        """
        Delete the CAPTCHA image.

        Returns:
            True  -> row was deleted
            False -> no row existed
        """

        with self.db_pool.acquire() as conn:
            cur = conn.cursor()

            cur.execute(
                """
                DELETE FROM captcha_images
                WHERE challenge_id = %s
                RETURNING challenge_id
                """,
                (str(challenge_id),),
            )

            row = cur.fetchone()
            conn.commit()

        return row is not None