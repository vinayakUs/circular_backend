from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(slots=True)
class CommentRecord:
    id: UUID
    expert_id: UUID
    user_db_id: UUID
    author_user_id: str
    author_name: str | None
    author_email: str | None
    text: str
    created_at: datetime


class CommentsRepository:

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("CommentsRepository requires db_pool")
        self.logger = __import__("logging").getLogger(__name__)
        self.db_pool = db_pool

    def get_by_expert_id(self, expert_id: UUID) -> list[CommentRecord]:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT c.id, c.expert_id, c.user_db_id, c.text, c.created_at,
                       u.user_id, u.name, u.email
                FROM comments c
                JOIN users u ON u.id = c.user_db_id
                WHERE c.expert_id = %s
                ORDER BY c.created_at ASC
                """,
                (str(expert_id),)
            )
            rows = cursor.fetchall()
            return [
                CommentRecord(
                    id=row[0],
                    expert_id=row[1],
                    user_db_id=row[2],
                    text=row[3],
                    created_at=row[4],
                    author_user_id=row[5],
                    author_name=row[6],
                    author_email=row[7],
                )
                for row in rows
            ]

    def create(
        self, expert_id: UUID, user_db_id: UUID, text: str
    ) -> CommentRecord:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                WITH ins AS (
                    INSERT INTO comments (expert_id, user_db_id, text)
                    VALUES (%s, %s, %s)
                    RETURNING id, expert_id, user_db_id, text, created_at
                )
                SELECT ins.id, ins.expert_id, ins.user_db_id, ins.text, ins.created_at,
                       u.user_id, u.name, u.email
                FROM ins JOIN users u ON u.id = ins.user_db_id
                """,
                (str(expert_id), str(user_db_id), text),
            )
            row = cursor.fetchone()
            conn.commit()
            return CommentRecord(
                id=row[0],
                expert_id=row[1],
                user_db_id=row[2],
                text=row[3],
                created_at=row[4],
                author_user_id=row[5],
                author_name=row[6],
                author_email=row[7],
            )

    def _insert_in_tx(
        self, cursor, expert_id: UUID, user_db_id: UUID, text: str
    ) -> CommentRecord:
        """Insert a comment using the caller's cursor. Caller owns commit.

        Used by CommentsService when the comment must commit atomically with
        follow-up writes (e.g. mention fan-out).
        """
        cursor.execute(
            """
            WITH ins AS (
                INSERT INTO comments (expert_id, user_db_id, text)
                VALUES (%s, %s, %s)
                RETURNING id, expert_id, user_db_id, text, created_at
            )
            SELECT ins.id, ins.expert_id, ins.user_db_id, ins.text, ins.created_at,
                   u.user_id, u.name, u.email
            FROM ins JOIN users u ON u.id = ins.user_db_id
            """,
            (str(expert_id), str(user_db_id), text),
        )
        r = cursor.fetchone()
        return CommentRecord(
            id=r[0],
            expert_id=r[1],
            user_db_id=r[2],
            text=r[3],
            created_at=r[4],
            author_user_id=r[5],
            author_name=r[6],
            author_email=r[7],
        )
