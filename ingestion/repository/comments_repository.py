from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(slots=True)
class CommentRecord:
    id: UUID
    expert_id: UUID
    user_id: str
    username: str
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
                SELECT id, expert_id, user_id, username, text, created_at
                FROM comments
                WHERE expert_id = %s
                ORDER BY created_at ASC
                """,
                (str(expert_id),)
            )
            rows = cursor.fetchall()
            return [
                CommentRecord(
                    id=row[0],
                    expert_id=row[1],
                    user_id=row[2],
                    username=row[3],
                    text=row[4],
                    created_at=row[5],
                )
                for row in rows
            ]

    def create(
        self, expert_id: UUID, user_id: str, username: str, text: str
    ) -> CommentRecord:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO comments (expert_id, user_id, username, text)
                VALUES (%s, %s, %s, %s)
                RETURNING id, expert_id, user_id, username, text, created_at
                """,
                (str(expert_id), user_id, username, text),
            )
            row = cursor.fetchone()
            conn.commit()
            return CommentRecord(
                id=row[0],
                expert_id=row[1],
                user_id=row[2],
                username=row[3],
                text=row[4],
                created_at=row[5],
            )
