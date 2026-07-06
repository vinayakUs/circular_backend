from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID


@dataclass(slots=True)
class UserRecord:
    id: UUID
    user_id: str
    department_id: UUID
    created_at: datetime
    created_by: str
    updated_at: datetime | None
    updated_by: str | None


class UsersRepository:

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("UsersRepository requires db_pool")
        self.logger = __import__("logging").getLogger(__name__)
        self.db_pool = db_pool

    def add_user(self, user_id: str, department_id: UUID, created_by: str) -> UserRecord | None:
        """Add a user to a department. Returns None if user already exists."""
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            # Check if user already exists
            cursor.execute("SELECT id FROM users WHERE user_id = %s", (user_id,))
            if cursor.fetchone():
                return None
            cursor.execute(
                """
                INSERT INTO users (user_id, department_id, created_by)
                VALUES (%s, %s, %s)
                RETURNING id, user_id, department_id, created_at, created_by, updated_at, updated_by
                """,
                (user_id, str(department_id), created_by),
            )
            row = cursor.fetchone()
            conn.commit()
        return self._row_to_record(row)

    def remove_user(self, user_id: str) -> bool:
        """Remove a user by user_id. Returns True if deleted."""
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM users WHERE user_id = %s RETURNING id", (user_id,))
            result = cursor.fetchone()
            conn.commit()
        return result is not None

    def get_user(self, user_id: str) -> UserRecord | None:
        """Get a user by user_id."""
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, user_id, department_id, created_at, created_by, updated_at, updated_by
                FROM users WHERE user_id = %s
                """,
                (user_id,),
            )
            row = cursor.fetchone()
        return self._row_to_record(row)

    def get_users_by_department(self, department_id: UUID) -> list[UserRecord]:
        """List all users in a department."""
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, user_id, department_id, created_at, created_by, updated_at, updated_by
                FROM users WHERE department_id = %s ORDER BY created_at
                """,
                (str(department_id),),
            )
            rows = cursor.fetchall()
        return [r for row in rows if (r := self._row_to_record(row))]

    def get_departments_by_user(self, user_id: str) -> list[UserRecord]:
        """List all department assignments for a user."""
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, user_id, department_id, created_at, created_by, updated_at, updated_by
                FROM users WHERE user_id = %s ORDER BY created_at
                """,
                (user_id,),
            )
            rows = cursor.fetchall()
        return [r for row in rows if (r := self._row_to_record(row))]

    def _row_to_record(self, row: Any) -> UserRecord | None:
        if row is None:
            return None
        return UserRecord(
            id=row[0],
            user_id=row[1],
            department_id=row[2],
            created_at=row[3],
            created_by=row[4],
            updated_at=row[5],
            updated_by=row[6],
        )
