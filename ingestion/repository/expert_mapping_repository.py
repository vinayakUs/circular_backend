from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import logging
from typing import Any
from uuid import UUID


@dataclass(slots=True)
class ExpertMappingRecord:
    id: UUID
    circular_id: UUID
    department_id: UUID
    expert_name: str
    highlight_text: str
    highlights: list[dict]
    created_by_user_id: UUID | None
    created_by_dep_id: UUID | None
    created_at: datetime
    updated_at: datetime


class ExpertMappingRepository:

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("ExpertMappingRepository requires db_pool")
        self.logger = __import__("logging").getLogger(__name__)
        self.db_pool = db_pool

    def _run(self, work, *, conn):
        """Run `work(conn)` on a caller-provided connection, or in a fresh transaction.

        Returns whatever `work` returns. When `conn` is provided, the caller owns
        the transaction lifecycle (use with `with conn.transaction():`). When `conn`
        is None, a connection is acquired and committed on success.
        """
        if conn is None:
            with self.db_pool.acquire() as c:
                result = work(c)
                c.commit()
                return result
        return work(conn)

    def acquire(self):
        """Acquire a connection from the pool. Caller owns the transaction lifecycle.

        Use for multi-step orchestration that needs atomicity across several
        repository calls: ``with self.repository.acquire() as conn: with conn.transaction(): ...``.
        """
        return self.db_pool.acquire()

    def save_expert_mapping(
        self,
        circular_id: UUID,
        title: str,
        text: str,
        highlights: list[dict],
        created_by_user_id: UUID | None = None,
        created_by_dep_id: UUID | None = None,
        *,
        conn: Any = None,
    ) -> UUID:
        def _work(c):
            cursor = c.cursor()
            cursor.execute(
                """
                INSERT INTO experts
                    (circular_id, expert_name, highlight_text, highlights,
                     created_by_user_id, created_by_dep_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    str(circular_id),
                    title,
                    text,
                    json.dumps(highlights),
                    str(created_by_user_id) if created_by_user_id else None,
                    str(created_by_dep_id) if created_by_dep_id else None,
                ),
            )
            return cursor.fetchone()[0]
        return self._run(_work, conn=conn)

    def update_expert_mapping(
        self,
        row_id: UUID,
        title: str,
        text: str,
        highlights: list[dict],
        *,
        conn: Any = None,
    ) -> bool:
        def _work(c):
            cursor = c.cursor()
            cursor.execute(
                """
                UPDATE experts
                SET expert_name = %s, highlight_text = %s,
                    highlights = %s, updated_at = NOW()
                WHERE id = %s
                RETURNING id
                """,
                (title, text, json.dumps(highlights), str(row_id)),
            )
            return cursor.fetchone() is not None
        return self._run(_work, conn=conn)

    def delete_expert_mapping(
        self, row_id: UUID, *, conn: Any = None,
    ) -> bool:
        def _work(c):
            cursor = c.cursor()
            cursor.execute(
                "DELETE FROM experts WHERE id = %s RETURNING id",
                (str(row_id),),
            )
            return cursor.fetchone() is not None
        return self._run(_work, conn=conn)

    def update_expert_status(self, expert_id: UUID, status: str) -> bool:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE experts SET status = %s, updated_at = NOW() WHERE id = %s RETURNING id",
                (status, str(expert_id)),
            )
            result = cursor.fetchone()
            conn.commit()
        return result is not None

    def get_expert_mappings_for_circular(self, circular_id: UUID) -> list[dict]:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT m.id, m.circular_id, m.expert_name, m.highlight_text,
                       m.highlights, m.created_at, m.updated_at, m.status,
                       m.created_by_user_id, m.created_by_dep_id,
                       u.user_id as created_by_username
                FROM experts m
                LEFT JOIN users u ON u.id = m.created_by_user_id
                WHERE m.circular_id = %s
                ORDER BY m.created_at
                """,
                (str(circular_id),),
            )
            rows = cursor.fetchall()
            if not rows:
                return []
            expert_ids = [r[0] for r in rows]
            cursor.execute(
                """
                SELECT edm.expert_id, edm.department_id, p.name
                FROM expert_departments_mapping edm
                LEFT JOIN properties p ON p.id = edm.department_id
                WHERE edm.expert_id = ANY(%s::uuid[])
                """,
                (expert_ids,),
            )
            dept_rows = cursor.fetchall()

        # Group depts by expert_id
        dept_by_expert: dict[str, list[dict]] = {}
        for expert_id, dept_id, dept_name in dept_rows:
            dept_by_expert.setdefault(str(expert_id), []).append(
                {"id": str(dept_id), "name": dept_name or ""}
            )

        return [
            {
                "id": str(r[0]),
                "circular_id": str(r[1]),
                "dept_ids": [d["id"] for d in dept_by_expert.get(str(r[0]), [])],
                "dept_names": [d["name"] for d in dept_by_expert.get(str(r[0]), [])],
                "title": r[2],
                "text": r[3],
                "highlights": json.loads(r[4]) if isinstance(r[4], str) else (r[4] if isinstance(r[4], list) else []),
                "status": r[7] or "open",
                "created_at": r[5].isoformat() if r[5] else None,
                "updated_at": r[6].isoformat() if r[6] else None,
                "created_by_user_id": str(r[8]) if r[8] else None,
                "created_by_dep_id": str(r[9]) if r[9] else None,
                "created_by_username": r[10] if r[10] else None,
            }
            for r in rows
        ]

    def get_existing_expert_ids(self, expert_ids: list) -> set:
        """Return the subset of `expert_ids` that currently exist in the experts table.

        Used by the service to pre-validate updates and return 404 before entering
        the write transaction. One round-trip regardless of input size.
        """
        if not expert_ids:
            return set()
        ids = [str(eid) for eid in expert_ids]
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM experts WHERE id = ANY(%s::uuid[])",
                (ids,),
            )
            # Normalize to UUID so the caller's set-difference works regardless
            # of whether psycopg2 returns UUID columns as str or uuid.UUID.
            return {UUID(row[0]) if isinstance(row[0], str) else row[0] for row in cursor.fetchall()}

    def get_experts_by_department(
        self,
        department_id: UUID | None,
        source: str | None,
        status: str | None,
        from_date: date | None,
        to_date: date | None,
        full_circular_no: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict], int]:
        conditions, params = [], []
        p = 1
        if department_id:
            conditions.append("edm.department_id = %s"); params.append(str(department_id))
        if source:
            conditions.append("c.source = %s"); params.append(source.upper())
        if status:
            conditions.append("e.status = %s"); params.append(status.lower())
        if from_date:
            conditions.append("c.issue_date >= %s"); params.append(from_date)
        if to_date:
            conditions.append("c.issue_date <= %s"); params.append(to_date)
        if full_circular_no:
            conditions.append("UPPER(c.full_reference) LIKE UPPER(%s)")
            params.append(f"%{full_circular_no}%")

        where = " AND ".join(conditions) if conditions else "1=1"

        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT COUNT(DISTINCT e.id)
                FROM experts e
                JOIN expert_departments_mapping edm ON edm.expert_id = e.id
                JOIN circulars c ON c.id = e.circular_id
                WHERE {where}
                """,
                params,
            )
            total_row = cursor.fetchone()
            total = total_row[0] if total_row else 0

            cursor.execute(
                f"""
                SELECT DISTINCT e.id, e.expert_name, e.highlight_text,
                       e.status, e.created_at, e.updated_at,
                       e.created_by_user_id, u.user_id AS created_by,
                       c.id AS circ_id, c.full_reference, c.source, c.issue_date, c.title
                FROM experts e
                JOIN expert_departments_mapping edm ON edm.expert_id = e.id
                JOIN circulars c ON c.id = e.circular_id
                LEFT JOIN users u ON u.id = e.created_by_user_id
                WHERE {where}
                ORDER BY c.issue_date DESC
                LIMIT %s OFFSET %s
                """,
                [*params, limit, offset],
            )
            rows = cursor.fetchall()

        experts = [
            {
                "id": str(r[0]),
                "expert_name": r[1],
                "highlight_text": r[2],
                "status": r[3],
                "created_at": r[4].isoformat() if r[4] else None,
                "updated_at": r[5].isoformat() if r[5] else None,
                "created_by": r[7],                    # LDAP uid, may be NULL
                "circular": {
                    "id": str(r[8]),
                    "full_reference": r[9],
                    "source": r[10],
                    "issue_date": r[11].isoformat() if r[11] else None,
                    "title": r[12],
                },
            }
            for r in rows
        ]
        return experts, total
    

    # Expert depertment mapping functions

    def save_expert_departments_mapping(
        self, expert_id: UUID, dept_ids: list[UUID], *, conn: Any = None,
    ) -> None:
        """Insert (expert, department) pairs. Idempotent: ON CONFLICT DO NOTHING."""
        if not dept_ids:
            return

        def _work(c):
            cursor = c.cursor()
            for dept_id in dept_ids:
                cursor.execute(
                    """
                    INSERT INTO expert_departments_mapping (expert_id, department_id)
                    VALUES (%s, %s)
                    ON CONFLICT (expert_id, department_id) DO NOTHING
                    """,
                    (str(expert_id), str(dept_id)),
                )

        self._run(_work, conn=conn)

    def delete_expert_departments_mapping(
        self, expert_id: UUID, *, conn: Any = None,
    ) -> None:
        def _work(c):
            cursor = c.cursor()
            cursor.execute(
                "DELETE FROM expert_departments_mapping WHERE expert_id = %s",
                (str(expert_id),),
            )

        self._run(_work, conn=conn)

    def get_expert_departments_mapping(self, expert_id: UUID) -> list[UUID]:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT department_id FROM expert_departments_mapping WHERE expert_id = %s",
                (str(expert_id),),
            )
            return [UUID(row[0]) for row in cursor.fetchall()]
