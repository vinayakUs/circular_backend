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

    def save_expert_mapping(
        self, circular_id: UUID, dept_id: UUID, title: str, text: str, highlights: list[dict],
        created_by_user_id: UUID | None = None, created_by_dep_id: UUID | None = None
    ) -> UUID:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO experts
                    (circular_id, department_id, expert_name, highlight_text, highlights,
                     created_by_user_id, created_by_dep_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (str(circular_id), str(dept_id), title, text, json.dumps(highlights),
                 str(created_by_user_id) if created_by_user_id else None,
                 str(created_by_dep_id) if created_by_dep_id else None),
            )
            row_id = cursor.fetchone()[0]
            conn.commit()
        return row_id

    def update_expert_mapping(
        self, row_id: UUID, dept_id: UUID, title: str, text: str, highlights: list[dict]
    ) -> bool:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE experts
                SET department_id = %s, expert_name = %s, highlight_text = %s,
                    highlights = %s, updated_at = NOW()
                WHERE id = %s
                RETURNING id
                """,
                (str(dept_id), title, text, json.dumps(highlights), str(row_id)),
            )
            result = cursor.fetchone()
            conn.commit()
        return result is not None

    def delete_expert_mapping(self, row_id: UUID) -> bool:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM experts WHERE id = %s RETURNING id",
                (str(row_id),),
            )
            result = cursor.fetchone()
            conn.commit()
        return result is not None

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
                SELECT m.id, m.circular_id, m.department_id, m.expert_name, m.highlight_text,
                       m.highlights, m.created_at, m.updated_at, m.status, p.name as dept_name,
                       m.created_by_user_id, m.created_by_dep_id,
                       u.user_id as created_by_username
                FROM experts m
                LEFT JOIN properties p ON p.id = m.department_id
                LEFT JOIN users u ON u.id = m.created_by_user_id
                WHERE m.circular_id = %s
                ORDER BY m.created_at
                """,
                (str(circular_id),),
            )
            rows = cursor.fetchall()
        return [
            {
                "id": str(r[0]),
                "circular_id": str(r[1]),
                "dept_id": str(r[2]),
                "dept_name": r[9] or "",
                "title": r[3],
                "text": r[4],
                "highlights": json.loads(r[5]) if isinstance(r[5], str) else (r[5] if isinstance(r[5], list) else []),
                "status": r[8] or "open",
                "created_at": r[6].isoformat() if r[6] else None,
                "updated_at": r[7].isoformat() if r[7] else None,
                "created_by_user_id": str(r[10]) if r[10] else None,
                "created_by_dep_id": str(r[11]) if r[11] else None,
                "created_by_username": r[12] if r[12] else None,
            }
            for r in rows
        ]

    def get_experts_by_department(
        self,
        department_id: UUID | None,
        source: str | None,
        from_date: date | None,
        to_date: date | None,
        full_circular_no: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict], int]:
        conditions, params = [], []
        p = 1
        if department_id:
            conditions.append(f"cdm.department_id = %s"); params.append(str(department_id)); p += 1
        if source:
            conditions.append(f"c.source = %s"); params.append(source.upper()); p += 1
        if from_date:
            conditions.append(f"c.issue_date >= %s"); params.append(from_date); p += 1
        if to_date:
            conditions.append(f"c.issue_date <= %s"); params.append(to_date); p += 1
        if full_circular_no:
            conditions.append(f"UPPER(c.full_reference) LIKE UPPER(%s)"); params.append(f"%{full_circular_no}%"); p += 1

        where = " AND ".join(conditions) if conditions else "1=1"

        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT COUNT(*) FROM experts e JOIN circulars c ON e.circular_id = c.id WHERE {where}",
                params,
            )
            total_row = cursor.fetchone()
            total = total_row[0] if total_row else 0

            cursor.execute(
                f"""
                SELECT e.id, e.expert_name, e.highlight_text,
                       c.id AS circ_id, c.full_reference, c.source, c.issue_date, c.title
                FROM experts e
                JOIN circulars c ON e.circular_id = c.id
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
                "circular": {
                    "id": str(r[3]),
                    "full_reference": r[4],
                    "source": r[5],
                    "issue_date": r[6].isoformat() if r[6] else None,
                    "title": r[7],
                },
            }
            for r in rows
        ]
        return experts, total