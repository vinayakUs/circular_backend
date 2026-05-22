from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import logging
from typing import Any
from uuid import UUID

from ingestion.repository._uuid_utils import _raw_to_uuid, _uuid_to_raw


@dataclass(slots=True)
class ExpertMappingRecord:
    id: UUID
    circular_id: UUID
    department_id: UUID
    expert_name: str
    highlight_text: str
    highlights: list[dict]
    created_at: datetime
    updated_at: datetime


class ExpertMappingRepository:

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("ExpertMappingRepository requires db_pool")
        self.logger = __import__("logging").getLogger(__name__)
        self.db_pool = db_pool

    def save_expert_mapping(
        self, circular_id: UUID, dept_id: UUID, title: str, text: str, highlights: list[dict]
    ) -> UUID:
        with self.db_pool.acquire() as conn:
            out_id = conn.cursor().var(bytes)
            conn.cursor().execute(
                """
                INSERT INTO circular_department_mapping
                    (circular_id, department_id, expert_name, highlight_text, highlights)
                VALUES (:1, :2, :3, :4, :5)
                RETURNING id INTO :6
                """,
                (_uuid_to_raw(circular_id), _uuid_to_raw(dept_id), title, text, json.dumps(highlights), out_id),
            )
            conn.commit()
        return _raw_to_uuid(out_id.getvalue()[0])

    def update_expert_mapping(
        self, row_id: UUID, dept_id: UUID, title: str, text: str, highlights: list[dict]
    ) -> bool:
        with self.db_pool.acquire() as conn:
            out_id = conn.cursor().var(bytes)
            conn.cursor().execute(
                """
                UPDATE circular_department_mapping
                SET department_id = :1, expert_name = :2, highlight_text = :3,
                    highlights = :4, updated_at = SYSTIMESTAMP
                WHERE id = :5
                RETURNING id INTO :6
                """,
                (_uuid_to_raw(dept_id), title, text, json.dumps(highlights), _uuid_to_raw(row_id), out_id),
            )
            conn.commit()
        values = out_id.getvalue()
        return bool(values and values[0] is not None)

    def delete_expert_mapping(self, row_id: UUID) -> bool:
        with self.db_pool.acquire() as conn:
            out_id = conn.cursor().var(bytes)
            conn.cursor().execute(
                "DELETE FROM circular_department_mapping WHERE id = :1 RETURNING id INTO :2",
                (_uuid_to_raw(row_id), out_id),
            )
            conn.commit()
        values = out_id.getvalue()
        return bool(values and values[0] is not None)

    def get_expert_mappings_for_circular(self, circular_id: UUID) -> list[dict]:
        with self.db_pool.acquire() as conn:
            rows = conn.cursor().execute(
                """
                SELECT m.id, m.circular_id, m.department_id, m.expert_name, m.highlight_text,
                       m.highlights, m.created_at, m.updated_at, p.name as dept_name
                FROM circular_department_mapping m
                LEFT JOIN properties p ON p.id = m.department_id
                WHERE m.circular_id = :1
                ORDER BY m.created_at
                """,
                (_uuid_to_raw(circular_id),),
            ).fetchall()
        return [
            {
                "id": str(_raw_to_uuid(r[0])),
                "circular_id": str(_raw_to_uuid(r[1])),
                "dept_id": str(_raw_to_uuid(r[2])),
                "dept_name": r[8] or "",
                "title": r[3],
                "text": r[4],
                "highlights": json.loads(r[5]) if isinstance(r[5], str) else (r[5] if isinstance(r[5], list) else []),
                "created_at": r[6].isoformat() if r[6] else None,
                "updated_at": r[7].isoformat() if r[7] else None,
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
            conditions.append(f"cdm.department_id = :{p}"); params.append(_uuid_to_raw(department_id)); p += 1
        if source:
            conditions.append(f"c.source = :{p}"); params.append(source.upper()); p += 1
        if from_date:
            conditions.append(f"c.issue_date >= :{p}"); params.append(from_date); p += 1
        if to_date:
            conditions.append(f"c.issue_date <= :{p}"); params.append(to_date); p += 1
        if full_circular_no:
            conditions.append(f"UPPER(c.full_reference) LIKE UPPER(:{p})"); params.append(f"%{full_circular_no}%"); p += 1

        where = " AND ".join(conditions) if conditions else "1=1"

        with self.db_pool.acquire() as conn:
            total_row = conn.cursor().execute(
                f"SELECT COUNT(*) FROM circular_department_mapping cdm JOIN circulars c ON cdm.circular_id = c.id WHERE {where}",
                params,
            ).fetchone()
            total = total_row[0] if total_row else 0

            rows = conn.cursor().execute(
                f"""
                SELECT * FROM (
                    SELECT cdm.id, cdm.expert_name, cdm.highlight_text,
                           c.id AS circ_id, c.full_reference, c.source, c.issue_date, c.title,
                           ROW_NUMBER() OVER (ORDER BY c.issue_date DESC) AS rn
                    FROM circular_department_mapping cdm
                    JOIN circulars c ON cdm.circular_id = c.id
                    WHERE {where}
                )
                WHERE rn > :offset AND rn <= :limit
                """,
                [*params, offset, offset + limit],
            ).fetchall()

        experts = [
            {
                "id": str(_raw_to_uuid(r[0])),
                "expert_name": r[1],
                "highlight_text": r[2],
                "circular": {
                    "id": str(_raw_to_uuid(r[3])),
                    "full_reference": r[4],
                    "source": r[5],
                    "issue_date": r[6].isoformat() if r[6] else None,
                    "title": r[7],
                },
            }
            for r in rows
        ]
        return experts, total