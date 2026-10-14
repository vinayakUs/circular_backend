
from __future__ import annotations

from datetime import date, datetime
from typing import Any, List, Optional, Tuple
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_serializer


class ActionItemDTO(BaseModel):
    """DTO for action item API output."""

    model_config = ConfigDict(str_strip_whitespace=True)

    id: UUID
    circular_id: UUID
    action_item: str
    deadline: Optional[date] = None
    priority: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    @field_serializer("created_at")
    def _ser_created_at(self, v: datetime) -> str:
        return v.isoformat()

    @field_serializer("updated_at")
    def _ser_updated_at(self, v: datetime) -> str:
        return v.isoformat()

    @field_serializer("deadline")
    def _ser_deadline(self, v: Optional[date]) -> Optional[str]:
        return v.isoformat() if v else None


class ActionItemListResponseDTO(BaseModel):
    """DTO for paginated action items list response."""

    action_items: List[ActionItemDTO]
    total: int
    limit: int
    offset: int

_VALID_PRIORITIES = frozenset({"critical", "high", "medium", "low"})

class ActionItemRepository:
    """Repository for managing action items in the database."""

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("ActionItemRepository requires db_pool")
        self.logger = __import__("logging").getLogger(__name__)
        self.db_pool = db_pool


    # ──────────────────────────────────────────────────────────────────────
    # WRITES
    # ──────────────────────────────────────────────────────────────────────

    def insert_action_items(
        self,
        circular_id: UUID,
        items: list[Any],
    ) -> int:
        """Insert a list of action items for a given circular. Returns count written.

        No DELETE here — callers wanting idempotency should call
        `delete_action_items_for_circular` first (delete-then-insert pattern).
        Items accept any object with `.action_item`, `.deadline`, `.priority`.
        """
        if not items:
            return 0

        with self.db_pool.acquire() as conn:
            with conn.transaction():
                cursor = conn.cursor()
                for item in items:
                    priority = getattr(item, "priority", None)
                    if priority is not None and priority not in _VALID_PRIORITIES:
                        raise ValueError(
                            f"Invalid priority '{priority}'. "
                            f"Must be one of: {sorted(_VALID_PRIORITIES)}"
                        )
                    cursor.execute(
                        """
                        INSERT INTO action_items (
                            circular_id, action_item, deadline, priority
                        )
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            str(circular_id),
                            item.action_item,
                            item.deadline,
                            priority,
                        ),
                    )

        self.logger.info(
            "Inserted action items circular_id=%s count=%s",
            circular_id, len(items),
        )
        return len(items)

    def delete_action_items_for_circular(self, circular_id: UUID) -> int:
        """Delete all action items for a circular. Returns count deleted.

        Call before re-inserting to make `insert_action_items` idempotent.
        """
        with self.db_pool.acquire() as conn:
            with conn.transaction():
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM action_items WHERE circular_id = %s",
                    (str(circular_id),),
                )
                count = cursor.rowcount
        self.logger.info(
            "Deleted action items circular_id=%s count=%s",
            circular_id, count,
        )
        return count

    # ──────────────────────────────────────────────────────────────────────
    # READS
    # ──────────────────────────────────────────────────────────────────────

    def get_action_items(
        self,
        circular_id: Optional[UUID] = None,
        priority: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[ActionItemDTO], int]:
        """Retrieve action items with optional filters. Returns (items, total).

        Filters:
            - circular_id: only items for this circular
            - priority: exact match (critical/high/medium/low)
            - start_date / end_date: inclusive range on `deadline`
        """
        if priority is not None and priority not in _VALID_PRIORITIES:
            raise ValueError(
                f"Invalid priority '{priority}'. "
                f"Must be one of: {sorted(_VALID_PRIORITIES)}"
            )

        conditions: list[str] = []
        params: list[Any] = []

        if circular_id is not None:
            conditions.append("circular_id = %s")
            params.append(str(circular_id))
        if priority is not None:
            conditions.append("priority = %s")
            params.append(priority)
        if start_date is not None:
            conditions.append("deadline >= %s")
            params.append(start_date)
        if end_date is not None:
            conditions.append("deadline <= %s")
            params.append(end_date)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        count_query = f"SELECT COUNT(*) FROM action_items WHERE {where_clause}"
        data_query = f"""
            SELECT id, circular_id, action_item, deadline, priority, created_at, updated_at
            FROM   action_items
            WHERE  {where_clause}
            ORDER BY created_at DESC
            LIMIT  %s OFFSET %s
        """

        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(count_query, tuple(params))
            total = cursor.fetchone()[0]

            cursor.execute(data_query, tuple(params) + (limit, offset))
            rows = cursor.fetchall()

        items = [
            ActionItemDTO(
                id=row[0],
                circular_id=row[1],
                action_item=row[2],
                deadline=row[3],
                priority=row[4],
                created_at=row[5],
                updated_at=row[6],
            )
            for row in rows
        ]
        return items, total

    def get_action_items_for_circular(
        self, circular_id: UUID,
    ) -> List[ActionItemDTO]:
        """Convenience: all action items for one circular, newest first."""
        items, _ = self.get_action_items(circular_id=circular_id, limit=1000, offset=0)
        return items

    # Used by persona-slug API endpoint to fetch deadlines
    def get_deadlines_for_persona(
        self,
        persona_slug: str,
        from_date: date,
        to_date: date,
        priority_csv: Optional[str] = None,
        exchange_csv: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[Tuple[UUID, date, str, str, Optional[str], UUID]], int]:
        """Return (rows, total) for deadlines matching this persona.

        Joins action_items → circulars → circular_persona_matches.
        Multiplication-safe: circular_persona_matches has UNIQUE PK on
        (circular_id, persona_slug), so the JOIN can't duplicate rows.

        Two queries: COUNT(*) for total + SELECT for the page.
        Both use the same WHERE clause + ORDER BY with stable tiebreaker.
        """
        # Normalize empty string to None (defensive — route already does this)
        priority_csv = priority_csv or None
        exchange_csv = exchange_csv or None

        where_parts = [
            "ai.deadline IS NOT NULL",
            "ai.deadline BETWEEN %s AND %s",
            # ::text casts make the placeholder type explicit — works with both
            # psycopg2 (client-side substitution) and psycopg3 (server-side binding)
            "(%s::text IS NULL OR ai.priority = ANY(string_to_array(%s::text, ',')))",
            "(%s::text IS NULL OR c.source    = ANY(string_to_array(%s::text, ',')))",
        ]
        where = " AND ".join(where_parts)

        count_query = f"""
            SELECT COUNT(*)
            FROM   action_items ai
            JOIN   circulars c ON c.id = ai.circular_id
            JOIN   circular_persona_matches m
                ON m.circular_id = c.id
                AND m.persona_slug = %s
            WHERE  {where}
        """
        data_query = f"""
            SELECT ai.id,
                ai.deadline,
                ai.action_item,
                c.source,
                ai.priority,
                c.id
            FROM   action_items ai
            JOIN   circulars c ON c.id = ai.circular_id
            JOIN   circular_persona_matches m
                ON m.circular_id = c.id
                AND m.persona_slug = %s
            WHERE  {where}
            ORDER BY ai.deadline ASC, ai.id ASC
            LIMIT  %s OFFSET %s
        """

        base_params = (
            persona_slug,
            from_date,
            to_date,
            priority_csv,
            priority_csv,
            exchange_csv,
            exchange_csv,
        )

        with self.db_pool.acquire() as conn:
            with conn.transaction():
                cursor = conn.cursor()
                cursor.execute(count_query, base_params)
                total = cursor.fetchone()[0]
                cursor.execute(data_query, base_params + (limit, offset))
                rows = cursor.fetchall()

        return list(rows), total