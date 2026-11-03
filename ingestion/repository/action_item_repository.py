
from __future__ import annotations

from datetime import date, datetime, timedelta
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
        Items accept any object with `.action_item`, `.deadline`.
        """
        if not items:
            return 0

        with self.db_pool.acquire() as conn:
            with conn.transaction():
                cursor = conn.cursor()
                for item in items:
                    cursor.execute(
                        """
                        INSERT INTO action_items (
                            circular_id, action_item, deadline
                        )
                        VALUES (%s, %s, %s)
                        """,
                        (
                            str(circular_id),
                            item.action_item,
                            item.deadline,
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
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Tuple[List[ActionItemDTO], int]:
        """Retrieve action items with optional filters. Returns (items, total).

        Filters:
            - circular_id: only items for this circular
            - start_date / end_date: inclusive range on `deadline`
        """
        conditions: list[str] = []
        params: list[Any] = []

        if circular_id is not None:
            conditions.append("circular_id = %s")
            params.append(str(circular_id))
        if start_date is not None:
            conditions.append("deadline >= %s")
            params.append(start_date)
        if end_date is not None:
            conditions.append("deadline <= %s")
            params.append(end_date)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        count_query = f"SELECT COUNT(*) FROM action_items WHERE {where_clause}"
        data_query = f"""
            SELECT id, circular_id, action_item, deadline, created_at, updated_at
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
                created_at=row[4],
                updated_at=row[5],
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

    # Used by both persona-scoped (slug provided) AND cross-persona
    # (slug=None) routes. When slug is None, the `circular_persona_matches`
    # JOIN and its placeholder are skipped — every deadline is in scope.
    def get_deadlines(
        self,
        persona_slug: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        exchange_csv: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Tuple[List[Tuple[UUID, date, str, str, UUID, str]], int]:
        """Return (rows, total) for deadlines, optionally filtered by persona.

        Joins action_items → circulars; when `persona_slug` is given,
        also JOINs circular_persona_matches to scope the result to that
        persona. Multiplication-safe: circular_persona_matches has UNIQUE
        PK on (circular_id, persona_slug), so the JOIN can't duplicate rows.

        When `limit` is set, two queries run: COUNT(*) for total + SELECT
        for the page. Both use the same WHERE clause + ORDER BY with a
        stable tiebreaker.

        Args:
            persona_slug:  filter to one persona's deadlines, or None for
                           cross-persona results.
            from_date:     inclusive lower bound on `deadline`. Defaults to
                           today-30d when omitted.
            to_date:       inclusive upper bound on `deadline`. Defaults to
                           from + 365d when omitted.
            limit:         page size. None (the default) means NO pagination
                           — every matched row in the range comes back and
                           `total` is simply len(rows). A positive int turns
                           on LIMIT/OFFSET paging.
            offset:        rows to skip. Applied with paging; without a
                           limit a positive offset shifts the unbounded
                           window (rows after it are all returned).
        """
        # ── Defaults ─────────────────────────────────────────
        exchange_csv = exchange_csv or None
        if from_date is None:
            from_date = date.today() - timedelta(days=30)
        if to_date is None:
            to_date = from_date + timedelta(days=365)
        if limit is not None and limit < 1:
            raise ValueError("limit must be an integer of at least 1")

        # ── WHERE / JOINs vary by whether persona_slug is present ──
        where_parts = [
            "ai.deadline IS NOT NULL",
            "ai.deadline BETWEEN %s AND %s",
            # ::text casts make the placeholder type explicit — works with both
            # psycopg2 (client-side substitution) and psycopg3 (server-side binding)
            "(%s::text IS NULL OR c.source = ANY(string_to_array(%s::text, ',')))",
        ]
        where = " AND ".join(where_parts)

        if persona_slug is not None:
            persona_join = (
                "JOIN circular_persona_matches m "
                "ON m.circular_id = c.id AND m.persona_slug = %s"
            )
        else:
            persona_join = ""

        # ── Queries ──────────────────────────────────────────
        # The paging clause is appended, not fixed: no limit means no LIMIT
        # in the SQL at all, so the result set is bounded only by the WHERE
        # clause (the date range). The COUNT boat is skipped in that mode —
        # with no LIMIT, `total` is just len(rows).
        count_query = f"""
            SELECT COUNT(*)
            FROM   action_items ai
            JOIN   circulars c ON c.id = ai.circular_id
            {persona_join}
            WHERE  {where}
        """
        paging_sql = ""
        if limit is not None:
            paging_sql = "LIMIT  %s OFFSET %s"
        elif offset > 0:
            paging_sql = "OFFSET %s"
        data_query = f"""
            SELECT ai.id,
                ai.deadline,
                ai.action_item,
                c.source,
                c.id,
                c.title        AS circular_title
            FROM   action_items ai
            JOIN   circulars c ON c.id = ai.circular_id
            {persona_join}
            WHERE  {where}
            ORDER BY ai.deadline ASC, ai.id ASC
            {paging_sql}
        """

        # Build params in the order placeholders appear in the SQL above:
        #   persona_slug (×1 if present), from_date, to_date, exchange_csv
        #   (×2) — then the paging placeholders, which follow paging_sql:
        #   limit + offset (paged) or a lone offset (offset-only tail).
        params: list = []
        if persona_slug is not None:
            params.append(persona_slug)
        params += [
            from_date,
            to_date,
            exchange_csv, exchange_csv,
        ]
        base_params = tuple(params)

        with self.db_pool.acquire() as conn:
            with conn.transaction():
                cursor = conn.cursor()
                if limit is None:
                    # Unbounded: everything the date range matches. Skip the
                    # COUNT — it would just agree with len(rows).
                    tail_params = (offset,) if offset > 0 else ()
                    cursor.execute(data_query, base_params + tail_params)
                    rows = cursor.fetchall()
                    total = len(rows)
                else:
                    cursor.execute(count_query, base_params)
                    total = cursor.fetchone()[0]
                    cursor.execute(data_query, base_params + (limit, offset))
                    rows = cursor.fetchall()

        return list(rows), total