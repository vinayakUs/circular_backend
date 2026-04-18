import logging
from datetime import date, datetime
from typing import Any
from uuid import UUID

from ingestion.dto.action_item_dto import ActionItemDTO
from ingestion.repository.circular_repository import CircularRepository


class ActionItemRepository:
    """Repository for managing action items in the database."""

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("ActionItemRepository requires db_pool")
        self.logger = logging.getLogger(__name__)
        self.db_pool = db_pool
        self.circular_repo = CircularRepository(db_pool)

    def insert_action_items(self, circular_id: UUID, items: list[Any]) -> None:
        """Inserts a list of action items for a given circular."""
        self.circular_repo._ensure_schema()
        if not items:
            return

        with self.db_pool.connection() as conn:
            for item in items:
                conn.execute(
                    """
                    INSERT INTO action_items (circular_id, action_item, deadline, priority)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (circular_id, item.action_item, item.deadline, item.priority),
                )

    def delete_action_items_for_circular(self, circular_id: UUID) -> None:
        """Deletes all action items for a given circular. Useful for idempotency."""
        self.circular_repo._ensure_schema()
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                DELETE FROM action_items WHERE circular_id = %s
                """,
                (circular_id,),
            )

    def get_action_items(
        self,
        circular_id: UUID | None = None,
        priority: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[ActionItemDTO], int]:
        """Retrieves action items with optional filters and pagination."""
        self.circular_repo._ensure_schema()

        conditions = []
        params = []

        if circular_id is not None:
            conditions.append("circular_id = %s")
            params.append(circular_id)

       
        where_clause = " AND ".join(conditions) if conditions else "1=1"

        count_query = f"SELECT COUNT(*) FROM action_items WHERE {where_clause}"
        data_query = f"""
            SELECT id, circular_id, action_item, deadline, priority, created_at, updated_at
            FROM action_items
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """

        with self.db_pool.connection() as conn:
            count_result = conn.execute(count_query, tuple(params))
            total = count_result.fetchone()[0]

            params_with_pagination = tuple(params) + (limit, offset)
            result = conn.execute(data_query, params_with_pagination)
            rows = result.fetchall()

        action_items = [
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

        return action_items, total
