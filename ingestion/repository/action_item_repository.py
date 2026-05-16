import logging
from datetime import date
from typing import Any, Optional
from uuid import UUID

from ingestion.dto.action_item_dto import ActionItemDTO
from ingestion.repository.circular_repository import CircularRepository, _raw_to_uuid, _uuid_to_raw


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

        circular_id_raw = _uuid_to_raw(circular_id)
        with self.db_pool.acquire_connection() as conn:
            for item in items:
                new_id = _uuid_to_raw(UUID.uuid4())
                conn.execute(
                    """
                    INSERT INTO action_items (id, circular_id, action_item, deadline, priority, persona)
                    VALUES (:1, :2, :3, :4, :5, :6)
                    """,
                    (new_id, circular_id_raw, item.action_item, item.deadline, item.priority, item.persona),
                )
            conn.commit()

    def delete_action_items_for_circular(self, circular_id: UUID) -> None:
        """Deletes all action items for a given circular. Useful for idempotency."""
        self.circular_repo._ensure_schema()
        circular_id_raw = _uuid_to_raw(circular_id)
        with self.db_pool.acquire_connection() as conn:
            conn.execute(
                "DELETE FROM action_items WHERE circular_id = :1",
                (circular_id_raw,),
            )
            conn.commit()

    def get_action_items(
        self,
        circular_id: UUID | None = None,
        priority: str | None = None,
        persona: str | None = None,
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
            conditions.append("circular_id = :1")
            params.append(_uuid_to_raw(circular_id))

        if priority is not None:
            idx = len(params) + 1
            conditions.append(f"priority = :{idx}")
            params.append(priority)

        if persona is not None:
            idx = len(params) + 1
            conditions.append(f"persona = :{idx}")
            params.append(persona)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        count_query = f"SELECT COUNT(*) FROM action_items WHERE {where_clause}"
        data_query = f"""
            SELECT id, circular_id, action_item, deadline, priority, persona, created_at, updated_at
            FROM action_items
            WHERE {where_clause}
            ORDER BY created_at DESC
            OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
        """

        with self.db_pool.acquire_connection() as conn:
            count_result = conn.execute(count_query, params)
            total = count_result.fetchone()[0]

            params_with_pagination = params + [offset, limit]
            result = conn.execute(data_query, params_with_pagination)
            rows = result.fetchall()

        action_items = [
            ActionItemDTO(
                id=_raw_to_uuid(row[0]),
                circular_id=_raw_to_uuid(row[1]),
                action_item=row[2],
                deadline=row[3],
                priority=row[4],
                persona=row[5],
                created_at=row[6],
                updated_at=row[7],
            )
            for row in rows
        ]

        return action_items, total