import logging
from typing import Any
from uuid import UUID

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
