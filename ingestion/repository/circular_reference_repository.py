from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass(slots=True)
class CircularReference:
    """Input payload for an outgoing reference edge."""
    relationship_type: str
    target_circular_number: str
    target_circular_id: UUID | None = None


class CircularReferenceRepository:

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("CircularReferenceRepository requires db_pool")
        self.logger = __import__("logging").getLogger(__name__)
        self.db_pool = db_pool

    def replace_references(
        self,
        source_circular_id: UUID,
        references: list[CircularReference],
    ) -> int:
        """Idempotent: delete outgoing refs for `source_circular_id`, then insert new ones.

        Returns the count written. Safe to call repeatedly for the same source —
        UNIQUE INDEX uq_circular_references_edge prevents duplicates within a batch.

        DELETE + INSERT loop runs inside an explicit conn.transaction() block —
        if any INSERT fails, the entire operation (including the DELETE)
        rolls back (M9 defense-in-depth fix).
        """
        with self.db_pool.acquire() as conn:
            with conn.transaction():
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM circular_references WHERE source_circular_id = %s",
                    (str(source_circular_id),),
                )
                for ref in references:
                    cursor.execute(
                        """
                        INSERT INTO circular_references (
                            source_circular_id, target_circular_id,
                            target_circular_number, relationship_type
                        )
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            str(source_circular_id),
                            str(ref.target_circular_id) if ref.target_circular_id else None,
                            ref.target_circular_number,
                            ref.relationship_type,
                        ),
                    )
        self.logger.info(
            "Replaced references source_circular_id=%s count=%s",
            source_circular_id, len(references),
        )
        return len(references)

    def get_direct_references(self, record_id: UUID) -> list[dict] | None:
        """Return incident edges and their opposite endpoints in one snapshot."""
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT ref.id, ref.source_circular_id, ref.target_circular_id,
                       ref.relationship_type, neighbor.id,
                       COALESCE(NULLIF(neighbor.full_reference, ''), neighbor.circular_id,
                                ref.target_circular_number),
                       neighbor.title, neighbor.source, neighbor.issue_date
                FROM circulars root
                LEFT JOIN circular_references ref
                  ON root.id = ref.source_circular_id OR root.id = ref.target_circular_id
                LEFT JOIN circulars neighbor ON neighbor.id = CASE
                    WHEN root.id = ref.source_circular_id THEN ref.target_circular_id
                    ELSE ref.source_circular_id END
                WHERE root.id = %s
                ORDER BY ref.id
                """,
                (str(record_id),),
            )
            rows = cursor.fetchall()
        if not rows:
            return None
        return [
            {
                "id": str(edge), "source": str(source),
                "target": str(target) if target else None, "relationship": relationship,
                "neighbor": {
                    "id": str(neighbor) if neighbor else None, "label": label,
                    "title": title, "exchange": exchange,
                    "date": issue_date.isoformat() if issue_date else None,
                },
            }
            for edge, source, target, relationship, neighbor, label, title, exchange, issue_date in rows
            if edge is not None
        ]
