import logging
from typing import Any, Optional
from uuid import UUID

from ingestion.dto.circular_reference_dto import CircularReferenceDTO
from ingestion.repository.circular_repository import CircularRepository


class CircularReferenceRepository:
    """Repository for managing circular cross-references."""

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("CircularReferenceRepository requires db_pool")
        self.logger = logging.getLogger(__name__)
        self.db_pool = db_pool
        self.circular_repo = CircularRepository(db_pool)

    def insert_reference(
        self,
        source_circular_id: UUID,
        reference_circular_no: str,
        relationship_nature: str,
        ref_circular_id: Optional[UUID] = None,
    ) -> None:
        """Inserts a single reference for a circular."""
        self.circular_repo._ensure_schema()

        with self.db_pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO circular_references (
                    source_circular_id, reference_circular_no, reference_circular_id,
                    relationship_nature, ref_circular_exist
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (source_circular_id, reference_circular_no)
                DO UPDATE SET
                    relationship_nature = EXCLUDED.relationship_nature,
                    reference_circular_id = EXCLUDED.reference_circular_id,
                    ref_circular_exist = EXCLUDED.ref_circular_exist,
                    updated_at = NOW()
                """,
                (
                    source_circular_id,
                    reference_circular_no,
                    ref_circular_id,
                    relationship_nature,
                    ref_circular_id is not None,
                ),
            )

    def insert_reference_batch(
        self,
        source_circular_id: UUID,
        references: list[dict],
    ) -> None:
        """Batch insert references for efficiency.

        Args:
            source_circular_id: UUID of the source circular
            references: List of dicts with 'reference_circular_no', 'relationship_nature',
                       and optional 'reference_circular_id' keys
        """
        for ref in references:
            self.insert_reference(
                source_circular_id=source_circular_id,
                reference_circular_no=ref["reference_circular_no"],
                relationship_nature=ref["relationship_nature"],
                ref_circular_id=ref.get("reference_circular_id"),
            )

    def delete_references_for_circular(self, circular_id: UUID) -> None:
        """Deletes all references for a given circular (idempotency)."""
        self.circular_repo._ensure_schema()
        with self.db_pool.connection() as conn:
            conn.execute(
                "DELETE FROM circular_references WHERE source_circular_id = %s",
                (circular_id,),
            )

    def get_references(
        self,
        circular_id: Optional[UUID] = None,
        relationship_nature: Optional[str] = None,
        unresolved_only: bool = False,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[CircularReferenceDTO], int]:
        """Retrieves references with optional filters."""
        self.circular_repo._ensure_schema()

        conditions = []
        params: list[Any] = []

        if circular_id is not None:
            conditions.append("source_circular_id = %s")
            params.append(circular_id)
        if relationship_nature is not None:
            conditions.append("relationship_nature = %s")
            params.append(relationship_nature)
        if unresolved_only:
            conditions.append("ref_circular_exist = FALSE")
            params.append(False)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        count_query = f"SELECT COUNT(*) FROM circular_references WHERE {where_clause}"
        data_query = f"""
            SELECT id, source_circular_id, reference_circular_no, reference_circular_id,
                   relationship_nature, ref_circular_exist, created_at, updated_at
            FROM circular_references
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

        references = [
            CircularReferenceDTO(
                id=row[0],
                source_circular_id=row[1],
                reference_circular_no=row[2],
                reference_circular_id=row[3],
                relationship_nature=row[4],
                ref_circular_exist=row[5],
                created_at=row[6],
                updated_at=row[7],
            )
            for row in rows
        ]
        return references, total

    def get_unresolved_references(self, limit: int = 100) -> list[CircularReferenceDTO]:
        """Get all references where the referenced circular doesn't exist in DB."""
        refs, _ = self.get_references(unresolved_only=True, limit=limit)
        return refs