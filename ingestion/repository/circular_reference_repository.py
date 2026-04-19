import logging
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel

from ingestion.dto.circular_reference_dto import CircularReferenceDTO, ExtractedReference, ReferenceWithNature
from ingestion.repository.circular_repository import CircularRepository


class CircularReferenceRepository:
    """Repository for managing circular cross-references."""

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("CircularReferenceRepository requires db_pool")
        self.logger = logging.getLogger(__name__)
        self.db_pool = db_pool
        self.circular_repo = CircularRepository(db_pool)

    def _validate_referenced_exists(self, referenced_id: str, referenced_source: str) -> bool:
        """Check if a referenced circular exists in our circulars table."""
        try:
            record = self.circular_repo.get_record(referenced_source, referenced_id)
            return record is not None
        except Exception:
            return False

    def insert_reference(
        self,
        source_circular_id: UUID,
        ref: ReferenceWithNature,
    ) -> None:
        """Inserts a single reference for a circular with existence validation."""
        self.circular_repo._ensure_schema()

        ref_id = ref.reference.referenced_id
        ref_source = ref.reference.referenced_source
        exists = self._validate_referenced_exists(ref_id, ref_source)

        relationship_nature = ref.relationship.relationship_nature if ref.relationship else None
        confidence = ref.relationship.confidence if ref.relationship else 0.0

        with self.db_pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO circular_references (
                    source_circular_id, referenced_circular_id, referenced_source,
                    referenced_full_ref, relationship_nature, confidence_score,
                    extraction_method, matched_text, referenced_circular_exists
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_circular_id, referenced_circular_id, referenced_source)
                DO UPDATE SET
                    relationship_nature = EXCLUDED.relationship_nature,
                    confidence_score = EXCLUDED.confidence_score,
                    referenced_circular_exists = EXCLUDED.referenced_circular_exists,
                    updated_at = NOW()
                """,
                (
                    source_circular_id,
                    ref_id,
                    ref_source,
                    ref.reference.referenced_full_ref,
                    relationship_nature,
                    confidence,
                    ref.reference.extraction_method,
                    ref.reference.matched_text,
                    exists,
                ),
            )

    def insert_reference_batch(
        self,
        source_circular_id: UUID,
        refs: list[ReferenceWithNature],
    ) -> None:
        """Batch insert references for efficiency."""
        for ref in refs:
            self.insert_reference(source_circular_id, ref)

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
        referenced_source: Optional[str] = None,
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
        if referenced_source is not None:
            conditions.append("referenced_source = %s")
            params.append(referenced_source)
        if unresolved_only:
            conditions.append("referenced_circular_exists = FALSE")
            params.append(False)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        count_query = f"SELECT COUNT(*) FROM circular_references WHERE {where_clause}"
        data_query = f"""
            SELECT id, source_circular_id, referenced_circular_id, referenced_source,
                   referenced_full_ref, relationship_nature, confidence_score,
                   extraction_method, matched_text, referenced_circular_exists,
                   created_at, updated_at
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
                referenced_circular_id=row[2],
                referenced_source=row[3],
                referenced_full_ref=row[4],
                relationship_nature=row[5],
                confidence_score=row[6],
                extraction_method=row[7],
                matched_text=row[8],
                referenced_circular_exists=row[9],
                created_at=row[10],
                updated_at=row[11],
            )
            for row in rows
        ]
        return references, total

    def get_unresolved_references(self, limit: int = 100) -> list[CircularReferenceDTO]:
        """Get all references where the referenced circular doesn't exist in DB."""
        refs, _ = self.get_references(unresolved_only=True, limit=limit)
        return refs