import logging
from datetime import date
from typing import Any, Optional
from uuid import UUID

from ingestion.dto.circular_reference_dto import CircularReferenceDTO
from ingestion.repository.circular_repository import CircularRepository, _raw_to_uuid, _uuid_to_raw


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
        source_id_raw = _uuid_to_raw(source_circular_id)
        ref_id_raw = _uuid_to_raw(ref_circular_id) if ref_circular_id else None
        new_id = _uuid_to_raw(UUID.uuid4())

        with self.db_pool.acquire_connection() as conn:
            conn.execute(
                """
                MERGE INTO circular_references cr
                USING (
                    SELECT :1 AS source_circular_id, :2 AS reference_circular_no,
                           :3 AS relationship_nature, :4 AS ref_circular_id,
                           :5 AS ref_circular_exist FROM DUAL
                ) src
                ON (cr.source_circular_id = src.source_circular_id
                    AND cr.reference_circular_no = src.reference_circular_no)
                WHEN MATCHED THEN
                    UPDATE SET cr.relationship_nature = src.relationship_nature,
                               cr.reference_circular_id = src.ref_circular_id,
                               cr.ref_circular_exist = src.ref_circular_exist,
                               cr.updated_at = SYSDATE
                WHEN NOT MATCHED THEN
                    INSERT (id, source_circular_id, reference_circular_no, reference_circular_id,
                            relationship_nature, ref_circular_exist)
                    VALUES (src.id, src.source_circular_id, src.reference_circular_no,
                            src.ref_circular_id, src.relationship_nature, src.ref_circular_exist)
                """,
                (source_id_raw, reference_circular_no, relationship_nature, ref_id_raw, ref_circular_id is not None),
            )
            conn.commit()

    def insert_reference_batch(
        self,
        source_circular_id: UUID,
        references: list[dict],
    ) -> None:
        """Batch insert references for efficiency."""
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
        circular_id_raw = _uuid_to_raw(circular_id)
        with self.db_pool.acquire_connection() as conn:
            conn.execute(
                "DELETE FROM circular_references WHERE source_circular_id = :1",
                (circular_id_raw,),
            )
            conn.commit()

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
            conditions.append("source_circular_id = :1")
            params.append(_uuid_to_raw(circular_id))

        if relationship_nature is not None:
            idx = len(params) + 1
            conditions.append(f"relationship_nature = :{idx}")
            params.append(relationship_nature)

        if unresolved_only:
            conditions.append("ref_circular_exist = 0")
            params.append(0)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        count_query = f"SELECT COUNT(*) FROM circular_references WHERE {where_clause}"
        data_query = f"""
            SELECT id, source_circular_id, reference_circular_no, reference_circular_id,
                   relationship_nature, ref_circular_exist, created_at, updated_at
            FROM circular_references
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

        references = [
            CircularReferenceDTO(
                id=_raw_to_uuid(row[0]),
                source_circular_id=_raw_to_uuid(row[1]),
                reference_circular_no=row[2],
                reference_circular_id=_raw_to_uuid(row[3]) if row[3] else None,
                relationship_nature=row[4],
                ref_circular_exist=bool(row[5]),
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