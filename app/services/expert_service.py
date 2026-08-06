from datetime import date
import json
import logging
from typing import Any
from uuid import UUID

from ingestion.repository import ExpertMappingRepository

logger = logging.getLogger(__name__)


class ExpertService:
    def __init__(self, db_pool):
        self.repository = ExpertMappingRepository(db_pool=db_pool)

    def get_experts_by_department(
        self,
        department_id: UUID | None,
        source: str | None,
        status: str | None,
        from_date: date | None,
        to_date: date | None,
        full_circular_no: str | None,
        page: int,
        page_size: int,
    ) -> dict[str, Any]:
        offset = (page - 1) * page_size
        experts, total = self.repository.get_experts_by_department(
            department_id=department_id,
            source=source,
            status=status,
            from_date=from_date,
            to_date=to_date,
            full_circular_no=full_circular_no,
            limit=page_size,
            offset=offset,
        )
        total_pages = (total + page_size - 1) // page_size if total > 0 else 0
        return {
            "items": experts,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

    # def save_experts(
    #     self,
    #     circular_id: UUID,
    #     experts: list[dict],
    #     original_ids: list[str] | None = None,
    #     created_by_user_id: UUID | None = None,
    #     created_by_dep_id: UUID | None = None,
    # ) -> dict[str, Any]:
    #     """Save/update experts for a circular. Delete any that were removed.
    #
    #     Each expert provides ``dept_ids`` (list of department UUIDs). Departments
    #     are synced via the expert_departments_mapping junction. The full set of
    #     writes runs in a single transaction — any failure rolls back the entire
    #     save, so a circular can never end up half-updated.
    #
    #     Validation is done BEFORE entering the transaction. The 400 (missing
    #     dept_ids on new experts) and 404 (referencing non-existent expert ids)
    #     paths return early; the transaction body itself has no error returns,
    #     so a normal exit always commits cleanly.
    #
    #     DEPRECATED: superseded by the per-operation methods below
    #     (add_expert, update_expert, delete_expert, set_expert_departments).
    #     The frontend has migrated to the RESTful endpoints that call those
    #     methods individually. Kept here as a reference implementation of
    #     the snapshot/diff pattern with full atomicity — uncomment if you
    #     need to fall back to bundle-save behavior.
    #     """
    #     # ---- Pre-validation (no DB writes yet) ----
    #
    #     # 1. New experts must have dept_ids
    #     for expert in experts:
    #         if not expert.get("id"):
    #             dept_ids_raw = expert.get("dept_ids") or []
    #             if not dept_ids_raw:
    #                 return {"error": "dept_ids is required for new experts"}, 400
    #
    #     # 2. Referenced expert ids must exist (404 detection before any writes)
    #     requested_ids = {UUID(e["id"]) for e in experts if e.get("id")}
    #     if requested_ids:
    #         existing_ids = self.repository.get_existing_expert_ids(list(requested_ids))
    #         missing = requested_ids - existing_ids
    #         if missing:
    #             return {"error": f"Update failed for id {next(iter(missing))}"}, 404
    #
    #     # ---- Transactional write (no early returns inside) ----
    #     with self.repository.acquire() as conn:
    #         with conn.transaction():
    #             # 1. Delete removed experts
    #             if original_ids:
    #                 current_ids = {e.get("id") for e in experts if e.get("id")}
    #                 removed_ids = set(original_ids) - current_ids
    #                 for removed_id in removed_ids:
    #                     self.repository.delete_expert_mapping(UUID(removed_id), conn=conn)
    #
    #             # 2. Upsert current experts
    #             for expert in experts:
    #                 row_id = expert.get("id")
    #                 title = expert.get("title", "")
    #                 text = expert.get("text", "")
    #
    #                 # New format only: dept_ids (list). Single-dept_id payloads are not accepted.
    #                 dept_ids: list[UUID] = [UUID(d) for d in (expert.get("dept_ids") or []) if d]
    #
    #                 highlights_raw = expert.get("highlights", "[]")
    #                 highlights = (
    #                     json.loads(highlights_raw)
    #                     if isinstance(highlights_raw, str)
    #                     else highlights_raw
    #                 )
    #
    #                 if row_id:
    #                     # ---- UPDATE existing expert ----
    #                     row_uuid = UUID(row_id)
    #                     self.repository.update_expert_mapping(
    #                         row_id=row_uuid,
    #                         title=title,
    #                         text=text,
    #                         highlights=highlights,
    #                         conn=conn,
    #                     )
    #                     # Re-sync departments: delete old, insert new
    #                     self.repository.delete_expert_departments_mapping(row_uuid, conn=conn)
    #                     self.repository.save_expert_departments_mapping(row_uuid, dept_ids, conn=conn)
    #                 else:
    #                     # ---- CREATE new expert ----
    #                     new_id = self.repository.save_expert_mapping(
    #                         circular_id=circular_id,
    #                         title=title,
    #                         text=text,
    #                         highlights=highlights,
    #                         created_by_user_id=created_by_user_id,
    #                         created_by_dep_id=created_by_dep_id,
    #                         conn=conn,
    #                     )
    #                     self.repository.save_expert_departments_mapping(new_id, dept_ids, conn=conn)
    #
    #     return {"success": True}

    def update_expert_status(self, expert_id: UUID, status: str) -> bool:
        """Update the status of an expert."""
        return self.repository.update_expert_status(expert_id, status)

    def get_experts_for_circular(self, circular_id: UUID) -> list[dict]:
        """Get all experts for a specific circular."""
        return self.repository.get_expert_mappings_for_circular(circular_id)


    def add_expert(
        self,
        circular_id: UUID,
        title: str,
        text: str,
        dept_ids: list[UUID],
        highlights: list[dict],
        created_by_user_id: UUID | None = None,
        created_by_dep_id: UUID | None = None
        ) -> UUID:
        """Insert one expert + its department mappings. Returns the new expert id."""

        if not dept_ids:
            raise ValueError("dept_ids is required")
        with self.repository.acquire() as conn:
            with conn.transaction():
                new_id = self.repository.save_expert_mapping(
                    circular_id=circular_id,
                    title=title,
                    text=text,
                    highlights=highlights,
                    created_by_user_id=created_by_user_id,
                    created_by_dep_id=created_by_dep_id,
                    conn=conn,
                )
                self.repository.save_expert_departments_mapping(new_id, dept_ids, conn=conn)
        return new_id

    def update_expert(
        self,
        expert_id: UUID,
        title: str,
        text: str,
        highlights: list[dict],
    ) -> bool:
        """Update one expert's content fields. Returns False if not found."""
        return self.repository.update_expert_mapping(
            row_id=expert_id,
            title=title,
            text=text,
            highlights=highlights,
        )

    def delete_expert(self, expert_id: UUID) -> bool:
        """Hard-delete one expert. CASCADE wipes dept mappings. Returns False if not found."""
        return self.repository.delete_expert_mapping(expert_id)

    def set_expert_departments(self, expert_id: UUID, dept_ids: list[UUID]) -> bool:
        """Replace the dept mapping for one expert. Returns False if expert not found."""
        with self.repository.acquire() as conn:
            with conn.transaction():
                # Verify expert exists; bail if not
                existing = self.repository.get_existing_expert_ids([expert_id])
                if not existing:
                    return False
                self.repository.delete_expert_departments_mapping(expert_id, conn=conn)
                self.repository.save_expert_departments_mapping(expert_id, dept_ids, conn=conn)
        return True