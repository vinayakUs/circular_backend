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

    def save_experts(
        self,
        circular_id: UUID,
        experts: list[dict],
        original_ids: list[str] | None = None,
        created_by_user_id: UUID | None = None,
        created_by_dep_id: UUID | None = None,
    ) -> dict[str, Any]:
        """Save/update experts for a circular. Delete any that were removed."""
        # Delete removed experts
        if original_ids:
            current_ids = {e.get("id") for e in experts if e.get("id")}
            removed_ids = set(original_ids) - current_ids
            for removed_id in removed_ids:
                self.repository.delete_expert_mapping(UUID(removed_id))

        # Upsert current experts
        for expert in experts:
            row_id = expert.get("id")
            title = expert.get("title", "")
            text = expert.get("text", "")
            dept_id = expert.get("dept_id")
            highlights_raw = expert.get("highlights", "[]")
            # Parse if it's a JSON string (sent by frontend)
            highlights = json.loads(highlights_raw) if isinstance(highlights_raw, str) else highlights_raw

            if row_id:
                row_uuid = UUID(row_id)
                dept_uuid = UUID(dept_id) if dept_id else None
                success = self.repository.update_expert_mapping(
                    row_id=row_uuid,
                    dept_id=dept_uuid,
                    title=title,
                    text=text,
                    highlights=highlights,
                )
                if not success:
                    return {"error": f"Update failed for id {row_id}"}, 404
            else:
                if not dept_id:
                    return {"error": "dept_id is required for new experts"}, 400
                self.repository.save_expert_mapping(
                    circular_id=circular_id,
                    dept_id=UUID(dept_id),
                    title=title,
                    text=text,
                    highlights=highlights,
                    created_by_user_id=created_by_user_id,
                    created_by_dep_id=created_by_dep_id,
                )

        return {"success": True}

    def update_expert_status(self, expert_id: UUID, status: str) -> bool:
        """Update the status of an expert."""
        return self.repository.update_expert_status(expert_id, status)

    def get_experts_for_circular(self, circular_id: UUID) -> list[dict]:
        """Get all experts for a specific circular."""
        return self.repository.get_expert_mappings_for_circular(circular_id)