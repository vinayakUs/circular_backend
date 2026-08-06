"""
Expert routes: CRUD on expert tasks attached to a circular, plus comments
on those tasks, plus the open/closed status flip, plus the
"experts by department" filter for the home screen.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Optional
from uuid import UUID

from flask import g, request

from app.auth.ldap_auth import require_auth
from app.routes._helpers import _parse_highlights, _parse_iso_date
from app.services.comments_service import CommentsService
from app.services.expert_service import ExpertService
from db.postgres_client import get_postgres_client
from ingestion.repository.users_repository import UsersRepository


def register_routes(app) -> None:


    @app.get("/api/circulars/<uuid:record_id>/experts")
    @require_auth
    def get_circular_experts(record_id: UUID):
        """List all expert tasks attached to a circular.

        Returns ``{"experts": [...]}``. An empty list is a valid response —
        the endpoint does not distinguish between "circular not found" and
        "circular exists with no experts", matching the frontend's empty
        state behavior.
        """
        try:
            db_client = get_postgres_client()
            service = ExpertService(db_pool=db_client.get_pool())
            experts = service.get_experts_for_circular(record_id)
        except Exception:
            return {
                "error": "Failed to load experts for circular.",
                "record_id": str(record_id),
            }, 500

        return {"experts": experts}



    @app.get("/api/circulars/<uuid:record_id>/experts/<uuid:expert_id>/comments")
    @require_auth
    def get_expert_comments(record_id: UUID, expert_id: UUID):
        """List all comments for an expert.

        Returns ``{"comments": [...]}``. An empty list is a valid response —
        the endpoint does not distinguish between "expert not found" and
        "expert exists with no comments", matching the frontend's empty
        state behavior.
        """
        try:
            service = CommentsService()
            comments = service.get_comments(expert_id)
        except Exception:
            return {
                "error": "Failed to load comments for expert.",
                "record_id": str(record_id),
                "expert_id": str(expert_id),
            }, 500

        return {"comments": comments}
    
    # ── "Experts by department" filter for the home screen ────────
    @app.get("/api/experts/by-department")
    @require_auth
    def get_experts_by_department() -> tuple[dict[str, Any], int] | dict[str, Any]:
        """List experts with optional filters.

        Query params (all optional):
            department_id   — UUID
            source          — str
            status          — "open" | "closed"
            from_date       — YYYY-MM-DD
            to_date         — YYYY-MM-DD
            full_circular_no — str
            page            — int (default 1)
            page_size       — int (default 25, max 200)

        Returns ``{"experts": {...}, "filters_applied": {...}}``.
        """

        raw_dept_id = request.args.get("department_id", "").strip() or None
        raw_source = request.args.get("source", "").strip() or None
        raw_status = request.args.get("status", "").strip().lower() or None
        raw_from_date = request.args.get("from_date", "").strip() or None
        raw_to_date = request.args.get("to_date", "").strip() or None
        raw_circular_no = request.args.get("full_circular_no", "").strip() or None

        try:
            page = max(1, int(request.args.get("page", 1)))
            page_size = max(1, min(int(request.args.get("page_size", 25)), 200))
        except ValueError:
            return {"error": "page and page_size must be integers."}, 400

        if raw_status and raw_status not in ("open", "closed"):
            return {"error": "status must be 'open', 'closed', or omitted."}, 400

        dept_uuid: Optional[UUID] = None
        if raw_dept_id:
            try:
                dept_uuid = UUID(raw_dept_id)
            except ValueError:
                return {"error": "Invalid department_id format"}, 400

        from_date_value: Optional[date] = _parse_iso_date(raw_from_date, "from_date")
        if isinstance(from_date_value, tuple):
            return from_date_value  # 400 error
        to_date_value: Optional[date] = _parse_iso_date(raw_to_date, "to_date")
        if isinstance(to_date_value, tuple):
            return to_date_value  # 400 error

        try:
            db_client = get_postgres_client()
            from app.services.expert_service import ExpertService  # local import avoids cycle in some envs
            service = ExpertService(db_pool=db_client.get_pool())
            experts_result = service.get_experts_by_department(
                department_id=dept_uuid,
                source=raw_source,
                status=raw_status,
                from_date=from_date_value,
                to_date=to_date_value,
                full_circular_no=raw_circular_no,
                page=page,
                page_size=page_size,
            )
        except Exception:
            return {"error": "Failed to load experts."}, 500

        return {
            "experts": experts_result,
            "filters_applied": {
                "department_id": raw_dept_id,
                "source": raw_source,
                "status": raw_status,
                "from_date": raw_from_date,
                "to_date": raw_to_date,
                "full_circular_no": raw_circular_no,
            },
        }

    @app.post("/api/circulars/<uuid:record_id>/experts/<uuid:expert_id>/comments")
    @require_auth
    def create_expert_comment(record_id: UUID, expert_id: UUID) -> tuple[dict, int]:
        """Create a comment on an expert task.

        Body: ``{"text": "..."}`` — required, non-empty after strip,
        max 5000 chars.

        Returns ``{"comment": {...}}`` with 201 on success. Returns 400
        if the text is missing/empty or exceeds the length cap.
        """
        body = request.get_json() or {}
        text = body.get("text", "").strip()
        if not text:
            return {"error": "text is required"}, 400
        if len(text) > 5000:
            return {"error": "text exceeds 5000 character limit"}, 400

        try:
            service = CommentsService()
            comment = service.create_comment(expert_id, g.user_db_id, text)
        except Exception:
            return {
                "error": "Failed to create comment.",
                "record_id": str(record_id),
                "expert_id": str(expert_id),
            }, 500

        return {"comment": comment}, 201

    @app.patch("/api/circulars/<uuid:record_id>/experts/<uuid:expert_id>/status")
    @require_auth
    def update_expert_status(record_id: UUID, expert_id: UUID) -> tuple[dict, int]:
        """Open or close an expert task.

        Body: ``{"status": "open" | "closed"}`` — required.

        Returns ``{"success": True}`` on update, 404 if the expert id
        does not exist, 400 if the status value is invalid.
        """

        body = request.get_json() or {}
        status = body.get("status", "").strip()
        if status not in ("open", "closed"):
            return {"error": "status must be 'open' or 'closed'"}, 400

        try:
            db_client = get_postgres_client()
            service = ExpertService(db_pool=db_client.get_pool())
            success = service.update_expert_status(expert_id, status)
        except Exception:
            return {
                "error": "Failed to update expert status.",
                "record_id": str(record_id),
                "expert_id": str(expert_id),
            }, 500

        if not success:
            return {
                "error": "Expert not found",
                "record_id": str(record_id),
                "expert_id": str(expert_id),
            }, 404
        return {"success": True}

    # ── RESTful CRUD on a single expert ──────────────────────────
    # These four endpoints replace the old snapshot/diff
    # `save_circular_experts` POST. Each user action in the UI
    # (add one expert, edit one expert, delete one expert, move an
    # expert to different departments) maps to exactly one of these
    # calls — no original_ids diff, no bundling, one transaction per
    # operation.

    @app.post("/api/circulars/<uuid:record_id>/experts")
    @require_auth
    def create_expert(record_id: UUID) -> tuple[dict, int]:
        """Add one expert task to a circular.

        Request body:
            title       (str)         — required, non-empty after strip.
            text        (str)         — optional, defaults to "".
            dept_ids    (list[UUID])   — required, must be non-empty. New
                                        experts without at least one
                                        department are rejected to avoid
                                        orphan rows.
            highlights  (list[dict] | JSON string) — optional.

        Returns:
            ``201 {"expert": {...}}`` on success, with the new expert id,
            circular_id, title, text, dept_ids, highlights, and the
            initial ``status: "open"``.

            ``400`` on validation failure (missing title, missing
            dept_ids, malformed UUID in dept_ids).

            ``500`` on database failure.
        """
        body = request.get_json() or {}
        title = body.get("title", "").strip()
        text = body.get("text", "").strip()
        raw_dept_ids = body.get("dept_ids") or []
        highlights = _parse_highlights(body.get("highlights"))

        if not title:
            return {"error": "title is required"}, 400
        if not raw_dept_ids:
            return {"error": "dept_ids is required"}, 400
        try:
            dept_ids = [UUID(d) for d in raw_dept_ids if d]
        except (ValueError, TypeError):
            return {"error": "dept_ids must be UUIDs"}, 400

        db_client = get_postgres_client()
        users_repo = UsersRepository(db_pool=db_client.get_pool())
        user = users_repo.get_user(g.current_user)
        user_dep_id = user.department_id if user else None
        try:
            user_db_id = UUID(g.user_db_id) if g.user_db_id else None
        except (ValueError, TypeError):
            user_db_id = None

        try:
            service = ExpertService(db_pool=db_client.get_pool())
            new_id = service.add_expert(
                circular_id=record_id,
                title=title,
                text=text,
                dept_ids=dept_ids,
                highlights=highlights,
                created_by_user_id=user_db_id,
                created_by_dep_id=user_dep_id,
            )
        except Exception:
            return {
                "error": "Failed to create expert.",
                "record_id": str(record_id),
            }, 500

        return {
            "expert": {
                "id": str(new_id),
                "circular_id": str(record_id),
                "title": title,
                "text": text,
                "dept_ids": [str(d) for d in dept_ids],
                "highlights": highlights,
                "status": "open",
            }
        }, 201

    @app.put("/api/circulars/<uuid:record_id>/experts/<uuid:expert_id>")
    @require_auth
    def update_expert(record_id: UUID, expert_id: UUID) -> tuple[dict, int]:
        """Update one expert's content fields (title, text, highlights).

        Departments are managed separately via
        ``PUT /api/circulars/<id>/experts/<expert_id>/departments`` — they
        are not part of this payload.

        Request body:
            title       (str)          — required, non-empty after strip.
            text        (str)          — optional, defaults to "".
            highlights  (list[dict] | JSON string) — optional.

        Returns:
            ``200 {"success": true}`` on update.

            ``400`` if title is missing.

            ``404`` if the expert id does not exist.

            ``500`` on database failure.
        """
        body = request.get_json() or {}
        title = body.get("title", "").strip()
        text = body.get("text", "").strip()
        highlights = _parse_highlights(body.get("highlights"))

        if not title:
            return {"error": "title is required"}, 400

        try:
            db_client = get_postgres_client()
            service = ExpertService(db_pool=db_client.get_pool())
            success = service.update_expert(expert_id, title, text, highlights)
        except Exception:
            return {
                "error": "Failed to update expert.",
                "record_id": str(record_id),
                "expert_id": str(expert_id),
            }, 500

        if not success:
            return {
                "error": "Expert not found",
                "record_id": str(record_id),
                "expert_id": str(expert_id),
            }, 404
        return {"success": True}

    @app.delete("/api/circulars/<uuid:record_id>/experts/<uuid:expert_id>")
    @require_auth
    def delete_expert(record_id: UUID, expert_id: UUID) -> tuple[dict, int]:
        """Hard-delete one expert task.

        CASCADE wipes the corresponding ``expert_departments_mapping``
        rows, so department linkage is cleaned up automatically.

        Returns:
            ``200 {"success": true}`` on delete.

            ``404`` if the expert id does not exist.

            ``500`` on database failure.
        """
        try:
            db_client = get_postgres_client()
            service = ExpertService(db_pool=db_client.get_pool())
            success = service.delete_expert(expert_id)
        except Exception:
            return {
                "error": "Failed to delete expert.",
                "record_id": str(record_id),
                "expert_id": str(expert_id),
            }, 500

        if not success:
            return {
                "error": "Expert not found",
                "record_id": str(record_id),
                "expert_id": str(expert_id),
            }, 404
        return {"success": True}

    @app.put("/api/circulars/<uuid:record_id>/experts/<uuid:expert_id>/departments")
    @require_auth
    def set_expert_departments(record_id: UUID, expert_id: UUID) -> tuple[dict, int]:
        """Replace the department assignments for one expert.

        All previous ``expert_departments_mapping`` rows for this expert
        are deleted, then the new list is inserted in their place. This
        is a wholesale replace — partial overlap is not preserved.

        Request body:
            dept_ids (list[UUID]) — required. Empty list is valid and
                                    clears all department assignments.

        Returns:
            ``200 {"success": true}`` on update.

            ``400`` if dept_ids is missing, not a list, or contains
            malformed UUIDs.

            ``404`` if the expert id does not exist.

            ``500`` on database failure.
        """
        body = request.get_json() or {}
        raw_dept_ids = body.get("dept_ids", [])
        if not isinstance(raw_dept_ids, list):
            return {"error": "dept_ids must be a list"}, 400
        try:
            dept_ids = [UUID(d) for d in raw_dept_ids if d]
        except (ValueError, TypeError):
            return {"error": "dept_ids must be UUIDs"}, 400

        try:
            db_client = get_postgres_client()
            service = ExpertService(db_pool=db_client.get_pool())
            success = service.set_expert_departments(expert_id, dept_ids)
        except Exception:
            return {
                "error": "Failed to set expert departments.",
                "record_id": str(record_id),
                "expert_id": str(expert_id),
            }, 500

        if not success:
            return {
                "error": "Expert not found",
                "record_id": str(record_id),
                "expert_id": str(expert_id),
            }, 404
        return {"success": True}
