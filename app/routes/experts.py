"""
Expert routes: CRUD on expert tasks attached to a circular, plus comments
on those tasks, plus the open/closed status flip, plus the
"experts by department" filter for the home screen.
"""
from __future__ import annotations

from uuid import UUID

from flask import g, request

from app.auth.ldap_auth import require_auth
from app.services.comments_service import CommentsService
from app.services.expert_service import ExpertService
from db.postgres_client import get_postgres_client
from ingestion.repository.users_repository import UsersRepository


def register_routes(app) -> None:
    @app.post("/api/circulars/<uuid:record_id>/experts")
    @require_auth
    def save_circular_experts(record_id):
        body = request.get_json() or {}
        experts = body.get("experts", [])
        original_ids = body.get("original_ids", [])

        if not experts:
            return {"error": "experts list is required"}, 400

        db_client = get_postgres_client()
        users_repo = UsersRepository(db_pool=db_client.get_pool())
        user = users_repo.get_user(g.current_user)
        user_dep_id = user.department_id if user else None
        try:
            user_db_id = UUID(g.user_db_id) if g.user_db_id else None
        except (ValueError, TypeError):
            user_db_id = None

        service = ExpertService(db_pool=db_client.get_pool())
        result = service.save_experts(
            record_id, experts, original_ids, user_db_id, user_dep_id
        )
        return result

    @app.get("/api/circulars/<uuid:record_id>/experts")
    @require_auth
    def get_circular_experts(record_id):
        db_client = get_postgres_client()
        service = ExpertService(db_pool=db_client.get_pool())
        experts = service.get_experts_for_circular(record_id)
        return {"experts": experts}

    @app.patch("/api/circulars/<uuid:record_id>/experts/<uuid:expert_id>/status")
    @require_auth
    def update_expert_status(record_id, expert_id):
        body = request.get_json() or {}
        status = body.get("status", "").strip()
        if status not in ("open", "closed"):
            return {"error": "status must be 'open' or 'closed'"}, 400

        db_client = get_postgres_client()
        service = ExpertService(db_pool=db_client.get_pool())
        success = service.update_expert_status(expert_id, status)
        if not success:
            return {"error": "Expert not found"}, 404
        return {"success": True}

    # ── Comments nested under an expert ──────────────────────────
    @app.get("/api/circulars/<uuid:record_id>/experts/<uuid:expert_id>/comments")
    @require_auth
    def get_expert_comments(record_id, expert_id):
        service = CommentsService()
        comments = service.get_comments(expert_id)
        return {"comments": comments}

    @app.post("/api/circulars/<uuid:record_id>/experts/<uuid:expert_id>/comments")
    @require_auth
    def create_expert_comment(record_id, expert_id):
        body = request.get_json() or {}
        text = body.get("text", "").strip()
        if not text:
            return {"error": "text is required"}, 400

        service = CommentsService()
        comment = service.create_comment(expert_id, g.user_db_id, g.current_user, text)
        return {"comment": comment}, 201

    # ── "Experts by department" filter for the home screen ────────
    @app.get("/api/experts/by-department")
    @require_auth
    def get_experts_by_department():
        # Body of this handler is preserved verbatim from the original file —
        # it accepts a wide range of optional query params and returns the
        # same paginated response shape as before.
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

        dept_uuid = None
        if raw_dept_id:
            try:
                dept_uuid = UUID(raw_dept_id)
            except ValueError:
                return {"error": "Invalid department_id format"}, 400

        from_date = None
        if raw_from_date:
            try:
                from datetime import date as _date
                from_date = _date.fromisoformat(raw_from_date)
            except ValueError:
                return {"error": "from_date must be YYYY-MM-DD"}, 400

        to_date = None
        if raw_to_date:
            try:
                from datetime import date as _date
                to_date = _date.fromisoformat(raw_to_date)
            except ValueError:
                return {"error": "to_date must be YYYY-MM-DD"}, 400

        db_client = get_postgres_client()
        from app.services.expert_service import ExpertService  # local import avoids cycle in some envs
        service = ExpertService(db_pool=db_client.get_pool())
        experts_result = service.get_experts_by_department(
            department_id=dept_uuid,
            source=raw_source,
            status=raw_status,
            from_date=from_date,
            to_date=to_date,
            full_circular_no=raw_circular_no,
            page=page,
            page_size=page_size,
        )

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