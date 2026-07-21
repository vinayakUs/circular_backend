"""
Mention routes: inbox (list / unread-count / mark read), and the search
endpoint used by the @mention picker in the comment textarea.
"""
from __future__ import annotations

from flask import g, request

from app.auth.ldap_auth import require_auth
from app.services.mentions_service import MentionsService
from db.postgres_client import get_postgres_client
from ingestion.repository.properties_repository import PropertiesRepository
from ingestion.repository.users_repository import UsersRepository


def register_routes(app) -> None:
    @app.get("/api/mentions")
    @require_auth
    def list_mentions():
        limit = int(request.args.get("limit", 20))
        offset = int(request.args.get("offset", 0))
        status = request.args.get("status") or None
        unread_only = request.args.get("unread_only", "false").lower() == "true"
        svc = MentionsService()
        return svc.list_for_user(g.current_user, limit, offset, status, unread_only)

    @app.get("/api/mentions/unread-count")
    @require_auth
    def mentions_unread_count():
        svc = MentionsService()
        return {"unread_count": svc.unread_count(g.current_user)}

    @app.get("/api/mentions/search")
    @require_auth
    def search_mention_targets():
        """Fuzzy search across users + departments for the @mention picker.

        Query params:
          q       — required, ≥1 char; matches case-insensitively on user_id
                    or department name. Empty q returns the first N items
                    (so the popup has something to show on a fresh '@').
          limit   — optional, default 8 per group.

        Response: { users: [...], departments: [...] }
        """
        q = (request.args.get("q") or "").strip()
        try:
            limit = max(1, min(int(request.args.get("limit", 8)), 25))
        except ValueError:
            limit = 8

        db_client = get_postgres_client()
        users_repo = UsersRepository(db_pool=db_client.get_pool())
        props_repo = PropertiesRepository(db_pool=db_client.get_pool())

        uq = q.lower()
        users = [
            {
                "id": str(u.id),
                "user_id": u.user_id,
                "email": u.email,
                "name": u.name,
                "label": u.user_id,
            }
            for u in users_repo.list_all()
            if not uq or uq in u.user_id.lower()
        ][:limit]

        depts = props_repo.list_by_type("department", include_archived=False)
        departments = [
            {
                "id": str(d.id),
                "name": d.name,
                "label": f"@dep:{d.name}",
            }
            for d in depts
            if not uq or uq in d.name.lower()
        ][:limit]

        return {"users": users, "departments": departments}

    @app.post("/api/mentions/<uuid:notif_id>/read")
    @require_auth
    def mark_mention_read(notif_id):
        svc = MentionsService()
        row = svc.mark_read(g.current_user, notif_id)
        if not row:
            return {"error": "Mention not found"}, 404
        return row

    @app.post("/api/mentions/read-all")
    @require_auth
    def mark_all_mentions_read():
        svc = MentionsService()
        return {"updated": svc.mark_all_read(g.current_user)}