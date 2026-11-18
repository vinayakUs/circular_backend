"""
Topics routes: per-user notification topic CRUD.

All endpoints are user-scoped via g.user_db_id — a user can only ever
see or touch their own topics.
"""
from __future__ import annotations

from flask import g, request
from psycopg2 import IntegrityError

from app.auth.ldap_auth import require_auth
from app.notifications.topics.user_notification_prefs_repository import (
    DIGEST_FREQUENCIES,
    UserNotificationPrefsRepository,
)
from app.notifications.topics.user_source_subscriptions_repository import (
    SOURCES,
    UserSourceSubscriptionsRepository,
)
from app.notifications.topics.user_topics_repository import UserTopicRepository
from db.postgres_client import get_postgres_client

ALERT_LEVELS = {"IMMEDIATE", "DAILY", "WEEKLY", "OFF"}


def _topic_json(t) -> dict:
    return {
        "id": str(t.id),
        "topic_name": t.topic_name,
        "keywords": t.keywords,
        "sources": t.sources,
        "alert_level": t.alert_level,
        "created_at": t.created_at.isoformat(),
        "updated_at": t.updated_at.isoformat(),
    }


def _validate(payload: dict, *, partial: bool) -> tuple[dict, str | None]:
    """Extract and validate topic fields. Returns (fields, error_message)."""
    fields: dict = {}

    if "topic_name" in payload or not partial:
        name = (payload.get("topic_name") or "").strip()
        if not name:
            return {}, "topic_name is required"
        if len(name) > 150:
            return {}, "topic_name must be 150 characters or fewer"
        fields["topic_name"] = name

    for key in ("keywords", "sources"):
        if key in payload or not partial:
            value = payload.get(key, [])
            if not isinstance(value, list) or not all(isinstance(x, str) for x in value):
                return {}, f"{key} must be an array of strings"
            fields[key] = [x.strip() for x in value if x.strip()]

    if "sources" in fields and not fields["sources"]:
        return {}, "at least one source is required"

    if "alert_level" in payload or not partial:
        level = (payload.get("alert_level") or "DAILY").upper()
        if level not in ALERT_LEVELS:
            return {}, f"alert_level must be one of {sorted(ALERT_LEVELS)}"
        fields["alert_level"] = level

    return fields, None


def register_routes(app) -> None:
    @app.get("/api/topics")
    @require_auth
    def list_topics():
        if not g.user_db_id:
            return {"error": "No linked user account"}, 403
        repo = UserTopicRepository(get_postgres_client().get_pool())
        return [_topic_json(t) for t in repo.list_by_user(g.user_db_id)]

    @app.post("/api/topics")
    @require_auth
    def create_topic():
        if not g.user_db_id:
            return {"error": "No linked user account"}, 403
        fields, error = _validate(request.get_json(silent=True) or {}, partial=False)
        if error:
            return {"error": error}, 400

        repo = UserTopicRepository(get_postgres_client().get_pool())
        topic = repo.create(user_id=g.user_db_id, **fields)
        if topic is None:
            return {"error": "A topic with this name already exists"}, 409
        return _topic_json(topic), 201

    @app.put("/api/topics/<uuid:topic_id>")
    @require_auth
    def update_topic(topic_id):
        if not g.user_db_id:
            return {"error": "No linked user account"}, 403
        fields, error = _validate(request.get_json(silent=True) or {}, partial=True)
        if error:
            return {"error": error}, 400
        if not fields:
            return {"error": "No fields to update"}, 400

        repo = UserTopicRepository(get_postgres_client().get_pool())
        try:
            topic = repo.update(g.user_db_id, topic_id, **fields)
        except IntegrityError:
            return {"error": "A topic with this name already exists"}, 409
        if topic is None:
            return {"error": "Topic not found"}, 404
        return _topic_json(topic)

    @app.delete("/api/topics/<uuid:topic_id>")
    @require_auth
    def delete_topic(topic_id):
        if not g.user_db_id:
            return {"error": "No linked user account"}, 403
        repo = UserTopicRepository(get_postgres_client().get_pool())
        if not repo.delete(g.user_db_id, topic_id):
            return {"error": "Topic not found"}, 404
        return "", 204

    @app.get("/api/notification-prefs")
    @require_auth
    def get_notification_prefs():
        if not g.user_db_id:
            return {"error": "No linked user account"}, 403
        repo = UserNotificationPrefsRepository(get_postgres_client().get_pool())
        prefs = repo.get(g.user_db_id)
        return {"digest_frequency": prefs.digest_frequency if prefs else "NONE"}

    @app.put("/api/notification-prefs")
    @require_auth
    def update_notification_prefs():
        if not g.user_db_id:
            return {"error": "No linked user account"}, 403
        payload = request.get_json(silent=True) or {}
        level = (payload.get("digest_frequency") or "").upper()
        if level not in DIGEST_FREQUENCIES:
            return {"error": f"digest_frequency must be one of {sorted(DIGEST_FREQUENCIES)}"}, 400
        repo = UserNotificationPrefsRepository(get_postgres_client().get_pool())
        prefs = repo.upsert(g.user_db_id, level)
        return {"digest_frequency": prefs.digest_frequency}

    @app.get("/api/source-subscriptions")
    @require_auth
    def get_source_subscriptions():
        if not g.user_db_id:
            return {"error": "No linked user account"}, 403
        repo = UserSourceSubscriptionsRepository(get_postgres_client().get_pool())
        return {"sources": repo.list_for_user(g.user_db_id)}

    @app.put("/api/source-subscriptions")
    @require_auth
    def update_source_subscriptions():
        if not g.user_db_id:
            return {"error": "No linked user account"}, 403
        payload = request.get_json(silent=True) or {}
        sources = payload.get("sources", [])
        if not isinstance(sources, list) or not all(isinstance(x, str) for x in sources):
            return {"error": "sources must be an array of strings"}, 400
        sources = [x.strip().upper() for x in sources if x.strip()]
        if set(sources) - SOURCES:
            return {"error": f"sources must be one of {sorted(SOURCES)}"}, 400
        repo = UserSourceSubscriptionsRepository(get_postgres_client().get_pool())
        return {"sources": repo.set_sources(g.user_db_id, sources)}
