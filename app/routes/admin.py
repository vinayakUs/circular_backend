"""
Admin routes: provisioning users into departments + per-user detail lookup.

All endpoints require authentication.
"""
from __future__ import annotations

from flask import g, request

from app.auth.ldap_auth import require_auth
from db.postgres_client import get_postgres_client
from ingestion.repository.properties_repository import PropertiesRepository
from ingestion.repository.users_repository import UsersRepository


def _serialize_user_record(u) -> dict:
    return {
        "id": str(u.id),
        "user_id": u.user_id,
        "department_id": str(u.department_id),
        "email": u.email,
        "name": u.name,
        "created_at": u.created_at.isoformat(),
        "created_by": u.created_by,
        "updated_at": u.updated_at.isoformat() if u.updated_at else None,
        "updated_by": u.updated_by,
    }


def register_routes(app) -> None:
    @app.get("/api/admin/departments/<uuid:dept_id>/users")
    @require_auth
    def list_department_users(dept_id):
        """List all users in a department."""
        db_client = get_postgres_client()
        repository = UsersRepository(db_pool=db_client.get_pool())
        users = repository.get_users_by_department(dept_id)
        return {"users": [_serialize_user_record(u) for u in users]}

    @app.post("/api/admin/departments/<uuid:dept_id>/users")
    @require_auth
    def add_user_to_department(dept_id):
        """Add a user to a department.

        Body: { user_id, email?, name? }
          • user_id — required, LDAP uid
          • email   — optional, stored in users.email (nullable)
          • name    — optional, stored in users.name (nullable)
        """
        body = request.get_json() or {}
        user_id = body.get("user_id", "").strip()
        email = body.get("email")
        name = body.get("name")
        created_by = g.current_user  # Use authenticated user automatically

        if not user_id:
            return {"error": "user_id is required"}, 400

        # Normalise empty strings to None so empty inputs don't write empty
        # strings to the DB columns.
        email_clean = (email or "").strip() or None
        name_clean = (name or "").strip() or None

        # Validate email format if provided.
        if email_clean and "@" not in email_clean:
            return {"error": "Invalid email format"}, 400

        db_client = get_postgres_client()
        repository = UsersRepository(db_pool=db_client.get_pool())

        # Verify department exists
        props_repo = PropertiesRepository(db_pool=db_client.get_pool())
        dept = props_repo.get_by_id(dept_id)
        if dept is None:
            return {"error": "Department not found"}, 404

        record = repository.add_user(
            user_id=user_id,
            department_id=dept_id,
            created_by=created_by,
            email=email_clean,
            name=name_clean,
        )
        if record is None:
            return {"error": "User already exists in this or another department"}, 409

        return _serialize_user_record(record), 201

    @app.delete("/api/admin/departments/<uuid:dept_id>/users/<user_id>")
    @require_auth
    def remove_user_from_department(dept_id, user_id):
        """Remove a user from a department."""
        db_client = get_postgres_client()
        repository = UsersRepository(db_pool=db_client.get_pool())
        deleted = repository.soft_delete_user(user_id, deleted_by=g.current_user)
        if not deleted:
            return {"error": "User not found"}, 404
        return {"success": True}

    @app.get("/api/admin/users/<user_id>")
    @require_auth
    def get_user_details(user_id):
        """Get user details by user_id."""
        db_client = get_postgres_client()
        repository = UsersRepository(db_pool=db_client.get_pool())
        user = repository.get_user(user_id)
        if user is None:
            return {"error": "User not found"}, 404
        return _serialize_user_record(user)