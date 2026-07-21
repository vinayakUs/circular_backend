"""
Property routes: create + list generic properties (departments are a 'type'
under this), plus the signatories list used by the expert filter UI.
"""
from __future__ import annotations

from flask import request

from app.auth.ldap_auth import require_auth
from db.postgres_client import get_postgres_client
from ingestion.repository.properties_repository import PropertiesRepository


def register_routes(app) -> None:
    @app.post("/api/properties")
    def create_property():
        body = request.get_json() or {}
        name = body.get("name", "").strip()
        prop_type = body.get("type", "").strip()
        metadata = body.get("metadata") or {}

        if not name:
            return {"error": "name is required"}, 400
        if not prop_type:
            return {"error": "type is required"}, 400

        db_client = get_postgres_client()
        repository = PropertiesRepository(db_pool=db_client.get_pool())
        record = repository.create(name, prop_type, metadata)
        if record is None:
            return {"error": "A property with this name and type already exists"}, 409
        return {
            "id": str(record.id),
            "name": record.name,
            "type": record.type,
            "metadata": record.metadata,
            "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat(),
        }, 201

    @app.get("/api/signatories")
    def list_signatories():
        db_client = get_postgres_client()
        from ingestion.repository.circular_signatory_repository import CircularSignatoryRepository
        repo = CircularSignatoryRepository(db_pool=db_client.get_pool())
        names = repo.list_distinct_names()
        return {"items": [{"name": n} for n in names]}

    @app.get("/api/departments")
    def list_departments():
        # NOTE: legacy endpoint — the frontend uses /api/properties/department
        # instead. Preserved as-is to avoid changing behaviour.
        db_client = get_postgres_client()

    @app.get("/api/properties/<string:prop_type>")
    @require_auth
    def list_properties(prop_type: str):
        include_archived = (
            request.args.get("include_archived", "false").strip().lower() == "true"
        )
        db_client = get_postgres_client()
        repository = PropertiesRepository(db_pool=db_client.get_pool())
        records = repository.list_by_type(prop_type, include_archived=include_archived)
        return {
            "type": prop_type,
            "items": [
                {
                    "id": str(r.id),
                    "name": r.name,
                    "archived": r.archived,
                    "metadata": r.metadata,
                    "created_at": r.created_at.isoformat(),
                    "updated_at": r.updated_at.isoformat(),
                }
                for r in records
            ],
        }