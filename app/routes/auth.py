"""
Auth routes: login, current user lookup, and the root health check.
"""
from __future__ import annotations

import logging

from flask import g, request
import psycopg2

from app.auth.ldap_auth import LDAPAuth, require_auth
from db.postgres_client import get_postgres_client
from ingestion.repository.properties_repository import PropertiesRepository
from ingestion.repository.users_repository import UsersRepository

logger = logging.getLogger(__name__)


def register_routes(app) -> None:
    @app.post("/api/auth/login")
    def login():
        body = request.get_json() or {}
        username = body.get("username", "").strip()
        password = body.get("password", "")

        if not username or not password:
            return {"error": "Username and password are required."}, 400

        auth = LDAPAuth()
        try:
            if not auth.authenticate(username, password):
                return {"error": "Invalid credentials"}, 401
        except Exception as e:
            logger.exception("LDAP auth error for %s", username)
            return {"error": "Auth service error"}, 500

        # Look up the provisioned DB row for this LDAP uid.
        db_client = get_postgres_client()
        users_repo = UsersRepository(db_pool=db_client.get_pool())
        try:
            user = users_repo.get_user(username)
            user_db_id = str(user.id) if user else None
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            logging.getLogger(__name__).error(
                "DB unavailable during login for %s: %s", username, e
            )
            return {"error": "Service temporarily unavailable"}, 503

        # Explicit empty-result handling: if the LDAP-authenticated user has no
        # row in the users table, refuse to issue a token. require_auth blocks
        # every protected route on g.user_db_id, so a None-id token would let
        # the user click around and then 403 on the first real request.
        if not user_db_id:
            logging.getLogger(__name__).warning(
                "LDAP user %s has no row in users table; refusing login", username
            )
            return {"error": "User not provisioned in DB"}, 403

        token = auth.create_token(username, user_db_id)
        return {"access_token": token, "token_type": "bearer"}

    @app.get("/api/auth/me")
    @require_auth
    def me():
        db_client = get_postgres_client()
        users_repo = UsersRepository(db_pool=db_client.get_pool())
        properties_repo = PropertiesRepository(db_pool=db_client.get_pool())

        user = users_repo.get_user(g.current_user)
        if not user:
            return {"username": g.current_user, "department": None}

        dept = properties_repo.get_by_id(user.department_id)
        return {
            "username": g.current_user,
            "user_db_id": str(user.id),
            "department_id": str(user.department_id),
            "department": dept.name if dept else None,
            "email": user.email,
            "name": user.name,
        }