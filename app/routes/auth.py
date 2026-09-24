"""
Auth routes: login, current user lookup, captcha, and the root health check.
"""
from __future__ import annotations

import base64
import logging
from uuid import UUID

from flask import g, request
import psycopg2

from app.auth.captcha.captcha_service import get_captcha_service
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
        captcha_id = body.get("captcha_id", "").strip()
        captcha_answer = body.get("captcha_answer", "")

        if not username or not password:
            return {"error": "Username and password are required."}, 400

        # CAPTCHA first — bots can't even reach LDAP with bad creds.
        # Both fields are required so empty/absent captcha is a 401,
        # not a 500.  Importantly, the LDAP server is never contacted
        # if the captcha is wrong — protects the directory server
        # from brute-force enumeration.
        if not captcha_id or not captcha_answer:
            return {"error": "Invalid or expired CAPTCHA"}, 401
        try:
            captcha_uuid = UUID(captcha_id)
        except (ValueError, AttributeError):
            return {"error": "Invalid or expired CAPTCHA"}, 401
        if not get_captcha_service().verify(captcha_uuid, captcha_answer):
            return {"error": "Invalid or expired CAPTCHA"}, 401

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

        # MFA: mint a pending token instead of issuing JWT directly.
        from app.auth.mfa.make_services import get_mfa_services
        _, pending_token_service = get_mfa_services()
        try:
            mfa_token = pending_token_service.mint(
                user_db_id=user_db_id,
                issued_ip=request.remote_addr,
                issued_user_agent=request.headers.get("User-Agent", ""),
                )
        except Exception as exc:
            logger.exception("MFA pending_token mint failed for %s", username)
            return {"error": "Service temporarily unavailable"}, 503

        # Tell the browser which channels are available for this user.
        channels = []
        if user.email:
            channels.append("email")
        if user.phone_e164 and user.phone_verified_at:
            channels.append("sms")

        return {
            "mfa_required": True,
            "mfa_token": mfa_token,
            "channels": channels,
            "otp_length": 6,
            "ttl_seconds": 300,
        }, 200


    @app.post("/api/auth/mfa/verify")
    def mfa_verify():
        """Verify the OTP code and mint a JWT on success."""
        body = request.get_json() or {}
        mfa_token = (body.get("mfa_token") or "").strip()
        otp = (body.get("otp") or "").strip()

        if not mfa_token or not otp:
            return {"error": "Invalid credentials or verification code"}, 401

        from app.auth.mfa.make_services import get_mfa_services
        challenge_service, pending_token_service = get_mfa_services()

        pending = pending_token_service.verify(mfa_token)
        if pending is None:
            return {"error": "Invalid credentials or verification code"}, 401

        outcome = challenge_service.verify(
            user_db_id=pending.user_db_id,
            code=otp,
        )

        if not outcome.success:
            if outcome.locked:
                return {"error": "Too many attempts; request a new code"}, 423
            # Generic 401 for wrong code / expired / consumed / not found
            return {"error": "Invalid credentials or verification code"}, 401

        # Success — consume the pending token and mint the JWT.
        pending_token_service.consume(pending.token_id)

        db_client = get_postgres_client()
        users_repo = UsersRepository(db_pool=db_client.get_pool())
        user = users_repo.get_by_uuid(str(pending.user_db_id))
        if user is None or user.is_deleted:
            return {"error": "User not authorized"}, 403

        token = LDAPAuth().create_token(user.user_id, str(user.id))
        from flask import make_response
        resp = make_response(
            {"access_token": token, "token_type": "bearer"}, 200
        )
        # Set a non-HttpOnly cookie so the Angular auth interceptor
        # (JS-side) can read it from document.cookie and attach the
        # Authorization header on subsequent requests. SameSite=Strict
        # + Secure-when-HTTPS provides reasonable CSRF/XSS mitigation
        # for our internal app. (True HttpOnly would be more secure
        # but would force the interceptor to use a different source
        # like localStorage or an in-memory service.)
        resp.set_cookie(
            "access_token", token,
            httponly=False,
            secure=request.is_secure,
            samesite="Strict",
            path="/", max_age=900,
        )
        return resp

    @app.post("/api/auth/mfa/request")
    def mfa_request():
        """Send an OTP to one or more of the user's chosen channels.

        Generates ONE OTP code and sends it (via the same code) to
        every channel the user has configured. User types whichever
        they receive into the verify step.
        """
        body = request.get_json() or {}
        mfa_token = (body.get("mfa_token") or "").strip()
        channels = body.get("channels") or []

        if not mfa_token:
            return {"error": "Invalid credentials or verification code"}, 401
        if not isinstance(channels, list) or not channels:
            return {"error": "channels must be a non-empty list"}, 400
        if any(c not in {"email", "sms"} for c in channels):
            return {"error": "Invalid channel"}, 400

        from app.auth.mfa.make_services import get_mfa_services
        challenge_service, pending_token_service = get_mfa_services()

        pending = pending_token_service.verify(mfa_token)
        if pending is None:
            return {"error": "Invalid credentials or verification code"}, 401

        try:
            enabled = challenge_service.issue(
                user_db_id=pending.user_db_id,
                channels=channels,
            )
        except Exception:
            logger.exception(
                "mfa_request issue failed for user=%s", pending.user_db_id
            )
            return {"error": "Service temporarily unavailable"}, 503

        if not enabled:
            # None of the requested channels are enabled for this user
            return {"error": "No available channels"}, 400

        return {
            "sent": True,
            "channels": enabled,
            "ttl_seconds": 300,
        }, 200

    @app.get("/api/auth/captcha/new")
    def new_captcha():
        """Issue a fresh captcha and return it inline as a data URL.

        Returns
        -------
        ``{"challenge_id": "<uuid>", "image_data": "data:image/png;base64,..."}``
        with HTTP 200.

        Notes
        -----
        Single-call design: the browser gets both the challenge UUID
        *and* the rendered PNG in one round-trip. ``image_data`` is a
        standard ``data:`` URL that ``<img [src]="...">`` accepts
        directly — no second HTTP request needed for the image.

        The plaintext answer is **never** included in this response
        — only the SHA-256 hash is stored server-side.

        Public endpoint — no auth required (login hasn't happened yet).
        """
        challenge, png_bytes = get_captcha_service().new_challenge()
        image_data = (
            "data:image/png;base64," + base64.b64encode(png_bytes).decode("ascii")
        )
        return {
            "challenge_id": str(challenge.id),
            "image_data": image_data,
        }, 200

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
