import ldap3
import jwt
import logging
import ssl
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any

from flask import request, g

from config import Config

logger = logging.getLogger(__name__)

# ==== AUTH METHOD SELECTOR ====
# Uncomment USE_NTLM = True for Windows AD (production)
# Uncomment USE_NTLM = False for OpenLDAP (development)

USE_NTLM = False  # Set to False to use simple bind
# ==============================


class LDAPAuth:
    """LDAP authentication and JWT token management."""

    def __init__(
        self,
        server: str | None = None,
        base_dn: str | None = None,
        user_dn_template: str | None = None,
    ):
        self.server = server or Config.LDAP_SERVER
        self.base_dn = base_dn or Config.LDAP_BASE_DN
        self.user_dn_template = user_dn_template or Config.LDAP_USER_DN_TEMPLATE

    def authenticate(self, username: str, password: str) -> bool:
        """Authenticate user against LDAP directory."""

        # ==== NTLM (Windows AD / Production) ====
        if USE_NTLM:
            conn = None
            try:
                from ldap3 import Tls, Server, Connection, ALL, NTLM
                tls = Tls(validate=ssl.CERT_NONE)
                srv = Server(self.server, port=Config.LDAP_PORT, use_ssl=True, tls=tls, get_info=ALL)
                user = f"{Config.LDAP_DOMAIN}\\{username}"
                conn = Connection(srv, user=user, password=password, authentication=NTLM, auto_bind=True)
                return True
            except ldap3.core.exceptions.LDAPBindError:
                # Invalid credentials - expected user error. Single INFO
                # line keeps failed logins auditable without burying real
                # ops incidents under a stacktrace per wrong-password attempt.
                logger.info("LDAP NTLM bind rejected for user=%s (invalid credentials)", username)
                return False
            except Exception:
                # Server unreachable, TLS failure, malformed config - these
                # ARE ops-level incidents and the traceback is the signal.
                logger.exception("LDAP NTLM Bind failed for user=%s", username)
                return False
            finally:
                if conn is not None:
                    try:
                        conn.unbind()
                    except Exception:
                        logger.warning("LDAP unbind failed (NTLM) for user=%s", username, exc_info=True)

        # ==== Simple Bind (OpenLDAP / Development) ====
        else:
            conn = None
            try:
                user_dn = self.user_dn_template.format(username=username)
                server = ldap3.Server(self.server, get_info=ldap3.DSA)
                conn = ldap3.Connection(server, user=user_dn, password=password, auto_bind=True)
                return conn.bound
            except ldap3.core.exceptions.LDAPBindError:
                # Invalid credentials - expected user error. Single INFO
                # line keeps failed logins auditable without burying real
                # ops incidents under a stacktrace per wrong-password attempt.
                logger.info("LDAP bind rejected for user=%s (invalid credentials)", username)
                return False
            except Exception:
                # Server unreachable, DNS failure, malformed config - these
                # ARE ops-level incidents and the traceback is the signal.
                logger.exception("LDAP Simple Bind failed for user=%s", username)
                return False
            finally:
                if conn is not None:
                    try:
                        conn.unbind()
                    except Exception:
                        logger.warning("LDAP unbind failed (simple bind) for user=%s", username, exc_info=True)

    def create_token(self, username: str, user_db_id: str | None = None) -> str:
        """Create JWT token for authenticated user.

        Raises RuntimeError if JWT_PRIVATE_KEY is not configured - login route
        catches this and returns 503, so the failure is visible to ops rather
        than silently falling back to a known-bad key.
        """
        if not Config.JWT_PRIVATE_KEY:
            raise RuntimeError(
                "JWT_PRIVATE_KEY is not configured. "
                "Set the env var to a PEM-encoded RSA private key."
            )
        payload = {
            "sub": username,
            "user_db_id": user_db_id,
            "exp": datetime.now(timezone.utc)
                + timedelta(minutes=Config.JWT_EXPIRATION_MINUTES),
            "iat": datetime.now(timezone.utc),
        }
        return jwt.encode(payload, Config.JWT_PRIVATE_KEY, algorithm=Config.JWT_ALGORITHM)

    @staticmethod
    def decode_token(token: str) -> dict[str, Any] | None:
        """Decode and validate JWT token. Returns payload or None if invalid.

        If JWT_PUBLIC_KEY is not configured, logs an error and rejects every
        token - we refuse to fall back to a known-bad signing key.
        """
        if not Config.JWT_PUBLIC_KEY:
            logger.error(
                "JWT_PUBLIC_KEY is not configured; rejecting token for security. "
                "Set the env var to a PEM-encoded RSA public key."
            )
            return None
        try:
            payload = jwt.decode(
                token,
                Config.JWT_PUBLIC_KEY,
                algorithms=[Config.JWT_ALGORITHM],
            )
            return payload
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None


def require_auth(f):
    """Decorator to require valid JWT token AND active (non-soft-deleted) user.

    After JWT validation, this decorator also checks `users.is_deleted`
    to enforce M1 (soft-delete defense). A user soft-deleted today can
    still use their existing JWT for up to JWT_EXPIRATION_MINUTES; this
    DB check blocks them at the auth layer instead.

    Cost: 1 extra DB query per protected request (~1-2ms). For higher
    traffic, cache the result in Redis (see C6 - token revocation).

    Failure mode: if the DB is unreachable, fail-open (allow the request
    through). The user is authenticated by a valid JWT; denying the
    request during a DB outage would lock everyone out.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return {"error": "Missing or invalid Authorization header"}, 401

        token = auth_header.replace("Bearer ", "")
        payload = LDAPAuth.decode_token(token)

        if payload is None:
            return {"error": "Invalid or expired token"}, 401

        g.current_user = payload.get("sub")
        g.user_db_id = payload.get("user_db_id")

        if not g.user_db_id:
            return {"error": "User not authorized"}, 403

        # M1: defense against soft-deleted users.
        # JWT is stateless - a user soft-deleted today can still use
        # their existing token for up to JWT_EXPIRATION_MINUTES. This
        # DB check blocks them at the auth layer for EVERY protected
        # route - comments, experts, mentions, all of them.
        from db.postgres_client import get_postgres_client
        from ingestion.repository.users_repository import UsersRepository
        try:
            users_repo = UsersRepository(db_pool=get_postgres_client().get_pool())
            user = users_repo.get_by_uuid(g.user_db_id)
        except Exception as exc:
            # Fail-open on DB error: don't lock users out if Postgres is down.
            logger.warning("M1 soft-delete check failed (fail-open): %s", exc)
            user = None
        if user is None:
            return {
                "error": "User not authorized (soft-deleted or unknown)"
            }, 403

        return f(*args, **kwargs)

    return decorated