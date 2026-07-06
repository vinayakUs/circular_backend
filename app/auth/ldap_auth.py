import ldap3
import jwt
import ssl
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any

from flask import request, g

from config import Config

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
            try:
                from ldap3 import Tls, Server, Connection, ALL, NTLM
                tls = Tls(validate=ssl.CERT_NONE)
                srv = Server(self.server, port=Config.LDAP_PORT, use_ssl=True, tls=tls, get_info=ALL)
                user = f"{Config.LDAP_DOMAIN}\\{username}"
                conn = Connection(srv, user=user, password=password, authentication=NTLM, auto_bind=True)
                conn.unbind()
                return True
            except Exception as e:
                print(f"NTLM Auth failed: {e}")
                return False

        # ==== Simple Bind (OpenLDAP / Development) ====
        else:
            try:
                user_dn = self.user_dn_template.format(username=username)
                server = ldap3.Server(self.server, get_info=ldap3.DSA)
                conn = ldap3.Connection(server, user=user_dn, password=password, auto_bind=True)
                return conn.bound
            except Exception as e:
                print(f"Simple Bind failed: {e}")
                return False

    def create_token(self, username: str, user_db_id: str | None = None) -> str:
        """Create JWT token for authenticated user."""
        payload = {
            "sub": username,
            "user_db_id": user_db_id,
            "exp": datetime.now(timezone.utc) + timedelta(hours=Config.JWT_EXPIRATION_HOURS),
            "iat": datetime.now(timezone.utc),
        }
        return jwt.encode(payload, Config.JWT_SECRET, algorithm=Config.JWT_ALGORITHM)

    @staticmethod
    def decode_token(token: str) -> dict[str, Any] | None:
        """Decode and validate JWT token. Returns payload or None if invalid."""
        try:
            payload = jwt.decode(
                token,
                Config.JWT_SECRET,
                algorithms=[Config.JWT_ALGORITHM],
            )
            return payload
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None


def require_auth(f):
    """Decorator to require valid JWT token for endpoint access."""
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

        return f(*args, **kwargs)

    return decorated