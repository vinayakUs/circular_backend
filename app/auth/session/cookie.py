"""Auth cookie ownership.

The only module that writes or clears auth cookies, so set and clear always
use matching path/domain — a mismatch there makes the browser keep the cookie.
"""
from __future__ import annotations

from flask import request

from config import Config


ACCESS_TOKEN_COOKIE = "access_token"
REFRESH_TOKEN_COOKIE = "refresh_token"
CSRF_TOKEN_COOKIE = "csrf_token"

COOKIE_PATH = "/"


def access_max_age() -> int:
    """Derived from JWT_EXPIRATION_MINUTES so the cookie's max_age and the
    token's exp can never disagree."""
    return Config.JWT_EXPIRATION_MINUTES * 60


def refresh_max_age() -> int:
    """Refresh cookie lifetime; re-issued on every rotation."""
    return Config.REFRESH_TOKEN_TTL_MINUTES * 60


def csrf_max_age() -> int:
    """Tracks the refresh window, not the access window: the CSRF token
    rotates with the token pair, so an access-scale expiry would leave the
    client holding a dead value in the gap between refreshes."""
    return refresh_max_age()


def _attrs() -> dict:
    return {
        "path": COOKIE_PATH,
        # Empty config means host-only: omit the Domain attribute entirely.
        "domain": Config.COOKIE_DOMAIN or None,
        "secure": Config.COOKIE_SECURE,
        "samesite": Config.COOKIE_SAMESITE,
    }


def set_auth_cookies(
    response,
    *,
    access_token: str,
    refresh_token: str,
    csrf_token: str,
):
    """Attach the auth triple to a response."""
    response.set_cookie(
        ACCESS_TOKEN_COOKIE, access_token,
        httponly=True, max_age=access_max_age(), **_attrs(),
    )
    response.set_cookie(
        REFRESH_TOKEN_COOKIE, refresh_token,
        httponly=True, max_age=refresh_max_age(), **_attrs(),
    )
    # Readable by JS on purpose: the SPA must echo this value back in the
    # X-CSRF-Token header (double-submit). Safe to expose — it is not a
    # credential, and a stolen value is useless without the auth cookies.
    response.set_cookie(
        CSRF_TOKEN_COOKIE, csrf_token,
        httponly=False, max_age=csrf_max_age(), **_attrs(),
    )
    return response


def clear_auth_cookies(response):
    """Remove the auth triple. Flask emits expiring cookies whether or not
    one was actually present, so this is safe on an anonymous response."""
    for name in (ACCESS_TOKEN_COOKIE, REFRESH_TOKEN_COOKIE, CSRF_TOKEN_COOKIE):
        response.delete_cookie(name, httponly=False, **_attrs())
    return response


def get_access_token() -> str | None:
    return request.cookies.get(ACCESS_TOKEN_COOKIE)


def get_refresh_token() -> str | None:
    return request.cookies.get(REFRESH_TOKEN_COOKIE)


def get_csrf_cookie() -> str | None:
    return request.cookies.get(CSRF_TOKEN_COOKIE)
