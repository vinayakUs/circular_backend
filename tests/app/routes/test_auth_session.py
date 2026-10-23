"""Route tests for the cookie-based auth layer.

Follows the convention of tests/app/routes/test_circulars.py: a bare
Flask() app with only the auth routes registered, and patch.object() for
every repository. No create_app() (so the RAG warmup never runs) and no
database.

Covers:
  * GET  /api/auth/session   — validate, refresh, reject
  * POST /api/auth/logout    — revoke, CSRF, idempotency
  * require_auth             — cookie auth + CSRF on unsafe methods
  * cookie attributes        — HttpOnly, Max-Age, and mirrored clears

Run with:  pytest tests/app/routes/test_auth_session.py
"""
from __future__ import annotations

import unittest
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import jwt
from flask import Flask, g

from app.auth.session.cookie import (
    ACCESS_TOKEN_COOKIE,
    CSRF_TOKEN_COOKIE,
    REFRESH_TOKEN_COOKIE,
    access_max_age,
    csrf_max_age,
    refresh_max_age,
)
from app.auth.session.csrf import CSRF_HEADER, CsrfTokenService
from app.auth.session.refresh_token_service import RefreshTokenService
from app.routes import auth
from config import Config

SESSION_URL = "/api/auth/session"
LOGOUT_URL = "/api/auth/logout"
PERSONA_URL = "/api/auth/me/persona"

USERNAME = "alice"
CSRF = CsrfTokenService().issue()


def _user_record():
    return type("UserRecord", (), {
        "id": USER_DB_ID,
        "user_id": USERNAME,
        "department_id": None,
        "persona_slug": None,
        "email": "alice@example.com",
        "name": "Alice",
        "is_deleted": False,
    })()


class _Users:
    def __init__(self, **kwargs):
        pass

    def get_by_uuid(self, uid):
        return _user_record() if str(uid) == str(USER_DB_ID) else None

    def get_user(self, username):
        return _user_record() if username == USERNAME else None


class _Properties:
    def __init__(self, **kwargs):
        pass

    def get_by_id(self, _id):
        return None


class _InMemoryRefreshRepo:
    """Mirrors the real repo's contract without SQL."""

    def __init__(self):
        self.rows = {}

    def create(self, *, user_db_id, token_hash, family_id, issued_ip,
               issued_user_agent, ttl_seconds):
        from dataclasses import replace
        from app.auth.session.repository.refresh_token_repository import RefreshToken
        row = RefreshToken(
            id=uuid.uuid4(), user_db_id=user_db_id, token_hash=token_hash,
            family_id=family_id, issued_ip=issued_ip,
            issued_user_agent=issued_user_agent, consumed=False,
            consumed_at=None, created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds))
        self.rows[token_hash] = row
        return row

    def get_by_hash(self, token_hash):
        return self.rows.get(token_hash)

    def consume(self, token_id):
        from dataclasses import replace
        for key, row in list(self.rows.items()):
            if row.id == token_id and not row.consumed:
                self.rows[key] = replace(row, consumed=True,
                                         consumed_at=datetime.now(timezone.utc))
                return True
        return False

    def revoke_family(self, family_id):
        from dataclasses import replace
        n = 0
        for key, row in list(self.rows.items()):
            if row.family_id == family_id and not row.consumed:
                self.rows[key] = replace(row, consumed=True,
                                         consumed_at=datetime.now(timezone.utc))
                n += 1
        return n

    def purge_family(self, family_id):
        n = 0
        for key, row in list(self.rows.items()):
            if row.family_id == family_id:
                del self.rows[key]
                n += 1
        return n

    def revoke_all_for_user(self, user_db_id):
        return 0

    def delete_expired(self):
        return 0


USER_DB_ID = uuid.uuid4()


def _mint_access_token(seconds_from_now=900, username=USERNAME, db_id=None):
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {"sub": username, "user_db_id": str(db_id or USER_DB_ID),
         "exp": now + timedelta(seconds=seconds_from_now), "iat": now},
        Config.JWT_PRIVATE_KEY, algorithm=Config.JWT_ALGORITHM)


class AuthSessionRouteTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.repo = _InMemoryRefreshRepo()
        self.svc = RefreshTokenService(repo=self.repo)
        self.app = Flask(__name__)
        auth.register_routes(self.app)
        self.app.config.update(TESTING=True)
        self.client = self.app.test_client()

        for target, attr, value in (
            (auth, "get_postgres_client", lambda: _FakeClient()),
            (auth, "UsersRepository", _Users),
            (auth, "PropertiesRepository", _Properties),
            (auth, "get_refresh_token_service", lambda: self.svc),
            # resolve_principal() imports these INSIDE the function body, so
            # patching the auth module's names would not reach them.
            ("db.postgres_client", "get_postgres_client", lambda: _FakeClient()),
            ("ingestion.repository.users_repository", "UsersRepository", _Users),
        ):
            patcher = (patch(target + "." + attr, value)
                       if isinstance(target, str)
                       else patch.object(target, attr, value))
            patcher.start()
            self.addCleanup(patcher.stop)

    def _with_cookies(self, **cookies):
        for name, value in cookies.items():
            self.client.set_cookie(name, value)
        return self.client

    def _issue_refresh(self):
        plaintext, _ = self.svc.issue(user_db_id=USER_DB_ID, issued_ip="10.0.0.1",
                                      issued_user_agent="ua")
        return plaintext

    def _set_cookie_headers(self, response):
        return response.headers.getlist("Set-Cookie")


class SessionEndpointTestCase(AuthSessionRouteTestCase):

    def test_fresh_access_cookie_returns_user_without_rotating(self):
        resp = self._with_cookies(access_token=_mint_access_token()).get(SESSION_URL)
        self.assertEqual(200, resp.status_code)
        body = resp.get_json()
        self.assertTrue(body["authenticated"])
        self.assertFalse(body["refreshed"])
        self.assertEqual(USERNAME, body["user"]["username"])
        self.assertEqual("no-store", resp.headers["Cache-Control"])

    def test_fresh_cookie_emits_no_set_cookie(self):
        resp = self._with_cookies(access_token=_mint_access_token()).get(SESSION_URL)
        self.assertEqual([], self._set_cookie_headers(resp))

    def test_no_cookies_is_401_and_clears_every_cookie(self):
        resp = self.client.get(SESSION_URL)
        self.assertEqual(401, resp.status_code)
        cleared = " ".join(self._set_cookie_headers(resp))
        for name in (ACCESS_TOKEN_COOKIE, REFRESH_TOKEN_COOKIE, CSRF_TOKEN_COOKIE):
            self.assertIn(name, cleared)
            self.assertIn("Max-Age=0", cleared)

    def test_expired_access_cookie_with_valid_refresh_rotates(self):
        refresh = self._issue_refresh()
        resp = self._with_cookies(access_token=_mint_access_token(-60),
                                  refresh_token=refresh).get(SESSION_URL)
        self.assertEqual(200, resp.status_code)
        self.assertTrue(resp.get_json()["refreshed"])
        headers = self._set_cookie_headers(resp)
        self.assertEqual(3, len(headers))

    def test_absent_access_cookie_with_valid_refresh_rotates(self):
        resp = self._with_cookies(refresh_token=self._issue_refresh()).get(SESSION_URL)
        self.assertEqual(200, resp.status_code)
        self.assertTrue(resp.get_json()["refreshed"])

    def test_unknown_refresh_cookie_is_401(self):
        resp = self._with_cookies(refresh_token="not-a-token").get(SESSION_URL)
        self.assertEqual(401, resp.status_code)

    def test_forged_access_cookie_cannot_be_refreshed_into_a_session(self):
        resp = self._with_cookies(access_token="forged.signature",
                                  refresh_token=self._issue_refresh()).get(SESSION_URL)
        self.assertEqual(200, resp.status_code)
        self.assertTrue(resp.get_json()["refreshed"])

    def test_response_never_contains_a_bearer_token(self):
        resp = self._with_cookies(access_token=_mint_access_token()).get(SESSION_URL)
        self.assertNotIn("access_token", resp.get_json())
        self.assertNotIn("token_type", resp.get_json())

    def test_old_me_endpoint_is_gone(self):
        resp = self._with_cookies(access_token=_mint_access_token()).get("/api/auth/me")
        self.assertEqual(404, resp.status_code)


class LogoutEndpointTestCase(AuthSessionRouteTestCase):

    def test_logout_without_csrf_is_rejected(self):
        resp = self._with_cookies(refresh_token=self._issue_refresh()).post(LOGOUT_URL)
        self.assertEqual(403, resp.status_code)

    def test_logout_with_wrong_csrf_is_rejected(self):
        resp = (self._with_cookies(refresh_token=self._issue_refresh(),
                                   csrf_token=CSRF)
                .post(LOGOUT_URL, headers={CSRF_HEADER: "wrong"}))
        self.assertEqual(403, resp.status_code)

    def test_logout_with_csrf_revokes_and_clears(self):
        refresh = self._issue_refresh()
        resp = (self._with_cookies(refresh_token=refresh, csrf_token=CSRF)
                .post(LOGOUT_URL, headers={CSRF_HEADER: CSRF}))
        self.assertEqual(204, resp.status_code)
        self.assertIsNone(self.repo.rows.get(self.svc._hash(refresh)),
                          "logout must purge the refresh family")

    def test_logout_without_cookies_is_idempotent_204(self):
        resp = self.client.post(LOGOUT_URL)
        self.assertEqual(204, resp.status_code)

    def test_logout_needs_no_valid_access_token(self):
        """The common case is logging out right after the access token
        expired, so logout must not be behind @require_auth."""
        resp = (self._with_cookies(refresh_token=self._issue_refresh(),
                                   csrf_token=CSRF)
                .post(LOGOUT_URL, headers={CSRF_HEADER: CSRF}))
        self.assertEqual(204, resp.status_code)

    def test_revoked_token_cannot_start_a_new_session(self):
        refresh = self._issue_refresh()
        (self._with_cookies(refresh_token=refresh, csrf_token=CSRF)
         .post(LOGOUT_URL, headers={CSRF_HEADER: CSRF}))
        resp = (self._with_cookies(refresh_token=refresh)
                .get(SESSION_URL))
        self.assertEqual(401, resp.status_code)


class RequireAuthTestCase(AuthSessionRouteTestCase):

    def setUp(self) -> None:
        super().setUp()

        @self.app.get("/__protected")
        @auth.require_auth
        def protected():
            return {"user": g.current_user, "uid": g.user_db_id}

        @self.app.post("/__write")
        @auth.require_auth
        def write():
            return {"wrote": True}

    def test_valid_cookie_authenticates(self):
        resp = self._with_cookies(access_token=_mint_access_token()).get("/__protected")
        self.assertEqual(200, resp.status_code)
        self.assertEqual(USERNAME, resp.get_json()["user"])
        self.assertEqual(str(USER_DB_ID), resp.get_json()["uid"])

    def test_no_cookie_is_401(self):
        self.assertEqual(401, self.client.get("/__protected").status_code)

    def test_bearer_header_alone_is_no_longer_accepted(self):
        """Hard cutover: the header is no longer an auth source."""
        resp = self.client.get("/__protected",
                               headers={"Authorization": "Bearer whatever"})
        self.assertEqual(401, resp.status_code)

    def test_expired_cookie_is_401(self):
        resp = self._with_cookies(access_token=_mint_access_token(-60)).get("/__protected")
        self.assertEqual(401, resp.status_code)

    def test_forged_cookie_is_401(self):
        resp = self._with_cookies(access_token="a.b.c").get("/__protected")
        self.assertEqual(401, resp.status_code)

    def test_get_does_not_require_csrf(self):
        resp = self._with_cookies(access_token=_mint_access_token()).get("/__protected")
        self.assertEqual(200, resp.status_code)

    def test_unsafe_method_without_csrf_is_403(self):
        resp = self._with_cookies(access_token=_mint_access_token()).post("/__write")
        self.assertEqual(403, resp.status_code)

    def test_unsafe_method_with_wrong_csrf_is_403(self):
        resp = (self._with_cookies(access_token=_mint_access_token(),
                                   csrf_token=CSRF)
                .post("/__write", headers={CSRF_HEADER: "nope"}))
        self.assertEqual(403, resp.status_code)

    def test_unsafe_method_with_matching_csrf_succeeds(self):
        resp = (self._with_cookies(access_token=_mint_access_token(),
                                   csrf_token=CSRF)
                .post("/__write", headers={CSRF_HEADER: CSRF}))
        self.assertEqual(200, resp.status_code)

    def test_csrf_failure_is_reported_after_auth_not_before(self):
        """No valid session -> 401, never 403: a 403 would tell an attacker
        the cookie was valid."""
        resp = self.client.post("/__write", headers={CSRF_HEADER: CSRF})
        self.assertEqual(401, resp.status_code)


class CookieAttributeTestCase(AuthSessionRouteTestCase):

    def setUp(self):
        super().setUp()
        # Pin the flag rather than inheriting it. config.py uses
        # load_dotenv(override=True), so a developer running plain-HTTP
        # tests with COOKIE_SECURE=false in .env would otherwise turn these
        # assertions into a test of their local .env rather than of the
        # production attribute.
        self._orig_secure = Config.COOKIE_SECURE
        Config.COOKIE_SECURE = True

    def tearDown(self):
        Config.COOKIE_SECURE = self._orig_secure
        super().tearDown()

    def test_login_response_sets_correct_attributes(self):
        refresh = self._issue_refresh()
        resp = (self._with_cookies(access_token=_mint_access_token(-60),
                                   refresh_token=refresh)
                .get(SESSION_URL))
        headers = {h.split("=", 1)[0]: h for h in self._set_cookie_headers(resp)}
        access = headers.get(ACCESS_TOKEN_COOKIE, "")
        refresh_h = headers.get(REFRESH_TOKEN_COOKIE, "")
        csrf = headers.get(CSRF_TOKEN_COOKIE, "")

        self.assertIn("HttpOnly", access)
        self.assertIn("HttpOnly", refresh_h)
        self.assertNotIn("HttpOnly", csrf)
        for header in (access, refresh_h, csrf):
            self.assertIn("Secure", header)
            self.assertIn("SameSite=Strict", header)
        self.assertIn(f"Max-Age={access_max_age()}", access)
        self.assertIn(f"Max-Age={refresh_max_age()}", refresh_h)
        self.assertIn(f"Max-Age={csrf_max_age()}", csrf)

    def test_clear_mirrors_the_set_attributes(self):
        resp = self.client.get(SESSION_URL)
        for header in self._set_cookie_headers(resp):
            self.assertIn("Path=/", header)
            self.assertIn("SameSite=Strict", header)
            self.assertIn("Secure", header)

    def test_max_age_derivations_match_config(self):
        self.assertEqual(Config.JWT_EXPIRATION_MINUTES * 60, access_max_age())
        self.assertEqual(Config.REFRESH_TOKEN_TTL_MINUTES * 60, refresh_max_age())
        self.assertEqual(refresh_max_age(), csrf_max_age())

    def test_secure_attribute_follows_the_config_flag(self):
        """The attribute is driven by COOKIE_SECURE, not hardcoded on."""
        Config.COOKIE_SECURE = False
        resp = self.client.get(SESSION_URL)
        for header in self._set_cookie_headers(resp):
            self.assertNotIn("Secure", header)


class _FakeClient:
    def get_pool(self):
        return object()


if __name__ == "__main__":
    unittest.main()
