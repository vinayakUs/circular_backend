"""End-to-end auth tests over REAL HTTP.

Unlike test_auth_session.py (Flask test client, mocked repositories) and
test_refresh_token_repository_live.py (real SQL, no routing), this suite
exercises the whole stack at once:

    real TCP socket  ->  real Flask WSGI  ->  real routes
                      ->  real Postgres   ->  real cookie jar

A real ``werkzeug`` server is started on an ephemeral port and driven with
``requests``, which brings its own ``http.cookiejar``. That matters: the
jar enforces the same rules a browser does, so a cookie that is not stored,
not sent, or not returned shows up here as a failure rather than passing
because a test helper was lenient.

Why COOKIE_SECURE is disabled here
----------------------------------
Python's cookiejar refuses to return a ``Secure`` cookie over plain http
(DefaultCookiePolicy.return_ok_secure). This suite speaks http to
127.0.0.1, so with the production default of COOKIE_SECURE=True no cookie
would ever be sent back and every authenticated case would fail for a
reason unrelated to the code under test. The Secure attribute itself is
still asserted from the raw Set-Cookie header below, and is covered
against the real production config in test_auth_session.py.

Skips unless RUN_LIVE_HTTP_TESTS=1 and Postgres is reachable.

Run with:
    RUN_LIVE_HTTP_TESTS=1 pytest tests/app/routes/test_auth_http_live.py -v
"""
from __future__ import annotations

import os
import threading
import unittest
import uuid
from datetime import datetime, timedelta, timezone

import jwt
import pytest
import requests
from werkzeug.serving import make_server

from app.auth.session.cookie import (
    ACCESS_TOKEN_COOKIE,
    CSRF_TOKEN_COOKIE,
    REFRESH_TOKEN_COOKIE,
)
from app.auth.session.csrf import CSRF_HEADER
from app.auth.session.make_services import get_refresh_token_service
from config import Config

SKIP_REASON = "set RUN_LIVE_HTTP_TESTS=1 (and a reachable Postgres) to run"

# COOKIE_DOMAIN is empty, so cookie.py emits no Domain attribute and the
# jar files server cookies under this host. Cookies injected by hand must
# use the same domain, or the jar ends up holding two entries with the same
# name (CookieConflictError) and Flask reads whichever arrives first.
_HOST = "127.0.0.1"


# ── real server ────────────────────────────────────────────────
@pytest.fixture(scope="module")
def live_server():
    if os.getenv("RUN_LIVE_HTTP_TESTS") != "1":
        pytest.skip(SKIP_REASON)

    from flask import Flask
    from werkzeug.middleware.proxy_fix import ProxyFix
    from app.routes import auth

    # http to 127.0.0.1 -> cookiejar would discard Secure cookies.
    # Restored in teardown: Config is module-global and cookie.py reads
    # it per request, so leaving it False would mask the Secure flag for
    # every later suite in the same process.
    original_secure = Config.COOKIE_SECURE
    Config.COOKIE_SECURE = False

    app = Flask(__name__)
    app.config.from_object("config.Config")
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    auth.register_routes(app)

    server = make_server("127.0.0.1", 0, app, threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield base_url
    finally:
        server.shutdown()
        thread.join(timeout=5)
        Config.COOKIE_SECURE = original_secure


@pytest.fixture(scope="module")
def pool():
    if os.getenv("RUN_LIVE_HTTP_TESTS") != "1":
        pytest.skip(SKIP_REASON)
    from db.postgres_client import get_postgres_client
    return get_postgres_client().get_pool()


@pytest.fixture(scope="module")
def real_user(pool):
    """A genuine users row. The FK on refresh_tokens demands it."""
    if os.getenv("RUN_LIVE_HTTP_TESTS") != "1":
        pytest.skip(SKIP_REASON)
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('public.refresh_tokens')")
        if cur.fetchone()[0] is None:
            pytest.skip("refresh_tokens table missing - apply db/postgres_schema.sql")
        cur.execute(
            "SELECT id, user_id, persona_slug FROM users "
            "WHERE is_deleted = FALSE ORDER BY created_at LIMIT 1")
        row = cur.fetchone()
        if row is None:
            pytest.skip("no active user row available")
        return {"id": row[0], "user_id": row[1], "persona_slug": row[2]}


def mint_access_token(user_db_id, seconds=900, username=None):
    # principal.username is read straight from the JWT "sub" claim, so a
    # token minted with a made-up name makes /session faithfully echo
    # that name back. Callers pass the real users.user_id.
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {"sub": username, "user_db_id": str(user_db_id),
         "exp": now + timedelta(seconds=seconds), "iat": now},
        Config.JWT_PRIVATE_KEY, algorithm=Config.JWT_ALGORITHM)


def backdate_consumed_at(pool, token_hash, seconds):
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE refresh_tokens SET consumed_at = NOW() - make_interval(secs => %s) "
            "WHERE token_hash = %s", (seconds, token_hash))
        conn.commit()


def backdate_family(pool, family_id, seconds):
    """Age every consumed row in a family, not just one token.

    The grace window is judged per token, from that token's own
    consumed_at. Backdating only the token that triggered detection
    leaves the *rotated* token looking freshly consumed, so a request
    presenting it is waved through as a benign two-tab race.
    """
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE refresh_tokens SET consumed_at = NOW() - make_interval(secs => %s) "
            "WHERE family_id = %s AND consumed = TRUE", (seconds, str(family_id)))
        conn.commit()


def rows_for_family(pool, family_id):
    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT token_hash, consumed FROM refresh_tokens WHERE family_id = %s",
            (str(family_id),))
        return cur.fetchall()


class BaseUrlSession(requests.Session):
    """``requests`` has no base-URL concept; browsers do.

    Every relative path in this suite would otherwise raise
    ``MissingSchema: No scheme supplied``. Resolving against the live
    server mirrors what a browser does with a relative URL, which is
    also the behaviour under test here.
    """

    def request(self, method, url, *args, **kwargs):
        if url.startswith("/"):
            url = LIVE_SERVER + url
        return super().request(method, url, *args, **kwargs)


def session_with_access(base_url, token):
    """A browser-like session carrying a valid access cookie."""
    client = BaseUrlSession()
    client.cookies.set(ACCESS_TOKEN_COOKIE, token, path="/", domain=_HOST)
    return client


# ── tests ──────────────────────────────────────────────────────
class LiveHttpSessionTestCase(unittest.TestCase):
    """Each test gets its own family; tearDown purges them from the DB."""

    def setUp(self) -> None:
        self.base_url = LIVE_SERVER
        self.pool = LIVE_POOL
        self.user = LIVE_USER
        self.svc = get_refresh_token_service()
        self._families = []
        self._sessions = []

    def _issue_refresh(self, grace_as=None):
        plaintext, record = self.svc.issue(
            user_db_id=self.user["id"], issued_ip="127.0.0.1",
            issued_user_agent="live-http-test")
        self._families.append(record.family_id)
        if grace_as is not None:
            backdate_consumed_at(self.pool, self.svc._hash(plaintext), grace_as)
        return plaintext, record

    def _track(self, client):
        self._sessions.append(client)
        return client

    def tearDown(self) -> None:
        for client in getattr(self, "_sessions", []):
            client.close()
        for family_id in getattr(self, "_families", []):
            with self.pool.acquire() as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM refresh_tokens WHERE family_id = %s",
                            (str(family_id),))
                conn.commit()

    # -- anonymous ------------------------------------------------
    def test_anonymous_session_is_401(self):
        r = requests.get(f"{self.base_url}/api/auth/session", timeout=10)
        self.assertEqual(401, r.status_code)

    def test_anonymous_session_clears_cookies(self):
        r = requests.get(f"{self.base_url}/api/auth/session", timeout=10)
        joined = "\n".join(r.raw.headers.getlist("Set-Cookie"))
        for name in (ACCESS_TOKEN_COOKIE, REFRESH_TOKEN_COOKIE, CSRF_TOKEN_COOKIE):
            self.assertIn(name, joined)
        self.assertIn("Max-Age=0", joined)

    # -- authenticated --------------------------------------------
    def test_valid_access_cookie_returns_the_real_user(self):
        client = self._track(session_with_access(
            self.base_url, mint_access_token(self.user["id"], username=self.user["user_id"])))
        r = client.get("/api/auth/session", timeout=10)
        self.assertEqual(200, r.status_code)
        body = r.json()
        self.assertTrue(body["authenticated"])
        self.assertFalse(body["refreshed"])
        self.assertEqual(self.user["user_id"], body["user"]["username"])
        self.assertEqual("no-store", r.headers.get("Cache-Control"))

    def test_valid_cookie_emits_no_new_cookies(self):
        client = self._track(session_with_access(
            self.base_url, mint_access_token(self.user["id"], username=self.user["user_id"])))
        r = client.get("/api/auth/session", timeout=10)
        self.assertEqual([], r.raw.headers.getlist("Set-Cookie"))

    def test_response_never_carries_a_bearer_token(self):
        client = self._track(session_with_access(
            self.base_url, mint_access_token(self.user["id"], username=self.user["user_id"])))
        body = client.get("/api/auth/session", timeout=10).json()
        self.assertNotIn("access_token", body)
        self.assertNotIn("token_type", body)

    # -- transparent refresh --------------------------------------
    def test_expired_access_cookie_refreshes_over_real_http(self):
        refresh, _ = self._issue_refresh()
        client = BaseUrlSession()
        self._track(client)
        client.cookies.set(
            ACCESS_TOKEN_COOKIE,
            mint_access_token(self.user["id"], seconds=-60,
                              username=self.user["user_id"]),
            path="/", domain=_HOST)
        client.cookies.set(REFRESH_TOKEN_COOKIE, refresh, path="/", domain=_HOST)

        r = client.get("/api/auth/session", timeout=10)
        self.assertEqual(200, r.status_code)
        self.assertTrue(r.json()["refreshed"])

    def test_rotation_consumes_the_old_row_in_the_database(self):
        refresh, record = self._issue_refresh()
        client = BaseUrlSession()
        self._track(client)
        client.cookies.set(REFRESH_TOKEN_COOKIE, refresh, path="/", domain=_HOST)
        client.get("/api/auth/session", timeout=10)

        rows = rows_for_family(self.pool, record.family_id)
        old = [row for row in rows if row[0] == self.svc._hash(refresh)]
        self.assertEqual(1, len(old))
        self.assertTrue(old[0][1], "the presented row must be consumed")

    def test_new_cookies_land_in_a_real_cookie_jar(self):
        refresh, _ = self._issue_refresh()
        client = BaseUrlSession()
        self._track(client)
        client.cookies.set(REFRESH_TOKEN_COOKIE, refresh, path="/", domain=_HOST)
        r = client.get("/api/auth/session", timeout=10)
        self.assertEqual(200, r.status_code)
        # The jar is the browser's contract: if these are absent the next
        # request would be anonymous.
        self.assertIsNotNone(client.cookies.get(ACCESS_TOKEN_COOKIE))
        self.assertIsNotNone(client.cookies.get(REFRESH_TOKEN_COOKIE))
        self.assertIsNotNone(client.cookies.get(CSRF_TOKEN_COOKIE))

    def test_refreshed_cookie_actually_authenticates_the_next_request(self):
        """The full silent-renewal loop over real HTTP."""
        refresh, _ = self._issue_refresh()
        client = BaseUrlSession()
        self._track(client)
        client.cookies.set(
            ACCESS_TOKEN_COOKIE,
            mint_access_token(self.user["id"], seconds=-60,
                              username=self.user["user_id"]),
            path="/", domain=_HOST)
        client.cookies.set(REFRESH_TOKEN_COOKIE, refresh, path="/", domain=_HOST)
        self.assertEqual(200, client.get("/api/auth/session", timeout=10).status_code)
        # Second call must use the NEW access cookie and succeed with no
        # further rotation.
        second = client.get("/api/auth/session", timeout=10)
        self.assertEqual(200, second.status_code)
        self.assertFalse(second.json()["refreshed"])

    def test_bad_refresh_cookie_is_401(self):
        client = BaseUrlSession()
        self._track(client)
        client.cookies.set(REFRESH_TOKEN_COOKIE, "not-a-real-token", path="/", domain=_HOST)
        self.assertEqual(401, client.get("/api/auth/session", timeout=10).status_code)

    # -- reuse detection ------------------------------------------
    def test_replay_outside_grace_revokes_the_family(self):
        refresh, record = self._issue_refresh()
        client = BaseUrlSession()
        self._track(client)
        client.cookies.set(REFRESH_TOKEN_COOKIE, refresh, path="/", domain=_HOST)
        self.assertEqual(200, client.get("/api/auth/session", timeout=10).status_code)

        # Age the consumption, then replay the stolen original.
        backdate_consumed_at(self.pool, self.svc._hash(refresh), 300)
        attacker = BaseUrlSession()
        self._track(attacker)
        attacker.cookies.set(REFRESH_TOKEN_COOKIE, refresh, path="/", domain=_HOST)
        self.assertEqual(401, attacker.get("/api/auth/session", timeout=10).status_code)

        # The whole family is revoked now. Age every row in it, so the
        # victim's replay below is judged outside the 30s grace window
        # rather than being waved through as a two-tab race.
        backdate_family(self.pool, record.family_id, 300)

        # The legitimate holder's ROTATED REFRESH token is now dead too.
        # Their access JWT is deliberately left expired here: a still-valid
        # access token short-circuits /session before the refresh path is
        # ever consulted (see the test below).
        victim = BaseUrlSession()
        self._track(victim)
        for name, value in client.cookies.items():
            victim.cookies.set(name, value, path="/", domain=_HOST)
        victim.cookies.set(
            ACCESS_TOKEN_COOKIE,
            mint_access_token(self.user["id"], seconds=-60,
                              username=self.user["user_id"]),
            path="/", domain=_HOST)
        self.assertEqual(401, victim.get("/api/auth/session", timeout=10).status_code)

    def test_family_revocation_does_not_kill_a_live_access_jwt(self):
        """Documents the accepted trade-off, not a bug.

        A signed access JWT cannot be revoked without a server-side
        blacklist, so reuse detection only kills the refresh chain. A
        stolen access token therefore keeps working until it expires.
        The 15-minute JWT TTL is the mitigation; this test exists so that
        shortening or removing that TTL is a deliberate decision rather
        than an accident nobody notices.
        """
        refresh, _ = self._issue_refresh()
        client = BaseUrlSession()
        self._track(client)
        client.cookies.set(REFRESH_TOKEN_COOKIE, refresh, path="/", domain=_HOST)
        self.assertEqual(200, client.get("/api/auth/session", timeout=10).status_code)

        backdate_consumed_at(self.pool, self.svc._hash(refresh), 300)
        attacker = BaseUrlSession()
        self._track(attacker)
        attacker.cookies.set(REFRESH_TOKEN_COOKIE, refresh, path="/", domain=_HOST)
        self.assertEqual(401, attacker.get("/api/auth/session", timeout=10).status_code)

        # Refresh chain is dead, but this holder still has a live access
        # cookie from the first call - /session answers from it alone.
        holder = BaseUrlSession()
        self._track(holder)
        for name, value in client.cookies.items():
            holder.cookies.set(name, value, path="/", domain=_HOST)
        r = holder.get("/api/auth/session", timeout=10)
        self.assertEqual(200, r.status_code)
        self.assertFalse(r.json()["refreshed"])

    # -- logout ----------------------------------------------------
    def test_logout_without_csrf_is_403(self):
        refresh, _ = self._issue_refresh()
        client = BaseUrlSession()
        self._track(client)
        client.cookies.set(REFRESH_TOKEN_COOKIE, refresh, path="/", domain=_HOST)
        self.assertEqual(403, client.post("/api/auth/logout", timeout=10).status_code)

    def test_logout_with_csrf_is_204(self):
        refresh, _ = self._issue_refresh()
        client = BaseUrlSession()
        self._track(client)
        client.cookies.set(REFRESH_TOKEN_COOKIE, refresh, path="/", domain=_HOST)
        first = client.get("/api/auth/session", timeout=10)
        csrf = first.json()["csrf_token"]
        client.cookies.set(CSRF_TOKEN_COOKIE, csrf, path="/", domain=_HOST)
        r = client.post("/api/auth/logout", timeout=10,
                        headers={CSRF_HEADER: csrf})
        self.assertEqual(204, r.status_code)

    def test_logout_purges_the_refresh_rows_from_the_database(self):
        refresh, record = self._issue_refresh()
        client = BaseUrlSession()
        self._track(client)
        client.cookies.set(REFRESH_TOKEN_COOKIE, refresh, path="/", domain=_HOST)
        body = client.get("/api/auth/session", timeout=10).json()
        csrf = body["csrf_token"]
        client.cookies.set(CSRF_TOKEN_COOKIE, csrf, path="/", domain=_HOST)
        client.post("/api/auth/logout", timeout=10, headers={CSRF_HEADER: csrf})
        self.assertEqual([], rows_for_family(self.pool, record.family_id),
                         "logout must DELETE the family, not just mark it")

    def test_logout_is_idempotent(self):
        client = BaseUrlSession()
        self._track(client)
        self.assertEqual(204, client.post("/api/auth/logout", timeout=10).status_code)
        self.assertEqual(204, client.post("/api/auth/logout", timeout=10).status_code)

    def test_session_after_logout_cannot_be_resumed(self):
        refresh, _ = self._issue_refresh()
        client = BaseUrlSession()
        self._track(client)
        client.cookies.set(REFRESH_TOKEN_COOKIE, refresh, path="/", domain=_HOST)
        body = client.get("/api/auth/session", timeout=10).json()
        csrf = body["csrf_token"]
        client.cookies.set(CSRF_TOKEN_COOKIE, csrf, path="/", domain=_HOST)
        client.post("/api/auth/logout", timeout=10, headers={CSRF_HEADER: csrf})

        replay = BaseUrlSession()
        self._track(replay)
        replay.cookies.set(REFRESH_TOKEN_COOKIE, refresh, path="/", domain=_HOST)
        self.assertEqual(401, replay.get("/api/auth/session", timeout=10).status_code)

    # -- CSRF on a real protected write ---------------------------
    def test_protected_write_without_csrf_is_403(self):
        client = self._track(session_with_access(
            self.base_url, mint_access_token(self.user["id"], username=self.user["user_id"])))
        r = client.put("/api/auth/me/persona", json={"slug": None}, timeout=10)
        self.assertEqual(403, r.status_code)

    def test_protected_write_with_csrf_succeeds_and_restores_persona(self):
        original = self.user["persona_slug"]
        client = self._track(session_with_access(
            self.base_url, mint_access_token(self.user["id"], username=self.user["user_id"])))
        session = client.get("/api/auth/session", timeout=10).json()
        csrf = session["csrf_token"]
        client.cookies.set(CSRF_TOKEN_COOKIE, csrf, path="/", domain=_HOST)
        try:
            r = client.put("/api/auth/me/persona", json={"slug": None},
                           timeout=10, headers={CSRF_HEADER: csrf})
            self.assertEqual(204, r.status_code)
        finally:
            client.put("/api/auth/me/persona", json={"slug": original},
                       timeout=10, headers={CSRF_HEADER: csrf})

    def test_protected_write_with_wrong_csrf_is_403(self):
        client = self._track(session_with_access(
            self.base_url, mint_access_token(self.user["id"], username=self.user["user_id"])))
        r = client.put("/api/auth/me/persona", json={"slug": None}, timeout=10,
                       headers={CSRF_HEADER: "wrong"})
        self.assertEqual(403, r.status_code)

    def test_unauthenticated_write_is_401_not_403(self):
        """A 403 would confirm the cookie was valid to an attacker."""
        r = requests.put(f"{self.base_url}/api/auth/me/persona",
                         json={"slug": None}, timeout=10,
                         headers={CSRF_HEADER: "anything"})
        self.assertEqual(401, r.status_code)

    # -- removed endpoint -------------------------------------------
    def test_old_me_endpoint_returns_404(self):
        client = self._track(session_with_access(
            self.base_url, mint_access_token(self.user["id"], username=self.user["user_id"])))
        self.assertEqual(404, client.get("/api/auth/me", timeout=10).status_code)


# Module-scope wiring so unittest.TestCase can see the pytest fixtures.
LIVE_SERVER = None
LIVE_POOL = None
LIVE_USER = None


def pytest_configure():
    pass


def _inject(request):
    global LIVE_SERVER, LIVE_POOL, LIVE_USER
    LIVE_SERVER = request.getfixturevalue("live_server")
    LIVE_POOL = request.getfixturevalue("pool")
    LIVE_USER = request.getfixturevalue("real_user")


@pytest.fixture(autouse=True)
def _bind(request):
    _inject(request)
    yield
