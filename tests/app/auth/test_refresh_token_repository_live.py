"""LIVE database tests for PostgresRefreshTokenRepository.

This is the only suite that exercises real SQL. It validates the two design
decisions that were previously only argued on paper:

  1. consume() is ATOMIC - two racing calls must yield exactly one True.
     Postgres serialises "UPDATE ... WHERE consumed = FALSE RETURNING", so
     the loser's WHERE simply stops matching.
  2. get_by_hash() returns CONSUMED rows. Reuse detection depends on a
     replayed token coming back as a row rather than as None.

Every row is removed in tearDown, so the suite leaves no residue.

Auto-skips when Postgres is unreachable, so CI without a database still
passes. Force with:  RUN_LIVE_DB_TESTS=1 pytest <file>

Run with:  pytest tests/app/auth/test_refresh_token_repository_live.py
"""
from __future__ import annotations

import os
import unittest
import uuid
from datetime import datetime, timedelta, timezone

from app.auth.session.refresh_token_service import (
    RefreshTokenInvalidError,
    RefreshTokenReuseError,
    RefreshTokenService,
)
from app.auth.session.repository.postgres_refresh_token_repository import (
    PostgresRefreshTokenRepository,
)

SKIP_REASON = "set RUN_LIVE_DB_TESTS=1 (and a reachable Postgres) to run"


def _connect():
    if os.getenv("RUN_LIVE_DB_TESTS") != "1":
        raise unittest.SkipTest(SKIP_REASON)
    from db.postgres_client import get_postgres_client
    return get_postgres_client().get_pool()


class LiveRefreshTokenRepositoryTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.pool = _connect()
        with cls.pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute("SELECT to_regclass('public.refresh_tokens')")
            if cur.fetchone()[0] is None:
                raise unittest.SkipTest(
                    "refresh_tokens table missing - apply db/postgres_schema.sql")

            # refresh_tokens.user_db_id is a real FOREIGN KEY to users(id),
            # so the suite cannot invent UUIDs the way the mocked tests do -
            # Postgres would reject the INSERT with ForeignKeyViolation.
            # Borrow an existing active user's id instead. The app itself
            # always does exactly this: look the user up, use the id it
            # returns.
            cur.execute("SELECT id FROM users WHERE is_deleted = FALSE LIMIT 1")
            row = cur.fetchone()
            if row is None:
                raise unittest.SkipTest(
                    "no active user row available to satisfy the FK")
            cls.real_user_id = row[0]

    def setUp(self) -> None:
        self.repo = PostgresRefreshTokenRepository(db_pool=self.pool)
        self.svc = RefreshTokenService(repo=self.repo)
        self.user_id = self.real_user_id
        self._issued_families: list[uuid.UUID] = []
        self._issued_plaintexts: list[str] = []

    def tearDown(self) -> None:
        if not hasattr(self, "repo"):
            return
        for family_id in self._issued_families:
            with self.pool.acquire() as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM refresh_tokens WHERE family_id = %s",
                            (str(family_id),))
                conn.commit()

    def _issue(self):
        plaintext, record = self.svc.issue(
            user_db_id=self.user_id, issued_ip="10.0.0.9",
            issued_user_agent="live-test-agent")
        self._issued_families.append(record.family_id)
        self._issued_plaintexts.append(plaintext)
        return plaintext, record

    def _backdate_consumed_at(self, token, seconds):
        """Push consumed_at into the past, beyond the reuse grace window.

        Without this a replay milliseconds after consumption lands INSIDE
        the grace window and is - correctly - treated as a benign concurrent
        rotation rather than theft.
        """
        with self.pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE refresh_tokens SET consumed_at = NOW() - make_interval(secs => %s) "
                "WHERE token_hash = %s", (seconds, self.svc._hash(token)))
            conn.commit()

    # -- schema shape ---------------------------------------------
    def test_table_and_indexes_exist(self):
        with self.pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute("SELECT indexname FROM pg_indexes "
                        "WHERE tablename = 'refresh_tokens'")
            names = {r[0] for r in cur.fetchall()}
        self.assertIn("idx_refresh_tokens_user_active", names)
        self.assertIn("idx_refresh_tokens_family", names)

    # -- column mapping (the silent-corruption risk) ---------------
    def test_row_maps_to_the_right_fields(self):
        """_row_to_record maps by tuple index. A column-order mismatch
        would not raise - it would silently swap family_id and issued_ip."""
        plaintext, record = self._issue()
        row = self.repo.get_by_hash(self.svc._hash(plaintext))
        self.assertEqual(record.id, row.id)
        self.assertEqual(self.user_id, row.user_db_id)
        self.assertEqual(record.family_id, row.family_id)
        self.assertEqual("10.0.0.9", str(row.issued_ip))
        self.assertEqual("live-test-agent", row.issued_user_agent)
        self.assertIsInstance(row.consumed_at, type(None))
        self.assertIsInstance(row.created_at, datetime)
        self.assertIsInstance(row.expires_at, datetime)
        self.assertTrue(row.expires_at > datetime.now(timezone.utc))

    def test_only_the_hash_is_stored(self):
        plaintext, _ = self._issue()
        with self.pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute("SELECT token_hash FROM refresh_tokens "
                        "WHERE user_db_id = %s", (str(self.user_id),))
            for (stored,) in cur.fetchall():
                self.assertEqual(64, len(stored))
                self.assertNotIn(plaintext, stored)

    # -- atomicity: the claim the whole race design rests on -------
    def test_consume_is_idempotent_and_returns_true_only_once(self):
        plaintext, record = self._issue()
        self.assertTrue(self.repo.consume(record.id))
        self.assertFalse(self.repo.consume(record.id))
        self.assertFalse(self.repo.consume(record.id))

    def test_concurrent_consume_yields_exactly_one_winner(self):
        """Two connections racing the same row. Postgres serialises them;
        exactly one may flip it."""
        import threading
        plaintext, record = self._issue()
        results = []
        barrier = threading.Barrier(2)

        def attempt():
            barrier.wait()
            results.append(self.repo.consume(record.id))

        threads = [threading.Thread(target=attempt) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(1, results.count(True), f"expected one winner, got {results}")
        self.assertEqual(1, results.count(False))

    # -- reuse detection depends on this ---------------------------
    def test_get_by_hash_returns_consumed_rows(self):
        plaintext, record = self._issue()
        self.assertFalse(self.repo.get_by_hash(self.svc._hash(plaintext)).consumed)
        self.repo.consume(record.id)
        found = self.repo.get_by_hash(self.svc._hash(plaintext))
        self.assertIsNotNone(found, "consumed rows MUST remain visible")
        self.assertTrue(found.consumed)
        self.assertIsNotNone(found.consumed_at)

    def test_get_by_hash_returns_expired_rows(self):
        plaintext, record = self._issue()
        with self.pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE refresh_tokens SET expires_at = NOW() - INTERVAL '1 day' WHERE id = %s", (str(record.id),))
            conn.commit()
        self.assertIsNotNone(self.repo.get_by_hash(self.svc._hash(plaintext)))

    def test_unknown_hash_returns_none(self):
        self.assertIsNone(self.repo.get_by_hash("0" * 64))

    # -- revocation ------------------------------------------------
    def test_revoke_family_marks_consumed_and_counts(self):
        first, first_record = self._issue()
        second, _ = self.svc.rotate(plaintext_token=first, issued_ip="10.0.0.9",
                                    issued_user_agent="t")
        self.assertEqual(1, self.repo.revoke_family(first_record.family_id))
        self.assertTrue(self.repo.get_by_hash(self.svc._hash(second)).consumed)
        self.assertEqual(0, self.repo.revoke_family(first_record.family_id))

    def test_revoke_family_leaves_other_families_alone(self):
        a_plaintext, a_record = self._issue()
        b_plaintext, b_record = self._issue()
        self.repo.revoke_family(a_record.family_id)
        self.assertTrue(self.repo.get_by_hash(self.svc._hash(a_plaintext)).consumed)
        untouched = self.repo.get_by_hash(self.svc._hash(b_plaintext))
        self.assertIsNotNone(untouched)
        self.assertFalse(untouched.consumed)
        self.assertEqual(b_record.family_id, untouched.family_id)

    def test_purge_family_deletes_rows(self):
        first, record = self._issue()
        second, _ = self.svc.rotate(plaintext_token=first, issued_ip="10.0.0.9",
                                    issued_user_agent="t")
        self.assertEqual(2, self.repo.purge_family(record.family_id))
        self.assertIsNone(self.repo.get_by_hash(self.svc._hash(first)))
        self.assertIsNone(self.repo.get_by_hash(self.svc._hash(second)))

    def test_delete_expired_only_removes_expired(self):
        plaintext, record = self._issue()
        with self.pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE refresh_tokens SET expires_at = NOW() - INTERVAL '1 day' WHERE id = %s", (str(record.id),))
            conn.commit()
        before = self.repo.delete_expired()
        self.assertGreaterEqual(before, 1)
        fresh, _ = self._issue()
        self.assertIsNotNone(self.repo.get_by_hash(self.svc._hash(fresh)))

    # -- end-to-end through the real SQL --------------------------
    def test_full_rotation_chain_against_real_sql(self):
        first, record = self._issue()
        second, second_record = self.svc.rotate(
            plaintext_token=first, issued_ip="10.0.0.9", issued_user_agent="t")
        self.assertEqual(record.family_id, second_record.family_id)
        third, third_record = self.svc.rotate(
            plaintext_token=second, issued_ip="10.0.0.9", issued_user_agent="t")
        self.assertEqual(record.family_id, third_record.family_id)
        self._backdate_consumed_at(first, seconds=120)
        with self.assertRaises(RefreshTokenReuseError):
            self.svc.rotate(plaintext_token=first, issued_ip="10.0.0.9", issued_user_agent="t")
        with self.assertRaises(RefreshTokenInvalidError):
            self.svc.rotate(plaintext_token="nope", issued_ip="10.0.0.9", issued_user_agent="t")
        self.assertIsNotNone(third)

    def test_logout_purges_so_the_token_cannot_return(self):
        first, record = self._issue()
        second, _ = self.svc.rotate(plaintext_token=first, issued_ip="10.0.0.9",
                                    issued_user_agent="t")
        self.assertTrue(self.svc.revoke(first))
        for dead in (first, second):
            self.assertIsNone(self.repo.get_by_hash(self.svc._hash(dead)))
            with self.assertRaises(RefreshTokenInvalidError):
                self.svc.rotate(plaintext_token=dead, issued_ip="10.0.0.9", issued_user_agent="t")

    def test_user_id_has_a_cascade_delete_foreign_key(self):
        """Asserted from the catalog rather than by creating and deleting a
        user row, which would need a department fixture to satisfy the FK."""
        with self.pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT confdeltype FROM pg_constraint "
                "WHERE conrelid = 'refresh_tokens'::regclass AND contype = 'f'")
            rows = cur.fetchall()
        self.assertEqual(1, len(rows), "expected exactly one foreign key")
        self.assertEqual("c", rows[0][0], "expected ON DELETE CASCADE")

    def test_replay_inside_grace_window_is_a_benign_rotation(self):
        """The other half of the grace-window contract: a token replayed
        moments after consumption yields a sibling, not an alarm. Guards
        against 'fixing' the grace window away and logging users out when
        two tabs refresh at once."""
        first, record = self._issue()
        self.svc.rotate(plaintext_token=first, issued_ip="10.0.0.9",
                        issued_user_agent="t")
        sibling, sibling_record = self.svc.rotate(
            plaintext_token=first, issued_ip="10.0.0.9", issued_user_agent="t")
        self.assertEqual(record.family_id, sibling_record.family_id)
        self.assertFalse(sibling_record.consumed)
        self.assertIsNotNone(self.repo.get_by_hash(self.svc._hash(sibling)))

if __name__ == "__main__":
    unittest.main()
