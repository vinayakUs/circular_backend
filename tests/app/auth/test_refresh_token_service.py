"""Unit tests for RefreshTokenService — the rotation state machine.

No database. The repository is an in-memory fake so every branch of
rotate() can be driven deterministically, including the two that are hard
to reproduce by hand:

  * losing the atomic consume() race
  * replaying a consumed token inside vs. outside the grace window

The SQL behind those branches is covered separately by
test_refresh_token_repository_live.py, which runs against a real database.

Run with:  pytest tests/app/auth/test_refresh_token_service.py
"""
from __future__ import annotations

import unittest
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from app.auth.session.refresh_token_service import (
    RefreshTokenInvalidError,
    RefreshTokenReuseError,
    RefreshTokenService,
)
from app.auth.session.repository.refresh_token_repository import RefreshToken


class FakeRefreshTokenRepository:
    """In-memory stand-in with the same atomicity contract as the SQL."""

    def __init__(self) -> None:
        self.rows: dict[str, RefreshToken] = {}
        self.consume_should_lose = False

    def _put(self, record):
        self.rows[record.token_hash] = record

    def create(self, *, user_db_id, token_hash, family_id, issued_ip,
               issued_user_agent, ttl_seconds):
        record = RefreshToken(
            id=uuid.uuid4(),
            user_db_id=user_db_id,
            token_hash=token_hash,
            family_id=family_id,
            issued_ip=issued_ip,
            issued_user_agent=issued_user_agent,
            consumed=False,
            consumed_at=None,
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds),
        )
        self._put(record)
        return record

    def get_by_hash(self, token_hash):
        # Deliberately unfiltered, mirroring the real repo: consumed rows
        # MUST come back or reuse detection cannot work.
        return self.rows.get(token_hash)

    def consume(self, token_id):
        if self.consume_should_lose:
            return False
        for record in list(self.rows.values()):
            if record.id == token_id and not record.consumed:
                self._put(replace(record, consumed=True,
                                  consumed_at=datetime.now(timezone.utc)))
                return True
        return False

    def _revoke_where(self, predicate) -> int:
        revoked = 0
        for record in list(self.rows.values()):
            if predicate(record) and not record.consumed:
                self._put(replace(record, consumed=True,
                                  consumed_at=datetime.now(timezone.utc)))
                revoked += 1
        return revoked

    def revoke_family(self, family_id):
        return self._revoke_where(lambda r: r.family_id == family_id)

    def revoke_all_for_user(self, user_db_id):
        return self._revoke_where(lambda r: r.user_db_id == user_db_id)

    def purge_family(self, family_id):
        purged = 0
        for record in list(self.rows.values()):
            if record.family_id == family_id:
                del self.rows[record.token_hash]
                purged += 1
        return purged

    def delete_expired(self):
        return 0

    def live_count(self) -> int:
        return len([r for r in self.rows.values() if not r.consumed])


class RefreshTokenServiceTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.repo = FakeRefreshTokenRepository()
        self.svc = RefreshTokenService(repo=self.repo)
        self.user_id = uuid.uuid4()
        self.ip = "10.0.0.1"
        self.ua = "pytest-agent"

    def _issue(self):
        return self.svc.issue(user_db_id=self.user_id, issued_ip=self.ip,
                              issued_user_agent=self.ua)

    def _rotate(self, token):
        return self.svc.rotate(plaintext_token=token, issued_ip=self.ip,
                               issued_user_agent=self.ua)

    @staticmethod
    def _hash(token):
        return RefreshTokenService._hash(token)

    def _age_consumed_at(self, token, seconds):
        key = self._hash(token)
        self.repo.rows[key] = replace(
            self.repo.rows[key],
            consumed_at=datetime.now(timezone.utc) - timedelta(seconds=seconds),
        )

    # -- issue ----------------------------------------------------
    def test_issue_returns_usable_plaintext_and_stores_only_its_hash(self):
        plaintext, record = self._issue()
        self.assertEqual(43, len(plaintext))
        stored = self.repo.rows[self._hash(plaintext)]
        self.assertEqual(self._hash(plaintext), stored.token_hash)
        self.assertNotIn(plaintext, stored.token_hash)
        self.assertFalse(stored.consumed)
        self.assertEqual(self.user_id, record.user_db_id)

    def test_each_login_starts_a_new_family(self):
        _, first = self._issue()
        _, second = self._issue()
        self.assertNotEqual(first.family_id, second.family_id)

    # -- rotate: happy path ---------------------------------------
    def test_rotate_consumes_the_old_token_and_keeps_the_family(self):
        first, first_record = self._issue()
        second, second_record = self._rotate(first)
        self.assertNotEqual(first, second)
        self.assertEqual(first_record.family_id, second_record.family_id)
        self.assertTrue(self.repo.rows[self._hash(first)].consumed)

    def test_rotated_token_is_usable_for_the_next_rotation(self):
        first, _ = self._issue()
        second, _ = self._rotate(first)
        third, record = self._rotate(second)
        self.assertNotEqual(second, third)
        self.assertFalse(self.repo.rows[self._hash(third)].consumed)
        self.assertIsNotNone(record)

    # -- rotate: invalid ------------------------------------------
    def test_unknown_token_is_invalid_without_alarm(self):
        with self.assertRaises(RefreshTokenInvalidError):
            self._rotate("not-a-real-token")

    def test_empty_token_is_invalid(self):
        with self.assertRaises(RefreshTokenInvalidError):
            self._rotate("")

    def test_expired_token_is_invalid_not_reuse(self):
        token, _ = self._issue()
        key = self._hash(token)
        self.repo.rows[key] = replace(
            self.repo.rows[key],
            expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        with self.assertRaises(RefreshTokenInvalidError):
            self._rotate(token)

    def test_expired_and_consumed_token_reports_reuse_not_invalid(self):
        """Ordering matters: consumed is checked first, so an old replayed
        token is an alarm even when it has also expired."""
        token, _ = self._issue()
        key = self._hash(token)
        self.repo.rows[key] = replace(
            self.repo.rows[key],
            consumed=True,
            consumed_at=datetime.now(timezone.utc) - timedelta(seconds=10_000),
            expires_at=datetime.now(timezone.utc) - timedelta(seconds=5_000))
        with self.assertRaises(RefreshTokenReuseError):
            self._rotate(token)

    # -- rotate: concurrent rotation ------------------------------
    def test_losing_the_consume_race_mints_nothing(self):
        """The loser's client must not mint, or the family branches and its
        own next request then looks like a replay."""
        token, _ = self._issue()
        self.repo.consume_should_lose = True
        with self.assertRaises(RefreshTokenInvalidError):
            self._rotate(token)
        self.assertEqual(1, len(self.repo.rows), "no successor should exist")
        # The original token is untouched: consume() only LIED about losing,
        # so nothing was actually flipped.
        self.assertEqual(1, self.repo.live_count())

    def test_second_tab_retry_within_grace_recovers_silently(self):
        first, first_record = self._issue()
        self._rotate(first)
        second, second_record = self._rotate(first)
        self.assertEqual(first_record.family_id, second_record.family_id)
        self.assertFalse(second_record.consumed)
        # Two live tokens: tab 1's successor and tab 2's grace-branch
        # sibling. They share a family but each is presented only once.
        self.assertEqual(2, self.repo.live_count())

    def test_consumed_at_in_the_future_is_not_graced(self):
        """Defensive: a clock-skewed future timestamp must not open the
        window indefinitely."""
        token, _ = self._issue()
        self._rotate(token)
        key = self._hash(token)
        self.repo.rows[key] = replace(
            self.repo.rows[key],
            consumed_at=datetime.now(timezone.utc) + timedelta(seconds=5))
        with self.assertRaises(RefreshTokenReuseError):
            self._rotate(token)

    def test_consumed_without_consumed_at_fails_closed(self):
        token, _ = self._issue()
        key = self._hash(token)
        self.repo.rows[key] = replace(self.repo.rows[key], consumed=True,
                                      consumed_at=None)
        with self.assertRaises(RefreshTokenReuseError):
            self._rotate(token)

    # -- rotate: reuse detection ----------------------------------
    def test_replay_outside_grace_revokes_the_whole_family(self):
        token, record = self._issue()
        live_a, _ = self._rotate(token)
        live_b, _ = self._rotate(live_a)
        self._age_consumed_at(token, seconds=120)
        with self.assertRaises(RefreshTokenReuseError):
            self._rotate(token)
        for candidate in (token, live_a, live_b):
            self.assertTrue(self.repo.rows[self._hash(candidate)].consumed)
        self.assertEqual(0, self.repo.live_count())
        self.assertEqual(record.family_id,
                         self.repo.rows[self._hash(live_b)].family_id)

    def test_replay_does_not_touch_another_family(self):
        stolen, _ = self._issue()
        self._rotate(stolen)
        self._age_consumed_at(stolen, seconds=120)
        unrelated, unrelated_record = self._issue()
        with self.assertRaises(RefreshTokenReuseError):
            self._rotate(stolen)
        self.assertFalse(self.repo.rows[self._hash(unrelated)].consumed)
        self.assertNotEqual(unrelated_record.family_id,
                            self.repo.rows[self._hash(stolen)].family_id)

    # -- revoke ---------------------------------------------------
    def test_revoke_kills_the_whole_family(self):
        token, _ = self._issue()
        successor, _ = self._rotate(token)
        self.assertTrue(self.svc.revoke(token))
        self.assertIsNone(self.repo.rows.get(self._hash(successor)))

    def test_revoke_unknown_or_empty_is_false_and_harmless(self):
        self.assertFalse(self.svc.revoke("nope"))
        self.assertFalse(self.svc.revoke(None))
        self.assertFalse(self.svc.revoke(""))

    def test_revoked_token_can_no_longer_rotate(self):
        token, _ = self._issue()
        self.svc.revoke(token)
        with self.assertRaises(RefreshTokenInvalidError):
            self._rotate(token)

    def test_logout_is_final_even_inside_the_grace_window(self):
        """Regression: revoke() used to mark rows consumed, which put a
        logged-out token inside the reuse grace window - where it was
        silently re-issued and the session survived logout."""
        token, _ = self._issue()
        successor, _ = self._rotate(token)
        self.svc.revoke(token)
        for dead in (token, successor):
            self.assertIsNone(self.repo.rows.get(self._hash(dead)))
            with self.assertRaises(RefreshTokenInvalidError):
                self._rotate(dead)

    def test_revoke_purges_rather_than_consuming(self):
        token, record = self._issue()
        self.svc.revoke(token)
        self.assertEqual(0, len([r for r in self.repo.rows.values()
                                 if r.family_id == record.family_id]))

    def test_revoke_all_for_user_spans_every_family(self):
        first, _ = self._issue()
        second, _ = self._issue()
        self.assertEqual(2, self.svc.revoke_all_for_user(self.user_id))
        self.assertTrue(self.repo.rows[self._hash(first)].consumed)
        self.assertTrue(self.repo.rows[self._hash(second)].consumed)


if __name__ == "__main__":
    unittest.main()
