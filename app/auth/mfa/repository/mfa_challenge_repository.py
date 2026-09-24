"""MfaChallengeRepository — ABC + dataclass for the mfa_challenges table.

Mirrors the pattern used by the captcha repository (ABC + dataclass in
one file; Postgres impl in a sibling file).
"""
from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


class ConsumeResult(enum.Enum):
    """Outcome of an attempt to verify an MFA challenge."""

    MATCH = "match"           # code matched; challenge is now consumed
    MISMATCH = "mismatch"     # wrong code; attempts incremented
    LOCKED = "locked"         # attempts reached max; challenge locked
    EXPIRED = "expired"       # challenge past expires_at
    CONSUMED = "consumed"     # challenge was already consumed


@dataclass(slots=True, frozen=True)
class MfaChallenge:
    id: UUID
    user_db_id: UUID
    channel: str
    code_hash: str
    attempts: int
    consumed: bool
    created_at: datetime
    expires_at: datetime


class MfaChallengeRepository(ABC):
    """Persistence boundary for the mfa_challenges table."""

    @abstractmethod
    def create(
        self,
        *,
        user_db_id: UUID,
        channel: str,
        code_hash: str,
        ttl_seconds: int,
    ) -> MfaChallenge:
        """Insert a new challenge. Caller has already validated inputs."""
        ...

    @abstractmethod
    def get_active(self, user_db_id: UUID) -> MfaChallenge | None:
        """Return the user's single active challenge (unconsumed, not
        expired), or None if no active challenge exists."""
        ...

    @abstractmethod
    def consume_if_match_and_under_attempts(
        self,
        *,
        challenge_id: UUID,
        candidate_hash: str,
        max_attempts: int,
    ) -> ConsumeResult:
        """ATOMIC compare-and-swap.

        Two concurrent verify calls cannot both win — the UPDATE acquires
        a row-level lock. Returns one of:
          MATCH     code matched; challenge consumed
          MISMATCH  wrong code; attempts incremented (still under max)
          LOCKED    attempts hit max; challenge now consumed
          EXPIRED   challenge past expires_at (or missing)
          CONSUMED  challenge was already consumed
        """
        ...

    @abstractmethod
    def consume_prior_for_user(self, user_db_id: UUID) -> int:
        """One-active-per-user invariant. Mark all unconsumed challenges
        for this user as consumed=True. Returns count affected."""
        ...
