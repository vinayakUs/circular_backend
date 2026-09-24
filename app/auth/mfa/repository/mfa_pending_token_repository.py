"""MfaPendingTokenRepository — ABC + dataclass for mfa_pending_tokens."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(slots=True, frozen=True)
class MfaPendingToken:
    token_id: UUID
    token_hash: str
    user_db_id: UUID
    issued_ip: str | None
    issued_user_agent: str | None
    consumed: bool
    created_at: datetime
    expires_at: datetime


class MfaPendingTokenRepository(ABC):
    """Persistence boundary for the mfa_pending_tokens table."""

    @abstractmethod
    def create(
        self,
        *,
        user_db_id: UUID,
        token_hash: str,
        issued_ip: str | None,
        issued_user_agent: str | None,
        ttl_seconds: int,
    ) -> MfaPendingToken:
        """Insert a new pending token. Caller invalidates prior tokens first."""
        ...

    @abstractmethod
    def get_active_by_hash(self, token_hash: str) -> MfaPendingToken | None:
        """Look up by SHA-256 hash of the plaintext token. Returns None if
        the token doesn't exist, is expired, or is already consumed."""
        ...

    @abstractmethod
    def consume(self, token_id: UUID) -> bool:
        """Atomically mark the token as consumed. Returns True iff this
        call did the flip (not a duplicate consume)."""
        ...

    @abstractmethod
    def consume_prior_for_user(self, user_db_id: UUID) -> int:
        """Mark all unconsumed pending tokens for this user as consumed.
        Called from mint() to enforce one-active-per-user."""
        ...
