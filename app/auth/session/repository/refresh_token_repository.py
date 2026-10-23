"""RefreshTokenRepository — ABC + dataclass for refresh_tokens."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(slots=True, frozen=True)
class RefreshToken:
    """One row of the refresh_tokens table.

    ``family_id`` groups a rotation chain, so reuse detection can kill a
    stolen token and the real session together.
    """

    id: UUID
    user_db_id: UUID
    token_hash: str
    family_id: UUID
    issued_ip: str | None
    issued_user_agent: str | None
    consumed: bool
    consumed_at: datetime | None
    created_at: datetime
    expires_at: datetime


class RefreshTokenRepository(ABC):
    """Persistence boundary for the refresh_tokens table."""

    @abstractmethod
    def create(
        self,
        *,
        user_db_id: UUID,
        token_hash: str,
        family_id: UUID,
        issued_ip: str | None,
        issued_user_agent: str | None,
        ttl_seconds: int,
    ) -> RefreshToken:
        """Insert a new refresh token row with expires_at = now + ttl."""
        ...

    @abstractmethod
    def get_by_hash(self, token_hash: str) -> RefreshToken | None:
        """Look up by SHA-256 hash. None only if no such row exists.

        Deliberately does NOT filter on ``consumed`` or ``expires_at``: a
        replayed token must come back as a row, because "already consumed"
        is the theft signal the service turns into a family-wide revocation.
        At most one row — ``token_hash`` is UNIQUE.
        """
        ...

    @abstractmethod
    def consume(self, token_id: UUID) -> bool:
        """Atomically mark a token consumed. True iff THIS call did the flip.

        False means a concurrent request got there first; the caller then
        refuses to mint, which stops two parallel refreshes from both
        succeeding.
        """
        ...

    @abstractmethod
    def revoke_family(self, family_id: UUID) -> int:
        """Consume every unconsumed token in one rotation chain.

        Returns rows revoked. Used on replay, where the legitimate client's
        token was already rotated out and a second presentation means a
        copy leaked.
        """
        ...

    @abstractmethod
    def revoke_all_for_user(self, user_db_id: UUID) -> int:
        """Consume every unconsumed token across all families for one user.

        Returns rows revoked.
        """
        ...

    @abstractmethod
    def purge_family(self, family_id: UUID) -> int:
        """DELETE every row in a rotation chain. Returns rows removed.

        Distinct from revoke_family(): a just-consumed row is
        indistinguishable from a just-rotated one and so falls inside the
        reuse grace window, where it would still be honoured.
        """
        ...

    @abstractmethod
    def delete_expired(self) -> int:
        """Delete rows whose expires_at has passed. Returns rows removed.

        Housekeeping — without it the table grows by one row per rotation,
        ~96 rows/day/user at a 15-minute access token.
        """
        ...
