"""Refresh-token rotation chain.

One token is issued at login with a fresh ``family_id``. Every refresh
consumes the presented row and mints a successor into the same family.
Presenting an already-consumed token means a copy leaked, so the whole
family is revoked — see ``rotate()``.
"""
from __future__ import annotations

import hashlib
import logging
import secrets
import uuid
from datetime import datetime, timezone
from uuid import UUID

from app.auth.session.repository.refresh_token_repository import (
    RefreshToken,
    RefreshTokenRepository,
)
from config import Config


logger = logging.getLogger(__name__)


class RefreshTokenError(Exception):
    """Base for all refresh-token failures."""


class RefreshTokenInvalidError(RefreshTokenError):
    """Token unknown, expired, or lost a concurrent-rotation race.

    Nothing is revoked and no security event is logged — this is the
    ordinary "session over, log in again" path.
    """


class RefreshTokenReuseError(RefreshTokenError):
    """A consumed token was presented again: the token leaked. The entire
    family has ALREADY been revoked by the time this is raised.
    """


class RefreshTokenService:
    def __init__(
        self,
        *,
        repo: RefreshTokenRepository,
        ttl_seconds: int = Config.REFRESH_TOKEN_TTL_MINUTES * 60,
        reuse_grace_seconds: int = Config.REFRESH_REUSE_GRACE_SECONDS,
    ) -> None:
        self._repo = repo
        self._ttl = ttl_seconds
        self._grace = reuse_grace_seconds

    # ── Issuance ────────────────────────────────────────────────
    def issue(
        self,
        *,
        user_db_id: UUID,
        issued_ip: str | None,
        issued_user_agent: str | None,
    ) -> tuple[str, RefreshToken]:
        """Mint the first token of a brand-new rotation chain."""
        return self._mint(
            user_db_id=user_db_id,
            family_id=uuid.uuid4(),
            issued_ip=issued_ip,
            issued_user_agent=issued_user_agent,
        )

    # ── Rotation ────────────────────────────────────────────────
    def rotate(
        self,
        *,
        plaintext_token: str,
        issued_ip: str | None,
        issued_user_agent: str | None,
    ) -> tuple[str, RefreshToken]:
        """Consume the presented token and mint its successor.

        Raises RefreshTokenInvalidError when the token is unknown, expired,
        or was consumed by a concurrent rotation first (nothing revoked);
        RefreshTokenReuseError when an already-consumed token comes back
        outside the grace window (the whole family is revoked first).
        """
        if not plaintext_token:
            raise RefreshTokenInvalidError("empty refresh token")

        record = self._repo.get_by_hash(self._hash(plaintext_token))
        if record is None:
            raise RefreshTokenInvalidError("unknown refresh token")

        if record.consumed:
            # Our own second tab racing us (benign) or a leaked copy (not).
            # The age of the consumption separates them; fail closed if
            # consumed_at is somehow missing.
            if self._within_grace(record.consumed_at):
                logger.info("Refresh replay within grace for user=%s family=%s", record.user_db_id, record.family_id)
                return self._mint(
                    user_db_id=record.user_db_id,
                    family_id=record.family_id,
                    issued_ip=issued_ip,
                    issued_user_agent=issued_user_agent,
                )

            revoked = self._repo.revoke_family(record.family_id)
            logger.warning("REFRESH TOKEN REUSE DETECTED user=%s family=%s consumed_at=%s revoked=%s token(s)", record.user_db_id, record.family_id, record.consumed_at, revoked)
            raise RefreshTokenReuseError(f"refresh token reused; revoked {revoked} token(s) in family {record.family_id}")

        if record.expires_at <= datetime.now(timezone.utc):
            raise RefreshTokenInvalidError("refresh token expired")

        # Atomic flip. If this returns False a concurrent request consumed
        # the token first. Do NOT mint: the loser's client still holds a
        # dead token, and minting here would branch the family and let the
        # user's own next request look like a replay. Its retry lands in
        # the grace branch above.
        if not self._repo.consume(record.id):
            raise RefreshTokenInvalidError("refresh token consumed concurrently")

        return self._mint(
            user_db_id=record.user_db_id,
            family_id=record.family_id,
            issued_ip=issued_ip,
            issued_user_agent=issued_user_agent,
        )

    # ── Revocation ──────────────────────────────────────────────
    def revoke(self, plaintext_token: str | None) -> bool:
        """Kill the family of the presented token.

        Purges rather than marks consumed, so a logged-out token stops
        existing instead of lingering inside the reuse grace window where
        it would still be honoured. False only if the token was unknown.
        """
        if not plaintext_token:
            return False
        record = self._repo.get_by_hash(self._hash(plaintext_token))
        if record is None:
            return False
        self._repo.purge_family(record.family_id)
        logger.info("Revoked refresh family %s for user=%s", record.family_id, record.user_db_id)
        return True

    def revoke_all_for_user(self, user_db_id: UUID) -> int:
        """End every session for a user."""
        revoked = self._repo.revoke_all_for_user(user_db_id)
        logger.info("Revoked %s refresh token(s) across all families for user=%s", revoked, user_db_id)
        return revoked

    # ── Internals ───────────────────────────────────────────────
    def _mint(
        self,
        *,
        user_db_id: UUID,
        family_id: UUID,
        issued_ip: str | None,
        issued_user_agent: str | None,
    ) -> tuple[str, RefreshToken]:
        plaintext = secrets.token_urlsafe(32)
        record = self._repo.create(
            user_db_id=user_db_id,
            token_hash=self._hash(plaintext),
            family_id=family_id,
            issued_ip=issued_ip,
            issued_user_agent=(issued_user_agent or "")[:200],
            ttl_seconds=self._ttl,
        )
        return plaintext, record

    @staticmethod
    def _hash(plaintext: str) -> str:
        return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()

    def _within_grace(self, consumed_at: datetime | None) -> bool:
        if consumed_at is None:
            return False
        age = (datetime.now(timezone.utc) - consumed_at).total_seconds()
        return 0 <= age <= self._grace
