"""MfaPendingTokenService — manages MFA pending-token lifecycle.

Issued after /login (captcha+password+DB-row success), consumed at /mfa/verify.
"""
from __future__ import annotations

import hashlib
import logging
import secrets
from typing import Any
from uuid import UUID

from app.auth.mfa.repository.mfa_pending_token_repository import (
    MfaPendingToken,
    MfaPendingTokenRepository,
)
from config import Config


logger = logging.getLogger(__name__)


class MfaPendingTokenService:
    def __init__(
        self,
        *,
        repo: MfaPendingTokenRepository,
        ttl_seconds: int = Config.MFA_PENDING_TOKEN_TTL_SECONDS,
    ) -> None:
        self._repo = repo
        self._ttl = ttl_seconds

    def mint(
        self,
        *,
        user_db_id: UUID,
        issued_ip: str | None,
        issued_user_agent: str | None,
    ) -> str:
        """Generate and store a new pending token. Returns plaintext token."""
        # Invalidate prior tokens for this user (one-active-per-user).
        self._repo.consume_prior_for_user(user_db_id)

        plaintext = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(plaintext.encode("utf-8")).hexdigest()
        self._repo.create(
            user_db_id=user_db_id,
            token_hash=token_hash,
            issued_ip=issued_ip,
            issued_user_agent=(issued_user_agent or "")[:200],
            ttl_seconds=self._ttl,
        )
        return plaintext

    def verify(self, plaintext_token: str) -> MfaPendingToken | None:
        """Look up by SHA-256 hash. Returns None if invalid/expired/consumed."""
        if not plaintext_token:
            return None
        token_hash = hashlib.sha256(plaintext_token.encode("utf-8")).hexdigest()
        return self._repo.get_active_by_hash(token_hash)

    def consume(self, token_id: UUID) -> bool:
        """Atomically mark token consumed. Single-use."""
        return self._repo.consume(token_id)
