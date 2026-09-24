"""MfaChallengeService — manages MFA OTP challenge lifecycle.

Concerns:
  - generate codes
  - hash codes (with pepper)
  - store / retrieve / verify challenges
  - one-active-per-user invariant
  - send OTP via NotificationService (delegates delivery)

Uses:
  - OtpGenerator (strategy)
  - OtpHasher (strategy)
  - MfaChallengeRepository (persistence)
  - NotificationService (delivery)
  - UsersRepository (recipient lookup)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from app.auth.mfa.repository.mfa_challenge_repository import (
    ConsumeResult,
    MfaChallengeRepository,
)
from app.auth.mfa.generators.base import OtpGenerator
from app.auth.mfa.hashing.base import OtpHasher
from app.services.notifications.models import NotificationRequest
from app.services.notifications.notification_service import NotificationService
from config import Config


logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class VerifyOutcome:
    """Outcome of a verify attempt. Generic — caller maps to HTTP code."""

    success: bool = False
    locked: bool = False      # attempts exceeded max
    expired: bool = False    # challenge past expires_at
    consumed: bool = False    # challenge was already consumed


class MfaChallengeService:
    def __init__(
        self,
        *,
        generator: OtpGenerator,
        hasher: OtpHasher,
        challenge_repo: MfaChallengeRepository,
        notification_service: NotificationService,
        users_repo: Any,                          # UsersRepository (existing)
        otp_length: int = Config.OTP_LENGTH,
        otp_ttl_seconds: int = Config.OTP_TTL_SECONDS,
        max_attempts: int = Config.OTP_MAX_ATTEMPTS,
    ) -> None:
        self._generator = generator
        self._hasher = hasher
        self._repo = challenge_repo
        self._notifications = notification_service
        self._users = users_repo
        self._length = otp_length
        self._ttl = otp_ttl_seconds
        self._max_attempts = max_attempts

    def issue(self, *, user_db_id: UUID, channels: list[str]) -> list[str]:
        """Generate one OTP and send it to all requested channels.

        Returns the list of channels that actually received the OTP
        (only those the user has configured for themselves).

        One-active-per-user invariant: any prior unconsumed challenge
        for this user is invalidated before the new one is stored.
        """
        user = self._users.get_by_uuid(str(user_db_id))
        if user is None:
            logger.warning("user %s not found; cannot issue OTP", user_db_id)
            return []

        # Filter to channels the user actually has configured.
        enabled: list[str] = []
        if "email" in channels and user.email:
            enabled.append("email")
        if "sms" in channels and user.phone_e164:
            enabled.append("sms")
        if not enabled:
            logger.warning(
                "user %s requested channels %r but has none enabled",
                user_db_id, channels,
            )
            return []

        # ONE code, ONE hash, ONE row. The channel column on the row is
        # informational (the primary channel); the same code is sent to
        # ALL enabled channels.
        code = self._generator.generate(self._length)
        code_hash = self._hasher.hash(code)
        self._repo.consume_prior_for_user(user_db_id)
        self._repo.create(
            user_db_id=user_db_id,
            channel=enabled[0],
            code_hash=code_hash,
            ttl_seconds=self._ttl,
        )

        for ch in enabled:
            self._send_otp(user_db_id=user_db_id, code=code, channel=ch)

        return enabled

    def verify(self, *, user_db_id: UUID, code: str) -> VerifyOutcome:
        """Verify a candidate code against the user's active challenge."""
        challenge = self._repo.get_active(user_db_id)
        if challenge is None:
            return VerifyOutcome(expired=True)

        candidate_hash = self._hasher.hash(code)
        result = self._repo.consume_if_match_and_under_attempts(
            challenge_id=challenge.id,
            candidate_hash=candidate_hash,
            max_attempts=self._max_attempts,
        )

        if result is ConsumeResult.MATCH:
            return VerifyOutcome(success=True)
        if result is ConsumeResult.LOCKED:
            return VerifyOutcome(locked=True)
        if result is ConsumeResult.EXPIRED:
            return VerifyOutcome(expired=True)
        if result is ConsumeResult.CONSUMED:
            return VerifyOutcome(consumed=True)
        # MISMATCH
        return VerifyOutcome()

    def _send_otp(self, *, user_db_id: UUID, code: str, channel: str) -> None:
        user = self._users.get_by_uuid(str(user_db_id))
        if user is None:
            logger.warning("user %s not found; cannot send OTP", user_db_id)
            return
        if channel == "email":
            recipient = user.email
            template = "otp_email.html"
            physical_channel = "smtp_email"
        elif channel == "sms":
            recipient = user.phone_e164
            template = "otp_sms.txt"
            physical_channel = "splitter_sms"   # not yet implemented
        else:
            logger.warning("unknown channel %r", channel)
            return
        if not recipient:
            logger.warning(
                "user %s has no %s on file; cannot send OTP",
                user_db_id, channel,
            )
            return

        try:
            self._notifications.send(NotificationRequest(
                channel=physical_channel,
                recipient=recipient,
                template_name=template,
                subject="CircularHub: Your verification code",
                variables={
                    "code": code,
                    "ttl_minutes": self._ttl // 60,
                },
            ))
        except Exception:
            # Never fail issue() on delivery error — user will see 401 on verify.
            logger.exception("OTP delivery failed for user=%s", user_db_id)
