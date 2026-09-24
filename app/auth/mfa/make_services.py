"""MFA composition root.

The ONLY file that imports concrete MFA classes. Builds and wires:
  - SecureRandomOtpGenerator + HmacSha256OtpHasher
  - PostgresMfaChallengeRepository + PostgresMfaPendingTokenRepository
  - MfaChallengeService + MfaPendingTokenService

Uses the existing notification subsystem via get_notification_service().
"""
from __future__ import annotations

import logging
import threading

from config import Config
from db.postgres_client import get_postgres_client

from app.auth.mfa.generators.secure_random_otp import SecureRandomOtpGenerator
from app.auth.mfa.hashing.hmac_sha256_otp import HmacSha256OtpHasher
from app.auth.mfa.repository.postgres_mfa_challenge_repository import (
    PostgresMfaChallengeRepository,
)
from app.auth.mfa.repository.postgres_mfa_pending_token_repository import (
    PostgresMfaPendingTokenRepository,
)
from app.auth.mfa.mfa_challenge_service import MfaChallengeService
from app.auth.mfa.mfa_pending_token_service import MfaPendingTokenService
from app.services.notifications.make_service import get_notification_service


logger = logging.getLogger(__name__)


def make_mfa_services() -> tuple[MfaChallengeService, MfaPendingTokenService]:
    """Build MFA services. Called once at app startup or lazily."""
    pool = get_postgres_client().get_pool()

    if not Config.OTP_PEPPER_SECRET:
        raise RuntimeError(
            "OTP_PEPPER_SECRET not set in env. "
            "Generate with: "
            "python -c \"import secrets; print(secrets.token_hex(32))\""
        )

    generator = SecureRandomOtpGenerator()
    hasher = HmacSha256OtpHasher(
        pepper=Config.OTP_PEPPER_SECRET.encode("utf-8")
    )

    challenge_repo = PostgresMfaChallengeRepository(db_pool=pool)
    pending_repo = PostgresMfaPendingTokenRepository(db_pool=pool)

    notification_service = get_notification_service()

    # UsersRepository already exists in ingestion/repository/
    from ingestion.repository.users_repository import UsersRepository
    users_repo = UsersRepository(db_pool=pool)

    challenge_service = MfaChallengeService(
        generator=generator,
        hasher=hasher,
        challenge_repo=challenge_repo,
        notification_service=notification_service,
        users_repo=users_repo,
    )
    pending_service = MfaPendingTokenService(repo=pending_repo)

    return challenge_service, pending_service


_challenge: MfaChallengeService | None = None
_pending: MfaPendingTokenService | None = None
_lock = threading.Lock()


def get_mfa_services() -> tuple[MfaChallengeService, MfaPendingTokenService]:
    """Lazy thread-safe singleton."""
    global _challenge, _pending
    if _challenge is None or _pending is None:
        with _lock:
            if _challenge is None or _pending is None:
                _challenge, _pending = make_mfa_services()
                logger.info("MFA services initialized")
    return _challenge, _pending
