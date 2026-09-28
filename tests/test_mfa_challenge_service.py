"""Tests for MfaChallengeService._send_otp error handling.

Exercises the failure-logging and correlation-id paths that the
NotificationService.send() result previously masked.
"""
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from uuid import UUID, uuid4

from app.auth.mfa.mfa_challenge_service import MfaChallengeService
from app.auth.mfa.repository.mfa_challenge_repository import (
    MfaChallenge,
)
from app.services.notifications.models import (
    DeliveryResult,
    NotificationRequest,
)


USER_UUID = UUID("11111111-1111-1111-1111-111111111111")


# ── Stubs ──────────────────────────────────────────────────────────────


class _StubGenerator:
    def generate(self, length: int) -> str:
        return "123456"


class _StubHasher:
    def hash(self, code: str) -> str:
        return f"h:{code}"


class _StubChallengeRepo:
    """Just enough to satisfy issue() — _send_otp doesn't touch the repo."""

    def create(self, *, user_db_id, channel, code_hash, ttl_seconds):
        now = datetime.now(timezone.utc)
        return MfaChallenge(
            id=uuid4(),
            user_db_id=user_db_id,
            channel=channel,
            code_hash=code_hash,
            attempts=0,
            consumed=False,
            created_at=now,
            expires_at=now,
        )

    def consume_prior_for_user(self, user_db_id):
        return 0

    def get_active(self, user_db_id):
        return None

    def consume_if_match_and_under_attempts(self, **kwargs):
        return None


class _StubUser:
    def __init__(self, email: str | None = None, phone_e164: str | None = None):
        self.email = email
        self.phone_e164 = phone_e164


class _StubUsersRepo:
    def __init__(self, user: _StubUser):
        self._user = user

    def get_by_uuid(self, uuid_str):
        return self._user


class _CapturingNotificationService:
    """Stub for NotificationService. Records every NotificationRequest sent
    and returns a configurable DeliveryResult."""

    def __init__(
        self,
        result: DeliveryResult,
        registered_channels: list[str] | None = None,
    ):
        self._result = result
        self._channels = registered_channels or ["smtp_email"]
        self.requests: list[NotificationRequest] = []

    def supported_channels(self) -> list[str]:
        return list(self._channels)

    def send(self, request: NotificationRequest) -> DeliveryResult:
        self.requests.append(request)
        return self._result


def _build_service(
    notification_service,
    user: _StubUser | None = None,
) -> MfaChallengeService:
    if user is None:
        user = _StubUser(email="alice@example.com")
    return MfaChallengeService(
        generator=_StubGenerator(),
        hasher=_StubHasher(),
        challenge_repo=_StubChallengeRepo(),
        notification_service=notification_service,
        users_repo=_StubUsersRepo(user),
    )


# ── Tests ──────────────────────────────────────────────────────────────


class SendOtpFailureLoggingTests(unittest.TestCase):
    """_send_otp must surface DeliveryResult failures via the logger."""

    def test_logs_error_code_and_detail_when_delivery_fails(self):
        failure = DeliveryResult.fail(
            channel="splitter_email",
            recipients=("alice@example.com",),
            error_code="splitter_401",
            error_detail="unauthorized",
            delivered_at=datetime.now(timezone.utc),
        )
        notif = _CapturingNotificationService(result=failure)
        svc = _build_service(notification_service=notif)

        with self.assertLogs(
            "app.auth.mfa.mfa_challenge_service", level="WARNING"
        ) as cm:
            svc.issue(user_db_id=USER_UUID, channels=["email"])

        log_text = "\n".join(cm.output)
        self.assertIn("splitter_401", log_text)
        self.assertIn("unauthorized", log_text)

    def test_does_not_log_warning_when_delivery_succeeds(self):
        success = DeliveryResult.ok(
            channel="splitter_email",
            recipients=("alice@example.com",),
            delivered_at=datetime.now(timezone.utc),
            latency_ms=10,
        )
        notif = _CapturingNotificationService(result=success)
        svc = _build_service(notification_service=notif)

        # No WARNING expected — successful deliveries must stay quiet.
        import logging
        logging.getLogger("app.auth.mfa.mfa_challenge_service").setLevel(
            logging.CRITICAL
        )
        try:
            logger = logging.getLogger("app.auth.mfa.mfa_challenge_service")
            records = []
            handler = logging.Handler()
            handler.emit = lambda record: records.append(record)
            logger.addHandler(handler)
            try:
                svc.issue(user_db_id=USER_UUID, channels=["email"])
            finally:
                logger.removeHandler(handler)
            warning_or_above = [
                r for r in records
                if r.levelno >= logging.WARNING
                and r.name == "app.auth.mfa.mfa_challenge_service"
            ]
            self.assertEqual(warning_or_above, [])
        finally:
            logging.getLogger("app.auth.mfa.mfa_challenge_service").setLevel(
                logging.NOTSET
            )


class SendOtpCorrelationIdTests(unittest.TestCase):
    """_send_otp must set correlation_id so the splitter can trace deliveries."""

    def test_passes_non_empty_correlation_id_per_user(self):
        success = DeliveryResult.ok(
            channel="splitter_email",
            recipients=("alice@example.com",),
            delivered_at=datetime.now(timezone.utc),
            latency_ms=10,
        )
        notif = _CapturingNotificationService(result=success)
        svc = _build_service(notification_service=notif)

        svc.issue(user_db_id=USER_UUID, channels=["email"])

        self.assertEqual(len(notif.requests), 1)
        req = notif.requests[0]
        self.assertIsNotNone(req.correlation_id)
        self.assertTrue(len(req.correlation_id) > 0)
        # Stable per-issue: must reference the user so traces can be grouped.
        self.assertIn(str(USER_UUID), req.correlation_id)


if __name__ == "__main__":
    unittest.main()

# ── Channel selection (regression for hardcoded "smtp_email") ──────────


class SendOtpChannelSelectionTests(unittest.TestCase):
    """_send_otp must pick the registered email channel — not a hardcoded one.

    Regression for the bug where EMAIL_BACKEND=splitter silently failed MFA
    delivery because _send_otp always used "smtp_email" regardless of what
    the composite had registered.
    """

    def _success(self) -> DeliveryResult:
        return DeliveryResult.ok(
            channel="email",
            recipients=("alice@example.com",),
            delivered_at=datetime.now(timezone.utc),
            latency_ms=10,
        )

    def test_uses_smtp_email_when_smtp_backend_registered(self):
        notif = _CapturingNotificationService(
            result=self._success(),
            registered_channels=["smtp_email"],
        )
        svc = _build_service(notification_service=notif)
        svc.issue(user_db_id=USER_UUID, channels=["email"])
        self.assertEqual(notif.requests[0].channel, "smtp_email")

    def test_uses_splitter_email_when_splitter_backend_registered(self):
        notif = _CapturingNotificationService(
            result=self._success(),
            registered_channels=["splitter_email"],
        )
        svc = _build_service(notification_service=notif)
        svc.issue(user_db_id=USER_UUID, channels=["email"])
        self.assertEqual(notif.requests[0].channel, "splitter_email")

    def test_picks_first_email_channel_when_multiple_registered(self):
        # Defensive: if the composite somehow has both, the first one wins.
        notif = _CapturingNotificationService(
            result=self._success(),
            registered_channels=["splitter_email", "smtp_email"],
        )
        svc = _build_service(notification_service=notif)
        svc.issue(user_db_id=USER_UUID, channels=["email"])
        self.assertEqual(notif.requests[0].channel, "splitter_email")

    def test_fails_loudly_when_no_email_channel_registered(self):
        # No email channel registered → explicit ERROR log + early return.
        # No silent fallback, no double-delivery, no swallowed traceback.
        notif = _CapturingNotificationService(
            result=self._success(),
            registered_channels=["splitter_sms"],  # only SMS, no email
        )
        svc = _build_service(notification_service=notif)
        with self.assertLogs(
            "app.auth.mfa.mfa_challenge_service", level="ERROR"
        ) as cm:
            svc.issue(user_db_id=USER_UUID, channels=["email"])
        log_text = "\n".join(cm.output)
        self.assertIn("No email channel registered", log_text)
        # The notification service was never called — we returned early.
        self.assertEqual(notif.requests, [])
