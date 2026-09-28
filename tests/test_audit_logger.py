"""Tests for NotificationLogAuditLogger — the DB-backed audit trail.

Verifies the logger writes one notification_logs row per delivery with:
- recipient_email = comma-joined recipients
- status = "SENT" on success, "FAILED" on failure
- error_message populated on failure
- sent_at = delivered_at on success
- template_name / subject / variables carried through from the request
- correlation_id preserved
- DB failures swallowed (ABC contract: never raises)
"""
from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from app.services.notifications.logger.audit_logger import (
    NotificationLogAuditLogger,
)
from app.services.notifications.logger.base import NotificationLogger
from app.services.notifications.models import DeliveryResult


# ── Fake repository ────────────────────────────────────────────────────


@dataclass
class CapturedLog:
    """Snapshot of a single row passed to the repo."""

    template_name: str | None
    recipient_email: str
    subject: str | None
    variables: dict[str, Any]
    status: str
    error_message: str | None
    sent_at: datetime | None
    correlation_id: str | None


class FakeNotificationLogRepository:
    """Records every record_outcome() call so tests can assert on it."""

    def __init__(self, raise_on_record: Exception | None = None):
        self.logs: list[CapturedLog] = []
        self.raise_on_record = raise_on_record

    def record_outcome(
        self,
        *,
        template_name: str | None,
        recipient_email: str,
        subject: str | None,
        variables: dict[str, Any],
        status: str,
        error_message: str | None,
        sent_at: datetime | None,
        correlation_id: str | None,
    ) -> UUID:
        if self.raise_on_record:
            raise self.raise_on_record
        log = CapturedLog(
            template_name=template_name,
            recipient_email=recipient_email,
            subject=subject,
            variables=dict(variables),
            status=status,
            error_message=error_message,
            sent_at=sent_at,
            correlation_id=correlation_id,
        )
        self.logs.append(log)
        return uuid4()


# ── Tests ───────────────────────────────────────────────────────────────


class AuditLoggerSuccessTests(unittest.TestCase):
    def test_writes_sent_row_on_successful_delivery(self):
        repo = FakeNotificationLogRepository()
        logger = NotificationLogAuditLogger(repo=repo)
        delivered_at = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = DeliveryResult.ok(
            channel="email",
            recipients=("alice@example.com",),
            delivered_at=delivered_at,
            latency_ms=42,
            provider_message_id="tx-1",
        )
        logger.record(
            result,
            correlation_id="corr-1",
            template_name="circular_notification.html",
            subject="Circular 123",
            variables={"full_reference": "123"},
        )
        self.assertEqual(len(repo.logs), 1)
        row = repo.logs[0]
        self.assertEqual(row.status, "SENT")
        self.assertEqual(row.recipient_email, "alice@example.com")
        self.assertEqual(row.template_name, "circular_notification.html")
        self.assertEqual(row.subject, "Circular 123")
        self.assertEqual(row.variables, {"full_reference": "123"})
        self.assertEqual(row.correlation_id, "corr-1")
        self.assertEqual(row.sent_at, delivered_at)
        self.assertIsNone(row.error_message)

    def test_joins_multiple_recipients_with_comma(self):
        repo = FakeNotificationLogRepository()
        logger = NotificationLogAuditLogger(repo=repo)
        result = DeliveryResult.ok(
            channel="email",
            recipients=("alice@example.com", "bob@example.com"),
            delivered_at=datetime.now(timezone.utc),
            latency_ms=10,
        )
        logger.record(result, correlation_id=None)
        self.assertEqual(
            repo.logs[0].recipient_email,
            "alice@example.com, bob@example.com",
        )


class AuditLoggerFailureTests(unittest.TestCase):
    def test_writes_failed_row_with_error_message_on_failure(self):
        repo = FakeNotificationLogRepository()
        logger = NotificationLogAuditLogger(repo=repo)
        delivered_at = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = DeliveryResult.fail(
            channel="email",
            recipients=("alice@example.com",),
            error_code="splitter_401",
            error_detail="unauthorized",
            delivered_at=delivered_at,
            latency_ms=10,
        )
        logger.record(
            result,
            correlation_id="corr-2",
            template_name="otp_email.html",
        )
        self.assertEqual(len(repo.logs), 1)
        row = repo.logs[0]
        self.assertEqual(row.status, "FAILED")
        self.assertEqual(row.error_message, "splitter_401: unauthorized")
        self.assertIsNone(row.sent_at)
        self.assertEqual(row.template_name, "otp_email.html")
        self.assertEqual(row.correlation_id, "corr-2")

    def test_failure_with_no_error_detail_still_writes_status(self):
        repo = FakeNotificationLogRepository()
        logger = NotificationLogAuditLogger(repo=repo)
        result = DeliveryResult.fail(
            channel="email",
            recipients=("a@x.com",),
            error_code="smtp_auth",
            error_detail=None,
            delivered_at=datetime.now(timezone.utc),
        )
        logger.record(result, correlation_id=None)
        row = repo.logs[0]
        self.assertEqual(row.status, "FAILED")
        self.assertEqual(row.error_message, "smtp_auth")


class AuditLoggerExceptionSafetyTests(unittest.TestCase):
    def test_swallows_repo_exception_and_does_not_propagate(self):
        # Audit failure must NEVER escape the logger — the delivery result
        # is already in the caller's hands, raising here would re-raise
        # out of NotificationService.send() and break the contract.
        repo = FakeNotificationLogRepository(
            raise_on_record=RuntimeError("audit DB unreachable")
        )
        logger = NotificationLogAuditLogger(repo=repo)
        result = DeliveryResult.ok(
            channel="email",
            recipients=("a@x.com",),
            delivered_at=datetime.now(timezone.utc),
            latency_ms=0,
        )
        # Must not raise.
        logger.record(result, correlation_id="x")
        # Nothing was written but no exception leaked.
        self.assertEqual(repo.logs, [])


class AuditLoggerIsNotificationLoggerTests(unittest.TestCase):
    def test_implements_notification_logger_interface(self):
        repo = FakeNotificationLogRepository()
        logger = NotificationLogAuditLogger(repo=repo)
        self.assertIsInstance(logger, NotificationLogger)


if __name__ == "__main__":
    unittest.main()