"""Tests for the DeliveryResult.recipients tuple migration."""
from __future__ import annotations

import unittest
from datetime import datetime, timezone

from app.services.notifications.models import DeliveryResult


_NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


class DeliveryResultRecipientsTests(unittest.TestCase):
    def test_ok_with_single_recipient(self):
        result = DeliveryResult.ok(
            channel="email",
            recipients=("alice@example.com",),
            delivered_at=_NOW,
            latency_ms=10,
        )
        self.assertEqual(result.recipients, ("alice@example.com",))
        self.assertTrue(result.success)

    def test_ok_with_multiple_recipients(self):
        result = DeliveryResult.ok(
            channel="email",
            recipients=("alice@example.com", "bob@example.com", "carol@example.com"),
            delivered_at=_NOW,
            latency_ms=10,
            provider_message_id="tx-123",
        )
        self.assertEqual(
            result.recipients,
            ("alice@example.com", "bob@example.com", "carol@example.com"),
        )
        self.assertEqual(result.provider_message_id, "tx-123")

    def test_fail_with_multiple_recipients(self):
        result = DeliveryResult.fail(
            channel="email",
            recipients=("alice@example.com", "bob@example.com"),
            error_code="splitter_401",
            error_detail="unauthorized",
            delivered_at=_NOW,
            latency_ms=5,
        )
        self.assertEqual(
            result.recipients,
            ("alice@example.com", "bob@example.com"),
        )
        self.assertFalse(result.success)
        self.assertEqual(result.error_code, "splitter_401")
        self.assertEqual(result.error_detail, "unauthorized")

    def test_fail_requires_error_code(self):
        # error_code is a required kwarg on fail() — missing it raises TypeError
        with self.assertRaises(TypeError):
            DeliveryResult.fail(
                channel="email",
                recipients=("alice@example.com",),
                delivered_at=_NOW,
            )

    def test_ok_rejects_error_code_kwarg(self):
        # ok() does not accept error_code at all — passing it raises TypeError
        with self.assertRaises(TypeError):
            DeliveryResult.ok(
                channel="email",
                recipients=("alice@example.com",),
                delivered_at=_NOW,
                latency_ms=0,
                error_code="oops",
            )

    def test_rejects_empty_recipients(self):
        with self.assertRaises(ValueError):
            DeliveryResult.ok(
                channel="email",
                recipients=(),
                delivered_at=_NOW,
                latency_ms=0,
            )


if __name__ == "__main__":
    unittest.main()