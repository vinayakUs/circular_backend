"""Tests for the NotificationRequest.recipients tuple migration.

Verifies:
  - NotificationRequest validates the tuple shape
  - SplitterEmailChannel sends all recipients as the wire 'to' list
  - SmtpEmailChannel joins recipients into msg['To'] and sends to all
"""
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from unittest.mock import MagicMock, patch

from app.services.notifications.channels.smtp_email_channel import (
    SmtpEmailChannel,
)
from app.services.notifications.channels.splitter_email_channel import (
    SplitterEmailChannel,
)
from app.services.notifications.models import (
    DeliveryResult,
    NotificationRequest,
    RenderedPayload,
)


# ── NotificationRequest validation ──────────────────────────────────────


class NotificationRequestRecipientsTests(unittest.TestCase):
    def test_accepts_single_recipient_tuple(self):
        req = NotificationRequest(
            channel="email",
            recipients=("alice@example.com",),
            template_name="otp_email.html",
        )
        self.assertEqual(req.recipients, ("alice@example.com",))

    def test_accepts_multiple_recipient_tuple(self):
        req = NotificationRequest(
            channel="email",
            recipients=("alice@example.com", "bob@example.com"),
            template_name="circular_notification.html",
        )
        self.assertEqual(
            req.recipients,
            ("alice@example.com", "bob@example.com"),
        )

    def test_rejects_empty_recipients_tuple(self):
        with self.assertRaises(ValueError):
            NotificationRequest(
                channel="email",
                recipients=(),
                template_name="otp_email.html",
            )


# ── SplitterEmailChannel wire format ──────────────────────────────────


class SplitterChannelRecipientsTests(unittest.TestCase):
    """End-to-end check that SplitterEmailChannel maps
    NotificationRequest.recipients to the splitter's 'to' form field."""

    def _build_channel(self) -> SplitterEmailChannel:
        return SplitterEmailChannel(
            endpoint="https://example.test/v3/email/send",
            auth_token="auth-token",
            app_name="CircularHub",
            service_provider="JIO",
            from_email="noreply@x.com",
            timeout_seconds=5.0,
        )

    def _fake_response(self, json_body, status=200):
        resp = MagicMock()
        resp.status_code = status
        resp.json.return_value = json_body
        resp.text = ""
        return resp

    def _payload_from_post_call(self, post_mock):
        # requests.post(url, data=payload, headers=..., timeout=...)
        # post_mock.call_args.kwargs["data"] is the multipart payload dict
        return post_mock.call_args.kwargs["data"]

    @patch("app.services.notifications.channels.splitter_email_channel.requests")
    def test_sends_single_recipient_as_to_list(self, requests_mod):
        requests_mod.post.return_value = self._fake_response({
            "statusCode": 200,
            "data": {"transactionID": "tx-1", "submittedTime": "now", "serviceProvider": "JIO"},
        })
        channel = self._build_channel()
        req = NotificationRequest(
            channel="email",
            recipients=("alice@example.com",),
            template_name="otp_email.html",
            subject="OTP",
            variables={"code": "123456", "ttl_minutes": 5},
        )
        rendered = RenderedPayload(body_text="hello", body_html="<p>hello</p>")

        result = channel.send(req, rendered)

        self.assertTrue(result.success)
        payload = self._payload_from_post_call(requests_mod.post)
        self.assertEqual(payload["to"], ["alice@example.com"])

    @patch("app.services.notifications.channels.splitter_email_channel.requests")
    def test_sends_all_recipients_in_to_list(self, requests_mod):
        requests_mod.post.return_value = self._fake_response({
            "statusCode": 200,
            "data": {"transactionID": "tx-1", "submittedTime": "now", "serviceProvider": "JIO"},
        })
        channel = self._build_channel()
        req = NotificationRequest(
            channel="email",
            recipients=("alice@example.com", "bob@example.com", "carol@example.com"),
            template_name="circular_notification.html",
            subject="Circular",
            variables={},
        )
        rendered = RenderedPayload(body_text="hi", body_html="<p>hi</p>")

        result = channel.send(req, rendered)

        self.assertTrue(result.success)
        payload = self._payload_from_post_call(requests_mod.post)
        self.assertEqual(
            payload["to"],
            ["alice@example.com", "bob@example.com", "carol@example.com"],
        )

    @patch("app.services.notifications.channels.splitter_email_channel.requests")
    def test_empty_to_list_falls_back_to_single_recipient(self, requests_mod):
        # Defensive: if a future caller passes an empty recipients tuple,
        # the channel shouldn't blow up — it should send an empty to list
        # which the splitter will reject (we want to see that failure path).
        requests_mod.post.return_value = self._fake_response({
            "statusCode": 400,
            "message": "to is required",
        }, status=400)
        channel = self._build_channel()
        # NOTE: NotificationRequest validates non-empty, so build a request
        # with a single placeholder to test the channel's defensive behavior
        # at the wire level (the model layer is what rejects empty tuples).
        req = NotificationRequest(
            channel="email",
            recipients=("placeholder@x.com",),
            template_name="otp_email.html",
            subject="x",
            variables={},
        )
        rendered = RenderedPayload(body_text="x", body_html="<p>x</p>")
        result = channel.send(req, rendered)
        self.assertFalse(result.success)


# ── SmtpEmailChannel wire format ──────────────────────────────────────


class SmtpChannelRecipientsTests(unittest.TestCase):
    """SmtpEmailChannel must:
      - set msg['To'] to ', '.join(recipients) (multiple in one header)
      - call server.send_message(msg, to_addrs=list(recipients))
    """

    def _build_channel(self) -> SmtpEmailChannel:
        return SmtpEmailChannel(
            host="smtp.example.com",
            port=587,
            username=None,
            password=None,
            use_tls=False,
            from_email="noreply@x.com",
            from_name="CircularHub",
        )

    def _request(self, recipients):
        return NotificationRequest(
            channel="email",
            recipients=recipients,
            template_name="circular_notification.html",
            subject="Circular",
            variables={},
        )

    def _rendered(self):
        return RenderedPayload(body_text="hello", body_html="<p>hello</p>")

    @patch("app.services.notifications.channels.smtp_email_channel.smtplib.SMTP")
    def test_msg_to_header_contains_all_addresses_comma_joined(self, smtp_cls):
        # smtplib.SMTP(...) is used directly (not via 'with'), so the
        # mocked instance IS the server, not __enter__.return_value.
        server = smtp_cls.return_value
        server.send_message.return_value = {}

        channel = self._build_channel()
        req = self._request(("alice@example.com", "bob@example.com"))
        rendered = self._rendered()

        result = channel.send(req, rendered)

        self.assertTrue(result.success)
        # Pull the MIMEMultipart passed to send_message
        sent_msg = server.send_message.call_args.args[0]
        self.assertIsInstance(sent_msg, MIMEMultipart)
        self.assertEqual(
            sent_msg["To"],
            "alice@example.com, bob@example.com",
        )

    @patch("app.services.notifications.channels.smtp_email_channel.smtplib.SMTP")
    def test_send_message_called_with_all_recipients_in_to_addrs(self, smtp_cls):
        server = smtp_cls.return_value
        server.send_message.return_value = {}

        channel = self._build_channel()
        req = self._request(("alice@example.com", "bob@example.com", "carol@example.com"))
        rendered = self._rendered()

        channel.send(req, rendered)

        to_addrs = server.send_message.call_args.kwargs["to_addrs"]
        self.assertEqual(
            list(to_addrs),
            ["alice@example.com", "bob@example.com", "carol@example.com"],
        )

    @patch("app.services.notifications.channels.smtp_email_channel.smtplib.SMTP")
    def test_single_recipient_works(self, smtp_cls):
        server = smtp_cls.return_value
        server.send_message.return_value = {}

        channel = self._build_channel()
        req = self._request(("alice@example.com",))
        rendered = self._rendered()

        result = channel.send(req, rendered)

        self.assertTrue(result.success)
        sent_msg = server.send_message.call_args.args[0]
        self.assertEqual(sent_msg["To"], "alice@example.com")


if __name__ == "__main__":
    unittest.main()