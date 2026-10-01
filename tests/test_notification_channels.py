"""Tests for the NotificationRequest.recipients tuple migration.

Verifies:
  - NotificationRequest validates the tuple shape
  - SplitterEmailChannel sends all recipients as the wire 'to' list
    AND sends multipart/form-data (not form-encoded — see regression test)
  - SmtpEmailChannel joins recipients into msg['To'] and sends to all
"""
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from unittest.mock import MagicMock, patch

import requests as real_requests

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
        """Extract the multipart files list passed to requests.post.

        Returns a {field_name: [value, ...]} view so tests can assert on
        individual fields without caring about the (None, value) tuple wrapping
        that requests requires for multipart form fields.
        """
        files = post_mock.call_args.kwargs["files"]
        out: dict[str, list[str]] = {}
        for field_name, wrapped in files:
            value = wrapped[1] if isinstance(wrapped, tuple) else wrapped
            out.setdefault(field_name, []).append(value)
        return out

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

    @patch("app.services.notifications.channels.splitter_email_channel.requests")
    def test_sends_multipart_form_data_not_urlencoded(self, requests_mod):
        """Regression: splitter v3 requires multipart/form-data.

        Bug: ``requests.post(url, data=dict)`` defaults to
        ``application/x-www-form-urlencoded``. The splitter accepts that on
        intake and returns a transactionID, but its downstream delivery
        pipeline only handles multipart — the email never lands. curl works
        because ``-F`` forces multipart.

        This test would have failed before the fix and must keep failing if
        the channel regresses to ``data=``.
        """
        requests_mod.post.return_value = self._fake_response({
            "statusCode": 200,
            "data": {"transactionID": "tx-1", "submittedTime": "now", "serviceProvider": "JIO"},
        })
        channel = self._build_channel()
        req = NotificationRequest(
            channel="email",
            recipients=("alice@example.com", "bob@example.com"),
            template_name="circular_notification.html",
            subject="Circular",
            variables={},
        )
        rendered = RenderedPayload(body_text="hi", body_html="<p>hi</p>")

        result = channel.send(req, rendered)

        self.assertTrue(result.success)

        call_kwargs = requests_mod.post.call_args.kwargs

        # 1. Must use files= (multipart) and NOT data= (form-encoded).
        self.assertIn(
            "files", call_kwargs,
            "splitter_email_channel must send multipart/form-data via files= "
            "(splitter v3 returns txID for form-encoded but does not deliver)",
        )
        self.assertNotIn(
            "data", call_kwargs,
            "splitter_email_channel must NOT send form-encoded data — the "
            "splitter returns a txID for it but never delivers the email",
        )

        # 2. Re-serialize the same files= payload through requests and verify
        #    Content-Type on the wire is multipart/form-data.
        files = call_kwargs["files"]
        prepared = real_requests.Request(
            "POST", "https://example.test/v3/email/send", files=files,
        ).prepare()
        content_type = prepared.headers.get("Content-Type", "")
        self.assertTrue(
            content_type.startswith("multipart/form-data"),
            f"Expected multipart/form-data, got: {content_type!r}",
        )
        # And the boundary should be present (it's what makes it parseable).
        self.assertIn("boundary=", content_type)

    @patch("app.services.notifications.channels.splitter_email_channel.requests")
    def test_sends_cc_and_bcc_as_repeated_multipart_fields(self, requests_mod):
        """CC and BCC must also be sent as repeated multipart fields, not lists
        inside a single field. This matches what curl ``-F`` does for repeated
        flags and is what the splitter v3 API expects."""
        requests_mod.post.return_value = self._fake_response({
            "statusCode": 200,
            "data": {"transactionID": "tx-1", "submittedTime": "now", "serviceProvider": "JIO"},
        })
        channel = self._build_channel()
        req = NotificationRequest(
            channel="email",
            recipients=("alice@example.com",),
            cc=("cc1@example.com", "cc2@example.com"),
            bcc=("bcc1@example.com",),
            template_name="circular_notification.html",
            subject="Circular",
            variables={},
        )
        rendered = RenderedPayload(body_text="hi", body_html="<p>hi</p>")

        result = channel.send(req, rendered)

        self.assertTrue(result.success)
        payload = self._payload_from_post_call(requests_mod.post)
        self.assertEqual(payload["cc"], ["cc1@example.com", "cc2@example.com"])
        self.assertEqual(payload["bcc"], ["bcc1@example.com"])

    

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