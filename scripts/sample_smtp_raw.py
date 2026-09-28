"""Show the raw data sent to the SMTP server and the raw server responses.

Run from the project root with the venv active:
    python scripts/sample_smtp_raw.py
"""
from __future__ import annotations

import sys
from io import StringIO
from unittest.mock import patch

from app.services.notifications.channels.smtp_email_channel import (
    SmtpEmailChannel,
)
from app.services.notifications.models import (
    NotificationRequest,
    RenderedPayload,
)
from config import Config


def build_channel() -> SmtpEmailChannel:
    return SmtpEmailChannel(
        host=Config.SMTP_HOST,
        port=Config.SMTP_PORT,
        username=Config.SMTP_USERNAME or None,
        password=Config.SMTP_PASSWORD or None,
        use_tls=Config.SMTP_USE_TLS,
        from_email=Config.SMTP_FROM_EMAIL,
        from_name=Config.SMTP_FROM_NAME,
    )


def main() -> int:
    channel = build_channel()
    request = NotificationRequest(
        channel="smtp_email",
        recipients=(
            "vinayak200054321@gmail.com",
            "radhikaandrew12@gmail.com",
        ),
        template_name="sample_email.html",
        subject="Sample — raw data capture",
        correlation_id="sample-raw-001",
        variables={"name": "Tester"},
    )
    rendered = RenderedPayload(
        body_text="Hello raw — text part.",
        body_html="<h1>Hello raw</h1><p>HTML part.</p>",
    )

    # Build the message BEFORE the SMTP conversation happens so we can dump
    # the raw bytes.
    msg = channel._build_message(request, rendered)

    print("=" * 78)
    print("RAW REQUEST (MIME bytes as Python would send them)")
    print("=" * 78)
    raw_bytes = msg.as_bytes()
    print(raw_bytes.decode("utf-8", "replace"))
    print()
    print("=" * 78)
    print("RAW REQUEST (byte length)")
    print("=" * 78)
    print(f"  {len(raw_bytes)} bytes")
    print(f"  sha256: {__import__('hashlib').sha256(raw_bytes).hexdigest()}")
    print()

    # Now actually send. Wrap smtplib.SMTP with debuglevel=1 so we see the
    # full SMTP conversation. debuglevel prints to stderr.
    captured_debug = StringIO()
    real_smtp = __import__("smtplib").SMTP

    def make_debug_smtp():
        class DebugSMTP:
            def __init__(self, host, port, timeout=None, **kw):
                self._real = real_smtp(host, port, timeout=timeout, **kw)
                self._real.set_debuglevel(1)
                # The channel will call set_debuglevel(0) right after
                # construction. Intercept set_debuglevel so we can re-enable
                # debug output and capture it.
                self._debug_enabled = True

            def __getattr__(self, name):
                return getattr(self._real, name)

            def set_debuglevel(self, level):
                # Always force debug ON regardless of what the channel sets.
                self._debug_enabled = True
                return self._real.set_debuglevel(1)

            def send_message(self, m, to_addrs=None):
                # Re-enable debug right before sending in case anything else
                # disabled it.
                self._real.set_debuglevel(1)
                print()
                print("=" * 78)
                print("SMTP CONVERSATION (debuglevel=1)")
                print("=" * 78)
                return self._real.send_message(m, to_addrs=to_addrs)
        return DebugSMTP

    with patch(
        "app.services.notifications.channels.smtp_email_channel.smtplib.SMTP",
        new=make_debug_smtp(),
    ):
        # Redirect stderr to capture debuglevel stream
        old_stderr = sys.stderr
        sys.stderr = captured_debug
        try:
            result = channel.send(request, rendered)
        finally:
            sys.stderr = old_stderr

    # Print the captured SMTP conversation (server banner + responses)
    smtp_log = captured_debug.getvalue()
    print(smtp_log if smtp_log else "(debuglevel produced no output)")
    print()

    print("=" * 78)
    print("RAW RESULT (DeliveryResult attributes)")
    print("=" * 78)
    for attr in (
        "success", "channel", "recipients",
        "delivered_at", "latency_ms",
        "provider_message_id", "error_code", "error_detail",
    ):
        val = getattr(result, attr)
        print(f"  {attr:22s} = {val!r}")
    print()

    print("=" * 78)
    print("RAW RESULT (delivery result string repr)")
    print("=" * 78)
    print(repr(result))

    return 0


if __name__ == "__main__":
    sys.exit(main())