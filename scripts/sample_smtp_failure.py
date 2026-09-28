"""Show raw data for a FAILING SMTP send — wrong password → smtp_auth error.

Run from the project root with the venv active:
    python scripts/sample_smtp_failure.py
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


def main() -> int:
    # Use the same config but force a wrong password to trigger smtp_auth.
    channel = SmtpEmailChannel(
        host=Config.SMTP_HOST,
        port=Config.SMTP_PORT,
        username=Config.SMTP_USERNAME or None,
        password="WRONG-PASSWORD-ON-PURPOSE",  # triggers SMTP AUTH error
        use_tls=Config.SMTP_USE_TLS,
        from_email=Config.SMTP_FROM_EMAIL,
        from_name=Config.SMTP_FROM_NAME,
    )

    request = NotificationRequest(
        channel="smtp_email",
        recipients=(
            "vinayak200054321@gmail.com",
            "radhikaandrew12@gmail.com",
        ),
        template_name="sample_email.html",
        subject="Sample — failure path",
        correlation_id="sample-fail-001",
        variables={"name": "Tester"},
    )
    rendered = RenderedPayload(
        body_text="This will not be sent.",
        body_html="<p>This will not be sent.</p>",
    )

    print("=" * 78)
    print("RAW REQUEST (MIME bytes — same shape as success path)")
    print("=" * 78)
    msg = channel._build_message(request, rendered)
    print(msg.as_bytes().decode("utf-8", "replace"))
    print()

    captured_debug = StringIO()
    real_smtp = __import__("smtplib").SMTP

    def make_debug_smtp():
        class DebugSMTP:
            def __init__(self, host, port, timeout=None, **kw):
                self._real = real_smtp(host, port, timeout=timeout, **kw)
                self._real.set_debuglevel(1)

            def __getattr__(self, name):
                return getattr(self._real, name)

            def set_debuglevel(self, level):
                # Always force debug ON regardless of channel's set_debuglevel(0).
                return self._real.set_debuglevel(1)

            def send_message(self, m, to_addrs=None):
                self._real.set_debuglevel(1)
                print("=" * 78)
                print("SMTP CONVERSATION (debuglevel=1)")
                print("=" * 78)
                return self._real.send_message(m, to_addrs=to_addrs)
        return DebugSMTP

    with patch(
        "app.services.notifications.channels.smtp_email_channel.smtplib.SMTP",
        new=make_debug_smtp(),
    ):
        old_stderr = sys.stderr
        sys.stderr = captured_debug
        try:
            result = channel.send(request, rendered)
        finally:
            sys.stderr = old_stderr

    smtp_log = captured_debug.getvalue()
    print(smtp_log if smtp_log else "(no SMTP conversation captured)")
    print()

    print("=" * 78)
    print("RAW RESULT (DeliveryResult on failure)")
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
    print("RAW RESULT (full repr)")
    print("=" * 78)
    print(repr(result))

    return 0


if __name__ == "__main__":
    sys.exit(main())