"""Send a sample email via the full NotificationService pipeline (SMTP backend)
and print every step: validate → render → dispatch → log.

Run from the project root with the venv active:
    python scripts/sample_notification_service_send.py
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from app.services.notifications.logger.base import NotificationLogger
from app.services.notifications.make_service import make_notification_service
from app.services.notifications.models import (
    DeliveryResult,
    NotificationRequest,
)
from config import Config


# ── A logger that prints every DeliveryResult to stdout ────────────────


class StdoutNotificationLogger(NotificationLogger):
    """Drop-in for NoopNotificationLogger that prints to stdout so we can see
    the LOG step in the pipeline output."""

    def record(self, result: DeliveryResult, correlation_id: str | None) -> None:
        print()
        print("=" * 72)
        print("LOG step (NotificationLogger.record)")
        print("=" * 72)
        print(f"  correlation_id : {correlation_id}")
        print(f"  success        : {result.success}")
        print(f"  recipients     : {result.recipients}")
        print(f"  channel        : {result.channel}")
        print(f"  error_code     : {result.error_code}")
        print(f"  provider_msg_id: {result.provider_message_id}")


# ── Wire-capture wrapper for smtplib.SMTP ──────────────────────────────


def make_logging_smtp(real_cls):
    class LoggingSMTP:
        def __init__(self, host, port, timeout=None, **kw):
            print()
            print("=" * 72)
            print("DISPATCH step — SMTP session opens")
            print("=" * 72)
            print(f"  [connect] {host}:{port} timeout={timeout}")
            self._real = real_cls(host, port, timeout=timeout, **kw)

        def __getattr__(self, name):
            return getattr(self._real, name)

        def starttls(self, *a, **kw):
            print("  [starttls]")
            return self._real.starttls(*a, **kw)

        def login(self, user, password):
            print(f"  [login] user={user!r} password=<redacted len={len(password)}>")
            return self._real.login(user, password)

        def send_message(self, msg, to_addrs=None):
            print()
            print("  --- MIME message (sent to SMTP server) ---")
            print(f"  From    : {msg['From']}")
            print(f"  To      : {msg['To']}")
            print(f"  Subject : {msg['Subject']}")
            if msg.get("X-Correlation-ID"):
                print(f"  X-Correlation-ID : {msg['X-Correlation-ID']}")
            print(f"  to_addrs: {list(to_addrs) if to_addrs else None}")
            print()
            print("  --- text/plain part ---")
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    print(part.get_payload(decode=True).decode("utf-8", "replace"))
                    break
            print()
            print("  --- text/html part ---")
            for part in msg.walk():
                if part.get_content_type() == "text/html":
                    print(part.get_payload(decode=True).decode("utf-8", "replace"))
                    break
            print()
            print("  [send_message] dispatching ...")
            refused = self._real.send_message(msg, to_addrs=to_addrs)
            print(f"  [send_message] returned refused={refused!r}")
            return refused

        def quit(self):
            print("  [quit]")
            return self._real.quit()

    return LoggingSMTP


# ── Build the service and run it ────────────────────────────────────────


def send_and_inspect(recipients: tuple[str, ...]) -> DeliveryResult:
    # Use the composition root so we exercise the same wiring the app uses.
    # EMAIL_BACKEND defaults to "smtp" → SmtpEmailChannel is selected.
    service = make_notification_service()
    # Replace the noop logger with one that prints results.
    service._notification_logger = StdoutNotificationLogger()

    # Pick the channel name the composite actually registered. The name
    # is "smtp_email" or "splitter_email" depending on EMAIL_BACKEND.
    registered = service.supported_channels()
    channel_name = registered[0] if registered else "smtp_email"

    request = NotificationRequest(
        channel=channel_name,  # whatever the composite registered
        recipients=recipients,
        template_name="sample_email.html",
        subject="Sample from NotificationService (SMTP)",
        correlation_id=f"sample-{datetime.now(timezone.utc).isoformat()}",
        variables={"name": "Tester"},
    )

    print("=" * 72)
    print("Backend selection")
    print("=" * 72)
    print(f"  EMAIL_BACKEND           : {Config.EMAIL_BACKEND}")
    print(f"  registered channels : {service.supported_channels()}")
    print(f"  using channel           : {channel_name}")

    print()
    print("=" * 72)
    print("NotificationRequest")
    print("=" * 72)
    print(f"  channel        : {request.channel}")
    print(f"  recipients     : {request.recipients}")
    print(f"  template_name  : {request.template_name}")
    print(f"  subject        : {request.subject}")
    print(f"  correlation_id : {request.correlation_id}")

    with patch(
        "app.services.notifications.channels.smtp_email_channel.smtplib.SMTP",
        new=make_logging_smtp(__import__("smtplib").SMTP),
    ):
        # Run the full pipeline: validate → render → dispatch → log
        result = service.send(request)

    print()
    print("=" * 72)
    print("DeliveryResult (returned by NotificationService.send)")
    print("=" * 72)
    print(f"  success             : {result.success}")
    print(f"  channel             : {result.channel}")
    print(f"  recipients          : {result.recipients}")
    print(f"  delivered_at        : {result.delivered_at.isoformat()}")
    print(f"  latency_ms          : {result.latency_ms}")
    print(f"  provider_message_id : {result.provider_message_id}")
    print(f"  error_code          : {result.error_code}")
    print(f"  error_detail        : {result.error_detail}")

    return result


if __name__ == "__main__":
    RECIPIENTS = (
        "vinayak200054321@gmail.com",
        "radhikaandrew12@gmail.com",
    )
    send_and_inspect(RECIPIENTS)