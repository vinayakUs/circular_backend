"""Send a sample email via the splitter and print full request/response details.

Run from the project root with the venv active:
    python scripts/sample_splitter_send.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from unittest.mock import patch

from app.services.notifications.channels.splitter_email_channel import (
    SplitterEmailChannel,
)
from app.services.notifications.models import (
    DeliveryResult,
    NotificationRequest,
    RenderedPayload,
)
from config import Config


def build_channel() -> SplitterEmailChannel:
    return SplitterEmailChannel(
        endpoint=Config.INTERNAL_SPLITTER_EMAIL_URL,
        auth_token=Config.INTERNAL_SPLITTER_AUTH_TOKEN,
        app_name=Config.INTERNAL_SPLITTER_APP_NAME,
        service_provider=Config.INTERNAL_SPLITTER_SERVICE_PROVIDER,
        from_email=Config.SMTP_FROM_EMAIL,
        timeout_seconds=Config.INTERNAL_SPLITTER_TIMEOUT_SECONDS,
    )


def send_and_inspect(recipients: tuple[str, ...]) -> DeliveryResult:
    channel = build_channel()

    request = NotificationRequest(
        channel="email",
        recipients=recipients,
        template_name="sample_email.html",
        subject="Sample from CircularHub splitter",
        correlation_id=f"sample-{datetime.now(timezone.utc).isoformat()}",
        variables={"name": "Tester"},
    )
    rendered = RenderedPayload(
        body_text="Hello — this is a sample email from the splitter integration.",
        body_html=(
            "<h1>Hello</h1>"
            "<p>This is a <b>sample email</b> from the CircularHub splitter integration.</p>"
            "<p>If you received this, the splitter v3 integration is working.</p>"
        ),
    )

    print("=" * 72)
    print("NotificationRequest")
    print("=" * 72)
    print(f"  channel        : {request.channel}")
    print(f"  recipients     : {request.recipients}")
    print(f"  template_name  : {request.template_name}")
    print(f"  subject        : {request.subject}")
    print(f"  correlation_id : {request.correlation_id}")

    real_post = __import__("requests").post

    def logging_post(url, *, data, headers, timeout):
        print()
        print("=" * 72)
        print("Wire payload (sent to splitter)")
        print("=" * 72)
        print(f"  URL     : {url}")
        print(f"  Headers : {headers}")
        print(f"  Timeout : {timeout}s")
        print("  Form fields:")
        for k, v in data.items():
            display = v if not isinstance(v, list) else f"{v}  (count={len(v)})"
            print(f"    {k!r:22s} = {display!r}")
        print()
        print("=" * 72)
        print("Awaiting splitter response ...")
        print("=" * 72)
        resp = real_post(url, data=data, headers=headers, timeout=timeout)
        print(f"  HTTP status  : {resp.status_code}")
        print(f"  Content-Type : {resp.headers.get('Content-Type')}")
        try:
            body = resp.json()
            print(f"  Body (JSON) :")
            print(json.dumps(body, indent=4))
        except Exception:
            print(f"  Body (raw)  : {resp.text[:500]}")
        return resp

    with patch(
        "app.services.notifications.channels.splitter_email_channel.requests.post",
        side_effect=logging_post,
    ):
        result = channel.send(request, rendered)

    print()
    print("=" * 72)
    print("DeliveryResult")
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