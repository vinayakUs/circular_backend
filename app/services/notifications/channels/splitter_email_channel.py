"""SplitterEmailChannel — delivers email via the org's notification splitter.

POSTs to the v3 endpoint with multipart/form-data. The splitter is the
production email gateway; SmtpEmailChannel (direct smtplib) is for dev/
fallback.

Per the splitter docs (v3):
    Method:   POST
    Headers:  Content-Type: multipart/form-data; boundary=...
              Authorization: <token>
    Form fields:
        from             (mandatory, string)   sender email
        to               (mandatory, list)     recipient(s)
        subject          (mandatory, string)
        html             (mandatory, string)
        appName          (mandatory, string)   APP-PNEUMONIC
    Response (200):
        { statusCode, message, data: { transactionID, submittedTime } }

Every transport failure maps to a stable error_code string so callers
can branch on the failure mode without parsing free-text.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone

import requests

from app.services.notifications.channels.base import NotificationChannel
from app.services.notifications.models import (
    DeliveryResult,
    NotificationRequest,
    RenderedPayload,
)


logger = logging.getLogger(__name__)


EMAIL_REGEX = re.compile(
    r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
)


class SplitterEmailChannel(NotificationChannel):
    """Delivers rendered payloads as email via the org's notification splitter.

    Args:
        endpoint:        Full URL of the v3 send endpoint.
        auth_token:      Value for the Authorization header.
        app_name:        APP-PNEUMONIC registered with the splitter.
        from_email:      Default sender email (request.from_email overrides if set).
        timeout_seconds: HTTP timeout per request.
    """

    def __init__(
        self,
        *,
        endpoint: str,
        auth_token: str,
        app_name: str,
        from_email: str,
        timeout_seconds: float = 10.0,
    ) -> None:
        self._endpoint = endpoint
        self._auth_token = auth_token
        self._app_name = app_name
        self._from_email = from_email
        self._timeout = timeout_seconds

    def supports(self, recipient: str) -> bool:
        return bool(recipient and EMAIL_REGEX.match(recipient))

    def send(
        self,
        request: NotificationRequest,
        rendered: RenderedPayload,
    ) -> DeliveryResult:
        started = datetime.now(timezone.utc)
        subject = request.subject or rendered.subject
        # Splitter expects HTML. Fall back to text wrapped in <pre> if no html.
        html_body = rendered.body_html or f"<pre>{rendered.body_text}</pre>"

        # Splitter v3 requires multipart/form-data (per the v3 API docs).
        # requests.post(url, data=dict) defaults to application/x-www-form-urlencoded,
        # which the splitter accepts on intake but does not deliver — it returns a
        # transactionID but the email never lands. curl works because -F forces
        # multipart. To match curl on the wire, build a list of
        # (field_name, (None, value)) tuples and pass via files=; the (None, value)
        # shape signals a regular form field (not a file upload) and requests sets
        # Content-Type: multipart/form-data with a generated boundary.
        multipart_payload: list[tuple[str, tuple[None, str]]] = [
            ("from",            (None, self._from_email)),
            ("subject",         (None, subject)),
            ("html",            (None, html_body, "text/html; charset=utf-8")),
            ("appName",         (None, self._app_name)),
        ]
        # v3 expects 'to' as a list — repeat the field once per recipient so the
        # splitter parses them as a true list (form-encoded collapses to ambiguous).
        multipart_payload += [("to", (None, r)) for r in request.recipients]
        if request.cc:
            multipart_payload += [("cc", (None, r)) for r in request.cc]
        if request.bcc:
            multipart_payload += [("bcc", (None, r)) for r in request.bcc]
        # If the caller set correlation_id, include it for splitter-side tracing.
        # if request.correlation_id:
        #     multipart_payload.append(("transactionId", (None, request.correlation_id)))

        headers = {
            "Authorization": self._auth_token,
            # files= triggers requests to set Content-Type: multipart/form-data
            # with a generated boundary. Do not override it manually.
        }

        try:
            response = requests.post(
                self._endpoint,
                files=multipart_payload,
                headers=headers,
                timeout=self._timeout,
                verify=False
            )
        except requests.exceptions.Timeout as e:
            return self._fail(request, started, "splitter_timeout", str(e))
        except requests.exceptions.ConnectionError as e:
            return self._fail(request, started, "splitter_connection", str(e))
        except requests.exceptions.RequestException as e:
            return self._fail(
                request, started, "splitter_network", type(e).__name__
            )
        except Exception as e:
            logger.exception(
                "unexpected exception in SplitterEmailChannel.send for %s",
                request.recipients,
            )
            return self._fail(request, started, "splitter_unknown", type(e).__name__)

        # Classify by HTTP status.
        if not (200 <= response.status_code < 300):
            return self._fail(
                request,
                started,
                f"splitter_{response.status_code}",
                response.text[:500] if response.text else "(empty body)",
            )

        # Parse JSON for transactionID.
        try:
            body = response.json()
        except (json.JSONDecodeError, ValueError) as e:
            return self._fail(
                request, started, "splitter_bad_response", f"JSON parse: {e}"
            )

        # v3 returns { statusCode, message, data: { transactionID, ... } }
        transaction_id = None
        if isinstance(body, dict):
            data = body.get("data") or {}
            if isinstance(data, dict):
                transaction_id = data.get("transactionID") or data.get("transactionId")

        # statusCode field in body is informational; if it's not 200, surface it.
        if isinstance(body, dict):
            inner_code = body.get("statusCode")
            if inner_code is not None and inner_code != 200:
                return self._fail(
                    request,
                    started,
                    f"splitter_body_{inner_code}",
                    body.get("message", "(no message)"),
                    provider_message_id=transaction_id,
                )

        ended = datetime.now(timezone.utc)
        latency_ms = int((ended - started).total_seconds() * 1000)
        return DeliveryResult.ok(
            channel="email",
            recipients=request.recipients,
            delivered_at=ended,
            latency_ms=latency_ms,
            provider_message_id=transaction_id,
        )

    def _fail(
        self,
        request: NotificationRequest,
        started: datetime,
        error_code: str,
        error_detail: str,
        provider_message_id: str | None = None,
    ) -> DeliveryResult:
        ended = datetime.now(timezone.utc)
        latency_ms = int((ended - started).total_seconds() * 1000)
        return DeliveryResult.fail(
            channel="email",
            recipients=request.recipients,
            error_code=error_code,
            error_detail=error_detail,
            delivered_at=ended,
            latency_ms=latency_ms,
        )
 