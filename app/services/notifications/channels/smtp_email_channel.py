"""SmtpEmailChannel — delivers email via SMTP.

Owns all SMTP transport code (smtplib) directly. No separate transport/
sub-package — the channel is the unit that knows both delivery semantics
("this is an email") and the wire details ("how to talk to SMTP").

CONTRACT (per NotificationChannel ABC):
    supports(r)  → True iff r matches the email regex.
    send(req, rendered) → NEVER raises; always returns DeliveryResult.

Every smtplib exception is mapped to a stable error_code string so
callers can pattern-match on the failure mode (retry, alert, fall
back, etc.) without parsing free-text error messages.
"""
from __future__ import annotations

import logging
import re
import smtplib
import ssl
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr

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


class SmtpEmailChannel(NotificationChannel):
    """Delivers rendered payloads as email via SMTP.

    Configuration matches the existing SMTP_* env vars in config.py.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        username: str | None,
        password: str | None,
        use_tls: bool,
        from_email: str,
        from_name: str,
        timeout_seconds: float = 5.0,
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._use_tls = use_tls
        self._from_email = from_email
        self._from_name = from_name
        self._timeout = timeout_seconds

    def supports(self, recipient: str) -> bool:
        return bool(recipient and EMAIL_REGEX.match(recipient))

    def send(
        self,
        request: NotificationRequest,
        rendered: RenderedPayload,
    ) -> DeliveryResult:
        msg = self._build_message(request, rendered)
        started = datetime.now(timezone.utc)

        server: smtplib.SMTP | None = None
        try:
            server = smtplib.SMTP(self._host, self._port, timeout=self._timeout)
            server.set_debuglevel(0)

            if self._use_tls:
                server.starttls(context=ssl.create_default_context())

            if self._username and self._password:
                server.login(self._username, self._password)

            refused = server.send_message(msg)
            ended = datetime.now(timezone.utc)
            latency_ms = int((ended - started).total_seconds() * 1000)

            if refused:
                return DeliveryResult.fail(
                    channel="email",
                    recipient=request.recipient,
                    error_code="smtp_recipient_refused",
                    error_detail=str(refused),
                    delivered_at=ended,
                    latency_ms=latency_ms,
                )

            # server.send_message doesn't return a message-id; construct a
            # synthetic one from the Message-Id header if present, else None.
            provider_id = msg.get("Message-Id")
            return DeliveryResult.ok(
                channel="email",
                recipient=request.recipient,
                delivered_at=ended,
                latency_ms=latency_ms,
                provider_message_id=provider_id,
            )

        except smtplib.SMTPAuthenticationError as e:
            return self._fail(request, started, "smtp_auth", str(e))
        except smtplib.SMTPRecipientsRefused as e:
            return self._fail(
                request, started, "smtp_recipient_refused",
                f"recipients={e.recipients}",
            )
        except smtplib.SMTPSenderRefused as e:
            return self._fail(
                request, started, "smtp_sender_refused",
                f"sender={e.sender}; code={e.smtp_code}; {e.smtp_error!r}",
            )
        except smtplib.SMTPDataError as e:
            return self._fail(
                request, started, "smtp_data_error",
                f"code={e.smtp_code}; {e.smtp_error!r}",
            )
        except smtplib.SMTPConnectError as e:
            return self._fail(request, started, "smtp_connect", str(e))
        except smtplib.SMTPHeloError as e:
            return self._fail(request, started, "smtp_helo", str(e))
        except smtplib.SMTPServerDisconnected as e:
            return self._fail(request, started, "smtp_disconnected", str(e))
        except smtplib.SMTPException as e:
            return self._fail(request, started, "smtp_error", type(e).__name__)
        except (TimeoutError, OSError) as e:
            return self._fail(request, started, "smtp_network", type(e).__name__)
        except Exception as e:
            # Last-resort catch — channels MUST NOT raise. Log at ERROR so
            # the unexpected exception is visible to ops.
            logger.exception(
                "unexpected exception in SmtpEmailChannel.send for %s",
                request.recipient,
            )
            return self._fail(request, started, "smtp_unknown", type(e).__name__)
        finally:
            if server is not None:
                try:
                    server.quit()
                except Exception:
                    logger.debug("smtp quit() failed (ignored)", exc_info=True)

    def _build_message(
        self,
        request: NotificationRequest,
        rendered: RenderedPayload,
    ) -> MIMEMultipart:
        msg = MIMEMultipart("alternative")
        msg["From"] = formataddr((self._from_name, self._from_email))
        msg["To"] = request.recipient
        msg["Subject"] = request.subject or rendered.subject
        if request.correlation_id:
            msg["X-Correlation-ID"] = request.correlation_id

        # Attach text first (per RFC 2046: prefer last → least preferred).
        # Clients that only render the first part get the text version.
        msg.attach(MIMEText(rendered.body_text, "plain", "utf-8"))
        if rendered.body_html:
            msg.attach(MIMEText(rendered.body_html, "html", "utf-8"))
        return msg

    def _fail(
        self,
        request: NotificationRequest,
        started: datetime,
        error_code: str,
        error_detail: str,
    ) -> DeliveryResult:
        ended = datetime.now(timezone.utc)
        latency_ms = int((ended - started).total_seconds() * 1000)
        return DeliveryResult.fail(
            channel="email",
            recipient=request.recipient,
            error_code=error_code,
            error_detail=error_detail,
            delivered_at=ended,
            latency_ms=latency_ms,
        )
