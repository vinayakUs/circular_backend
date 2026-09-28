"""NotificationService — orchestrates validate → render → dispatch → log.

Depends ONLY on the three ABCs. Concrete implementations (SMTP channel,
Jinja renderer, audit logger, etc.) are wired by the composition root
(make_service.py), not here.

This separation is what lets us build and verify the orchestration
logic with fake channels/renderers/loggers before any transport code
exists.
"""

from __future__ import annotations

import dataclasses
import logging
from datetime import datetime, timezone
from typing import Callable, Mapping

from app.services.notifications.channels.base import NotificationChannel
from app.services.notifications.logger.base import NotificationLogger
from app.services.notifications.models import (
    DeliveryResult,
    NotificationRequest,
)
from app.services.notifications.renderers.base import (
    TemplateNotFound,
    TemplateRenderError,
    TemplateRenderer,
)


logger = logging.getLogger(__name__)


class NotificationService:
    """Orchestrates a 4-step pipeline for delivering a notification.

    Pipeline (each step may produce a failure result; the pipeline
    short-circuits on the first failure):

        1. VALIDATE   — does any registered channel support this recipient?
        2. RENDER     — render the template (renderer MAY raise; we catch)
        3. DISPATCH   — hand the rendered payload to the channel
                        (channel MUST NOT raise; returns DeliveryResult)
        4. LOG        — record the outcome (logger MUST NOT raise; we catch)

    Step 4 is best-effort. A logger failure MUST NOT cause a delivery
    result to be lost — the caller already has the result in hand.
    """

    def __init__(
        self,
        *,
        channels: NotificationChannel,
        renderers: Mapping[str, TemplateRenderer],
        notification_logger: NotificationLogger,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not renderers:
            raise ValueError("renderers must contain at least one entry")
        if "default" not in renderers:
            raise ValueError(
                "renderers must contain a 'default' entry "
                "(NotificationRequest does not carry a renderer name)"
            )
        self._channels = channels
        self._renderers = dict(renderers)
        self._notification_logger = notification_logger
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def send(self, request: NotificationRequest) -> DeliveryResult:
        """Run the 4-step pipeline for one request. ALWAYS returns a result."""
        started = self._clock()

        # ── Step 1: VALIDATE ──────────────────────────────────────────
        # composite.supports() returns True iff any child supports.
        # For multi-recipient requests, every recipient must be supported.
        unsupported = [
            r for r in request.recipients
            if not self._channels.supports(r)
        ]
        if unsupported:
            return self._fail(
                request,
                error_code="invalid_recipient",
                error_detail=(
                    f"No registered channel supports {unsupported!r}"
                ),
                started=started,
            )

        # Email channels require a subject. SMS doesn't (body-only).
        # Checked here so we fail fast without rendering or contacting the wire.
        if self._channel_requires_subject(request.channel) and not request.subject:
            return self._fail(
                request,
                error_code="missing_subject",
                error_detail=(
                    f"Channel {request.channel!r} requires a subject "
                    f"(set NotificationRequest.subject)"
                ),
                started=started,
            )

        # ── Step 2: RENDER ────────────────────────────────────────────
        renderer = self._renderers["default"]
        try:
            rendered = renderer.render(request.template_name, request.variables)
        except TemplateNotFound as e:
            return self._fail(
                request,
                error_code="template_not_found",
                error_detail=str(e) or f"No template: {request.template_name}",
                started=started,
            )
        except TemplateRenderError as e:
            return self._fail(
                request,
                error_code="template_render_error",
                error_detail=str(e) or "Template rendering failed",
                started=started,
            )
        # ── Step 3: DISPATCH ──────────────────────────────────────────
        # Channel ABC contract: NEVER raises, ALWAYS returns DeliveryResult.
        # The channel may return ok() or fail() — both shapes are valid.
        channel_result = self._channels.send(request, rendered)

        # Stamp our orchestration timing onto whatever the channel returned.
        # Channel may have set placeholder timing; we override with our own
        # clock-based measurements so latency is consistent across steps.
        ended = self._clock()
        latency_ms = int((ended - started).total_seconds() * 1000)
        result = dataclasses.replace(
            channel_result,
            delivered_at=ended,
            latency_ms=latency_ms,
        )
        # ── Step 4: LOG (best-effort, never fails the pipeline) ───────
        try:
            self._notification_logger.record(
                result,
                request.correlation_id,
                template_name=request.template_name,
                subject=request.subject,
                variables=request.variables,
            )
        except Exception:
            # Swallow + log. The result is already in the caller's hands;
            # failing the whole send because the audit table is unreachable
            # is worse than losing one audit row.
            logger.warning(
                "notification logger failed; "
                "delivery result was success=%s error=%s",
                result.success,
                result.error_code,
                exc_info=True,
            )

        return result

    def supported_channels(self) -> list[str]:
        """Return the names of channels the composite knows about.

        Used by /healthz and admin tooling. Returns a single-element list
        with the class name if the channel isn't a Composite.
        """
        supported = getattr(self._channels, "supported_channels", None)
        if callable(supported):
            return list(supported())
        return [type(self._channels).__name__]

    # Channel-name conventions: any name ending in "_email" requires a
    # subject. SMS channels ("splitter_sms") are body-only and don't.
    _EMAIL_CHANNEL_SUFFIX = "_email"

    def _channel_requires_subject(self, channel_name: str) -> bool:
        """True if the channel name identifies an email channel that needs subject."""
        return channel_name.endswith(self._EMAIL_CHANNEL_SUFFIX)

    def _fail(
        self,
        request: NotificationRequest,
        *,
        error_code: str,
        error_detail: str,
        started: datetime,
    ) -> DeliveryResult:
        """Build a failure DeliveryResult with consistent timing."""
        ended = self._clock()
        latency_ms = int((ended - started).total_seconds() * 1000)
        return DeliveryResult.fail(
            channel=request.channel,
            recipients=request.recipients,
            error_code=error_code,
            error_detail=error_detail,
            delivered_at=ended,
            latency_ms=latency_ms,
        )