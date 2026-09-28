"""CompositeNotificationChannel — a NotificationChannel made of channels.

Holds a named registry of child channels and delegates each request to
the child whose name matches request.channel. Implements the same
NotificationChannel ABC so callers (NotificationService) treat it as
a single channel — they don't know or care how many children exist.

Why a composite (not a dict passed to the service):
    The service holds ONE NotificationChannel field. The composite IS
    that field. The service calls .supports() and .send() polymorphically.
    It never iterates children, never knows names exist, never branches
    on "email vs sms". All the lookup logic lives here.
"""
from __future__ import annotations

import logging
from typing import Mapping

from app.services.notifications.channels.base import NotificationChannel
from app.services.notifications.models import (
    DeliveryResult,
    NotificationRequest,
    RenderedPayload,
)


logger = logging.getLogger(__name__)


class CompositeNotificationChannel(NotificationChannel):
    """Registry of named channels. Routes by request.channel."""

    def __init__(
        self,
        channels: Mapping[str, NotificationChannel] | None = None,
    ) -> None:
        self._channels: dict[str, NotificationChannel] = dict(channels or {})

    def register(self, name: str, channel: NotificationChannel) -> None:
        """Add or replace a child channel by name."""
        if not name:
            raise ValueError("channel name must be non-empty")
        self._channels[name] = channel

    def resolve(self, name: str) -> NotificationChannel | None:
        """Return the child channel registered under `name`, or None."""
        return self._channels.get(name)

    def supported_channels(self) -> list[str]:
        """Return the names of all registered child channels."""
        return list(self._channels.keys())

    def supports(self, recipient: str) -> bool:
        """True iff any registered child claims to support the recipient."""
        return any(ch.supports(recipient) for ch in self._channels.values())

    def send(
        self,
        request: NotificationRequest,
        rendered: RenderedPayload,
    ) -> DeliveryResult:
        """Route to the child whose name matches request.channel.

        If no child is registered for that name, returns a structured
        DeliveryResult.fail with error_code='unknown_channel' — never
        raises (per NotificationChannel ABC contract).
        """
        child = self._channels.get(request.channel)
        if child is None:
            logger.warning(
                "unknown channel %r requested by %s; registered channels: %s",
                request.channel,
                request.recipients,
                sorted(self._channels.keys()),
            )
            return DeliveryResult.fail(
                channel=request.channel,
                recipients=request.recipients,
                error_code="unknown_channel",
                error_detail=(
                    f"No channel registered for {request.channel!r}; "
                    f"available: {sorted(self._channels.keys())}"
                ),
                delivered_at=_now(),
                latency_ms=0,
            )
        return child.send(request, rendered)


def _now():
    """Local import to avoid pulling datetime into module top-level."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc)
