"""Value objects for the notification subsystem.

These are the data structures that flow across the public API boundary:

Frozen dataclasses with `slots` — immutable, hashable, memory-efficient.
No behavior lives here; construction and validation only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Mapping


Channel = Literal["email", "sms"]
"""Supported delivery channels. Add values here when new channels are introduced."""



@dataclass(frozen=True, slots=True)
class NotificationRequest:
    """What a caller hands to NotificationService.send().

    Immutable so callers cannot mutate a request mid-flight between the
    validate → render → dispatch → log pipeline.
    """
    channel: Channel
    recipient: str
    template_name: str
    variables: Mapping[str, Any] = field(default_factory=dict)
    subject: str | None = None
    correlation_id: str | None = None

    def __post_init__(self) -> None:
        if not self.channel:
            raise ValueError("channel is required")
        if not self.recipient:
            raise ValueError("recipient is required")
        if not self.template_name:
            raise ValueError("template_name is required")


@dataclass(frozen=True, slots=True)
class RenderedPayload:
    """Output of a renderer; input to a channel.

    Channels don't know about templates; they consume this fully-prepared
    payload. Subject may be None for SMS; body_html may be None for SMS
    or for text-only email templates.
    """
    body_text: str
    body_html: str | None = None
    subject: str | None = None

    def __post_init__(self) -> None:
        if not self.body_text and not self.body_html:
            raise ValueError(
                "RenderedPayload must have at least one of body_text or body_html"
            )
        

@dataclass(frozen=True, slots=True)
class DeliveryResult:
    """What a channel returns and what NotificationService.send() returns.

    NO EXCEPTIONS cross this boundary. Every failure mode — transport error,
    auth failure, recipient refused, timeout, unknown channel — is encoded
    as success=False plus an error_code string.

    Callers pattern-match on (success, error_code) instead of try/except.
    """
    success: bool
    channel: str
    recipient: str
    delivered_at: datetime
    latency_ms: int
    provider_message_id: str | None = None
    error_code: str | None = None
    error_detail: str | None = None


    def __post_init__(self) -> None:
        if self.success and self.error_code is not None:
            raise ValueError(
                "DeliveryResult cannot have both success=True and an error_code"
            )
        if not self.success and self.error_code is None:
            raise ValueError(
                "DeliveryResult with success=False must have an error_code"
            )
        if self.latency_ms < 0:
            raise ValueError("latency_ms must be non-negative")

    @classmethod
    def ok(
        cls,
        *,
        channel: str,
        recipient: str,
        delivered_at: datetime,
        latency_ms: int,
        provider_message_id: str | None = None,
    ) -> "DeliveryResult":
        """Build a success result. Use this rather than the constructor directly
        so success=True is enforced."""
        return cls(
            success=True,
            channel=channel,
            recipient=recipient,
            delivered_at=delivered_at,
            latency_ms=latency_ms,
            provider_message_id=provider_message_id,
        )

    @classmethod
    def fail(
        cls,
        *,
        channel: str,
        recipient: str,
        error_code: str,
        error_detail: str | None = None,
        delivered_at: datetime,
        latency_ms: int = 0,
    ) -> "DeliveryResult":
        """Build a failure result. Use this rather than the constructor directly
        so error_code is enforced when success=False."""
        return cls(
            success=False,
            channel=channel,
            recipient=recipient,
            delivered_at=delivered_at,
            latency_ms=latency_ms,
            error_code=error_code,
            error_detail=error_detail,
        )
