"""NotificationChannel — the Strategy interface for delivery transports.

A NotificationChannel knows how to deliver a fully-rendered message to a
recipient. It does NOT know about templates, variables, or rendering —
those concerns belong to the renderer.

CONTRACT (enforced by all implementations):
    1. supports(recipient) returns True iff this channel can handle the
       given recipient address shape (email regex for SMTP, E.164 for SMS).
    2. send(request, rendered) NEVER raises. Every failure mode is encoded
       as DeliveryResult(success=False, error_code=...).
    3. send() always returns a DeliveryResult — never None.
    4. send() is responsible for stamping delivered_at and latency_ms.

Gets a typed result back. No try/except.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from app.services.notifications.models import (
    DeliveryResult,
    NotificationRequest,
    RenderedPayload,
)

class NotificationChannel(ABC):
    """Strategy interface for delivering a rendered message."""

    @abstractmethod
    def supports(self, recipient: str) -> bool:
        """Return True iff this channel can handle the given recipient.

        Examples:
          SmtpEmailChannel.supports("alice@x.com")  → True
          SmtpEmailChannel.supports("+91xxxxxxxxxx") → False
          SplitterSmsChannel.supports("+91xxxxxxxxxx") → True
          SplitterSmsChannel.supports("alice@x.com")  → False
        """
        ...

    @abstractmethod
    def send(
        self,
        request: NotificationRequest,
        rendered: RenderedPayload,
    ) -> DeliveryResult:
        """Deliver the rendered payload to the request's recipient.

        MUST NOT raise. MUST return a DeliveryResult.

        Implementations are responsible for:
          - building the protocol-specific message (MIME, JSON, etc.)
          - talking to the transport (smtplib, requests, etc.)
          - mapping every transport-level exception to a DeliveryResult.fail
            with a stable error_code string
          - stamping delivered_at and latency_ms on the returned result
        """
        ...
