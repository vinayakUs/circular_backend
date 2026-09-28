"""NotificationLogger — the Strategy interface for recording delivery outcomes.

A NotificationLogger receives every DeliveryResult (success and failure)
and persists a record. It is a side-effect sink; it MUST NOT influence
the result returned to the caller.

CONTRACT:
    1. record() NEVER raises. Any internal exception is swallowed and
       logged at WARN — the delivery result already exists in the
       caller's hands; failing the whole pipeline because the audit
       table is unreachable is worse than losing the audit row.
    2. record() returns None.

Eg: LoginAuditNotificationLogger (writes login_audit rows).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping

from app.services.notifications.models import DeliveryResult


class NotificationLogger(ABC):
    """Strategy interface for recording delivery outcomes."""

    @abstractmethod
    def record(
        self,
        result: DeliveryResult,
        correlation_id: str | None,
        *,
        template_name: str | None = None,
        subject: str | None = None,
        variables: Mapping[str, Any] | None = None,
    ) -> None:
        """Record the outcome of a delivery attempt.

        MUST NOT raise. Swallow internal exceptions and log at WARN.

        Args:
            result: the DeliveryResult returned by the channel.
            correlation_id: optional tracing token from the caller
                (e.g. login_audit row id from the MFA flow).
            template_name: name of the template used to render the body
                (for audit / debug context). Optional.
            subject: email subject line. Optional.
            variables: variable dict passed to the renderer. Optional.
        """
        ...
