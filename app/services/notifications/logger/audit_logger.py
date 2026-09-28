"""NotificationLogAuditLogger — writes a notification_logs row per delivery.

The new pipeline's logger interface sees the FINAL DeliveryResult only —
no in-flight PENDING state. So this logger captures the outcome in one
INSERT (via NotificationLogRepository.record_outcome()) instead of the
3-step create_log + mark_sent/mark_failed pattern the legacy EmailService used.

Maps DeliveryResult fields onto the existing notification_logs schema:
    status         → "SENT" / "FAILED"
    recipients     → comma-joined into recipient_email
    error_code     → prefixed error_message ("<code>: <detail>")
    delivered_at   → sent_at (on success only)
    provider_message_id → correlation_id (so splitter-side traces can be joined)

The ABC contract requires record() to NEVER raise. Any DB exception is
swallowed + logged at WARN — failing the audit step must not lose the
delivery result the caller already has in hand.
"""
from __future__ import annotations

import logging
from typing import Any, Mapping

from app.services.notifications.logger.base import NotificationLogger
from app.services.notifications.models import DeliveryResult


logger = logging.getLogger(__name__)


class NotificationLogAuditLogger(NotificationLogger):
    """Persists delivery outcomes to the notification_logs table.

    Args:
        repo: anything with a ``record_outcome(...)`` method matching the
            signature on NotificationLogRepository.
    """

    def __init__(self, repo: Any) -> None:
        self._repo = repo

    def record(
        self,
        result: DeliveryResult,
        correlation_id: str | None,
        *,
        template_name: str | None = None,
        subject: str | None = None,
        variables: Mapping[str, Any] | None = None,
    ) -> None:
        """Map DeliveryResult → notification_logs row. NEVER raises."""
        try:
            status = "SENT" if result.success else "FAILED"
            error_message = self._format_error(result)
            sent_at = result.delivered_at if result.success else None
            recipient_email = ", ".join(result.recipients)
            variables_dict = dict(variables) if variables else {}

            self._repo.record_outcome(
                template_name=template_name,
                recipient_email=recipient_email,
                subject=subject,
                variables=variables_dict,
                status=status,
                error_message=error_message,
                sent_at=sent_at,
                correlation_id=correlation_id,
            )
        except Exception:
            # ABC contract: audit failures must not propagate. The delivery
            # result is already in the caller's hands; raising here would
            # break the 4-step pipeline contract.
            logger.warning(
                "audit logger failed to record delivery "
                "(success=%s error=%s correlation_id=%s); "
                "delivery result was preserved, audit row lost",
                result.success,
                result.error_code,
                correlation_id,
                exc_info=True,
            )

    @staticmethod
    def _format_error(result: DeliveryResult) -> str | None:
        if result.success:
            return None
        code = result.error_code or "unknown_error"
        if result.error_detail:
            return f"{code}: {result.error_detail}"
        return code