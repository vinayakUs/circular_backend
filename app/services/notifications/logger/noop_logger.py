"""NoopNotificationLogger — a NotificationLogger that does nothing.

Used when audit/recovery isn't wired up yet (Phase 6 deferred). The
service still has a logger field, the pipeline still calls it, but no
DB writes happen.

Swappable for NotificationLogAuditLogger later — just change one
line in make_service.py.
"""
from __future__ import annotations

from typing import Any, Mapping

from app.services.notifications.logger.base import NotificationLogger
from app.services.notifications.models import DeliveryResult


class NoopNotificationLogger(NotificationLogger):
    """A logger that silently accepts every result."""

    def record(
        self,
        result: DeliveryResult,
        correlation_id: str | None,
        *,
        template_name: str | None = None,
        subject: str | None = None,
        variables: Mapping[str, Any] | None = None,
    ) -> None:
        """Drop the result on the floor."""
        return None
