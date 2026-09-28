"""Composition root for the notification subsystem.

The ONLY file in this subsystem that imports concrete classes.
Builds and wires every collaborator for NotificationService:

    channels ──▶ CompositeNotificationChannel
                 └─ "splitter_email" → SplitterEmailChannel   (prod, org splitter v3)
                    OR
                 └─ "smtp_email"    → SmtpEmailChannel       (dev/CI, direct SMTP)

    renderers ──▶ {"default": JinjaTemplateRenderer}
    logger    ──▶ NotificationLogAuditLogger                  (writes notification_logs)

Channel naming convention:
    smtp_email      — direct SMTP (Gmail for dev)
    splitter_email  — org notification splitter v3
    splitter_sms    — org notification splitter (when SMS docs land)

Mirrors get_captcha_service() / make_captcha_service() pattern.
"""
from __future__ import annotations

import logging
import threading
from pathlib import Path

from config import Config

# ── Concrete channels (Phase 4) ────────────────────────────────────
from app.services.notifications.channels.composite import (
    CompositeNotificationChannel,
)
from app.services.notifications.channels.smtp_email_channel import (
    SmtpEmailChannel,
)
from app.services.notifications.channels.splitter_email_channel import (
    SplitterEmailChannel,
)

# ── Concrete renderer (Phase 5) ────────────────────────────────────
from app.services.notifications.renderers.jinja_renderer import (
    JinjaTemplateRenderer,
)

# ── Concrete logger (Phase 6 — audit logger wired in) ────────────
from app.services.notifications.logger.audit_logger import (
    NotificationLogAuditLogger,
)
from app.services.notifications.repository.notification_log_repository import (
    NotificationLogRepository,
)
from db.postgres_client import get_postgres_client

# ── Orchestrator (Phase 7) ─────────────────────────────────────────
from app.services.notifications.notification_service import (
    NotificationService,
)


logger = logging.getLogger(__name__)


def make_notification_service() -> NotificationService:
    """Build a fully-wired NotificationService.

    Email backend selected by Config.EMAIL_BACKEND:
        "smtp"     → SmtpEmailChannel     (named "smtp_email" in registry)
        "splitter" → SplitterEmailChannel (named "splitter_email" in registry)

    MFA flow sends via channel="splitter_email" or "smtp_email" depending
    on which backend is active. Same NotificationRequest shape either way.

    Today the channel name IS the backend identifier. The MFA route layer
    (Phase 9) will translate a logical "email" channel into the right
    physical channel based on the user's channel choice.
    """
    if Config.EMAIL_BACKEND == "splitter":
        email_channel = SplitterEmailChannel(
            endpoint=Config.INTERNAL_SPLITTER_EMAIL_URL,
            auth_token=Config.INTERNAL_SPLITTER_AUTH_TOKEN,
            app_name=Config.INTERNAL_SPLITTER_APP_NAME,
            service_provider=Config.INTERNAL_SPLITTER_SERVICE_PROVIDER,
            from_email=Config.SMTP_FROM_EMAIL,
            timeout_seconds=Config.INTERNAL_SPLITTER_TIMEOUT_SECONDS,
        )
        email_channel_name = "splitter_email"
    else:
        # default: SMTP
        email_channel = SmtpEmailChannel(
            host=Config.SMTP_HOST,
            port=Config.SMTP_PORT,
            username=Config.SMTP_USERNAME,
            password=Config.SMTP_PASSWORD,
            use_tls=Config.SMTP_USE_TLS,
            from_email=Config.SMTP_FROM_EMAIL,
            from_name=Config.SMTP_FROM_NAME,
            timeout_seconds=10.0,
        )
        email_channel_name = "smtp_email"

    channels = CompositeNotificationChannel(channels={
        email_channel_name: email_channel,
    })

    # ── Renderers ───────────────────────────────────────────────────
    templates_dir = Path(__file__).parent / "templates"
    renderers = {
        "default": JinjaTemplateRenderer(template_dir=templates_dir),
    }

    # ── Logger ──────────────────────────────────────────────────────
    # DB-backed audit: writes one notification_logs row per delivery.
    # The repo's record_outcome() uses the same notification_logs schema
    # the legacy EmailService populated via create_log + mark_sent/failed.
    log_repo = NotificationLogRepository(
        db_pool=get_postgres_client().get_pool(),
    )
    notification_logger = NotificationLogAuditLogger(repo=log_repo)

    # ── Wire it all together ────────────────────────────────────────
    return NotificationService(
        channels=channels,
        renderers=renderers,
        notification_logger=notification_logger,
    )


# ── Lazy thread-safe singleton ─────────────────────────────────────
_service: NotificationService | None = None
_lock = threading.Lock()


def get_notification_service() -> NotificationService:
    """Lazy thread-safe singleton.

    Mirrors get_captcha_service() pattern. First call builds the service
    (pays channel construction cost); subsequent calls return the cached
    instance.
    """
    global _service
    if _service is None:
        with _lock:
            if _service is None:
                _service = make_notification_service()
                backend = Config.EMAIL_BACKEND
                logger.info(
                    "NotificationService initialized (email_backend=%s)", backend
                )
    return _service
