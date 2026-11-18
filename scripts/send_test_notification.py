#!/usr/bin/env python3
"""Send a single notification email for a given circular UUID, including its AI summary.

Usage:
    python -m scripts.send_test_notification <UUID> <email> [<email> ...]

Looks up the circular in the DB, fetches the summary from S3 (if available),
renders markdown -> HTML, and sends one notification to the given recipients
via the NotificationService (SMTP or splitter backend based on EMAIL_BACKEND).
"""
from __future__ import annotations

import logging
import sys
from uuid import uuid4

import markdown as md

from app.notifications.service.make_service import get_notification_service
from app.notifications.service.models import NotificationRequest
from config import Config
from db.postgres_client import get_postgres_client
from ingestion.repository.circular_repository import CircularRepository
from ingestion.repository.summary_repository import SummaryRepository

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def main(uuid_str: str, recipients: tuple[str, ...]) -> int:
    db_pool = get_postgres_client().get_pool()
    repo = CircularRepository(db_pool)
    record = repo.get_record_by_id(uuid_str)
    if record is None:
        log.error("No circular found for UUID=%s", uuid_str)
        return 1

    # Fetch summary text from S3 (returns None if no summary row / download fails)
    summary_repo = SummaryRepository(db_pool)
    summary_text = None
    try:
        summary_text = summary_repo.get_summary_text(record.id)
    except Exception:
        log.exception("Failed to download summary for circular_id=%s", record.id)

    if summary_text and summary_text.strip():
        summary_html = md.markdown(summary_text, extensions=["extra", "sane_lists"])
        summary_pending = False
        log.info("Summary present: %d chars", len(summary_text))
    else:
        summary_html = None
        summary_pending = True
        log.info("Summary missing — email will show 'pending' fallback")

    variables = {
        "full_reference": record.full_reference,
        "circular_uuid": str(record.id),
        "title": record.title,
        "department": record.department,
        "issue_date": str(record.issue_date) if record.issue_date else "",
        "source": record.source,
        "url": f"{Config.FRONTEND_BASE_URL}/circular/{record.id}",
        "applicable_to_nse": bool(getattr(record, "applicable_to_nse", False)),
        "summary_html": summary_html,
        "summary_pending": summary_pending,
    }

    service = get_notification_service()
    channel = service.supported_channels()[0]
    request = NotificationRequest(
        channel=channel,
        recipients=recipients,
        template_name="circular_notification.html",
        subject=f"CircularHub: {record.full_reference}",
        variables=variables,
        correlation_id=f"manual-test:{uuid4()}",
    )
    result = service.send(request)
    log.info(
        "Send result: success=%s recipients=%s provider_msg_id=%s error=%s",
        result.success,
        result.recipients,
        result.provider_message_id,
        result.error_detail,
    )
    return 0 if result.success else 1


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python -m scripts.send_test_notification <UUID> <email> [<email> ...]")
        sys.exit(2)
    sys.exit(main(sys.argv[1], tuple(sys.argv[2:])))