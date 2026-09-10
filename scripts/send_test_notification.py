#!/usr/bin/env python3
"""Send a single notification email for a given circular UUID, including its AI summary.

Usage:
    python -m scripts.send_test_notification <UUID>

Looks up the circular in the DB, fetches the summary from S3 (if available),
renders markdown -> HTML, and sends one BCC email via EmailService.
"""
from __future__ import annotations

import logging
import sys

import markdown as md

from config import Config
from db.postgres_client import get_postgres_client
from ingestion.repository.circular_repository import CircularRepository
from ingestion.repository.summary_repository import SummaryRepository
from services.notification_service import EmailService

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def main(uuid_str: str) -> int:
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

    data = {
        "circular_id": record.full_reference,
        "title": record.title,
        "department": record.department,
        "issue_date": str(record.issue_date) if record.issue_date else "",
        "source": record.source,
        "url": f"{Config.FRONTEND_BASE_URL}/circular/{record.id}",
        "pdf_url": record.pdf_url,
        "applicable_to_nse": bool(getattr(record, "applicable_to_nse", False)),
        "summary_html": summary_html,
        "summary_pending": summary_pending,
    }

    service = EmailService()
    success, error = service.send_notification_for_circular_to_all_bcc(
        str(record.id), data
    )
    log.info("Send result: success=%s error=%s", success, error)
    return 0 if success else 1


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m scripts.send_test_notification <UUID>")
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
