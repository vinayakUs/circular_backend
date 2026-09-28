"""
Cron-friendly notification worker.

Processes circulars in 'FETCHED' status that have not been notified yet,
and pending @mention notifications, sending each through the new
NotificationService. Exits with a code suitable for cron monitoring.

Run with:  python -m services.notification_worker

Exit codes:
  0  success (including "nothing to do")
  1  at least one notification failed to send
  2  unexpected error (raised exception)

Cron example (every 5 minutes):
  */5 * * * * cd /path/to/circular_backend && /path/to/venv/bin/python -m services.notification_worker >> /var/log/circular_notifications.log 2>&1
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler

import markdown as md

from app.services.notifications.make_service import get_notification_service
from app.services.notifications.models import NotificationRequest
from app.services.notifications.repository.notification_log_repository import (
    NotificationLogRepository,
)
from config import Config
from db.postgres_client import get_postgres_client
from ingestion.repository.circular_repository import CircularRepository
from ingestion.repository.mention_notifications_repository import (
    MentionNotificationsRepository,
)
from ingestion.repository.summary_repository import SummaryRepository


def _setup_logging(log_file: str | None) -> None:
    """Mirror ingestion.processor.runner: stdout always, rotating file if --log-file given.

    Rotates at midnight each day, files named notification.log.2026-07-17, etc.
    backupCount=9999 effectively keeps all files forever.
    """
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Remove any pre-existing handlers (e.g. from logging.basicConfig calls at import time)
    # to avoid duplicate log lines.
    root.handlers.clear()

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    root.addHandler(stdout_handler)

    if log_file:
        file_handler = TimedRotatingFileHandler(
            log_file,
            when="midnight",
            interval=1,
            backupCount=9999,
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)


def _build_subject(source: str | None, full_reference: str) -> str:
    """Build the email subject line from the circular's source and reference."""
    normalized = (source or "").upper()
    if normalized == "SEBI":
        prefix = "SEBI Circular"
    elif normalized == "SEBI_MASTER":
        prefix = "Sebi Master Circular"
    elif normalized == "NSE":
        prefix = "NSE Circular"
    else:
        prefix = "Circular"
    return f"{prefix}: {full_reference}"


def _prepare_pending_circulars(db_pool) -> list[dict]:
    """Query DB + S3 for circulars ready to notify.

    Returns a list of dicts shaped to feed NotificationRequest.variables for
    the circular_notification.html template. Each dict also carries
    `circular_id` (the public reference) and `circular_uuid` (for the
    correlation_id).
    """
    log = logging.getLogger(__name__)
    circular_repo = CircularRepository(db_pool)
    summary_repo = SummaryRepository(db_pool)
    log_repo = NotificationLogRepository(db_pool=db_pool)

    # Get recently fetched circulars (last 24h), already filtered by
    # summary readiness / 10h grace at the SQL level.
    fetched = circular_repo.list_recent_fetched_circulars_for_notification(hours=24)
    log.info("[NOTIFICATION DEBUG] Fetched circulars: %d", len(fetched))
    for f in fetched:
        log.info(
            "[NOTIFICATION DEBUG]   - id=%s full_reference=%s status=%s",
            f.id, f.full_reference, f.status,
        )

    # Filter out already notified.
    circular_ids = [str(r.id) for r in fetched]
    log.info("[NOTIFICATION DEBUG] circular_ids (as strings): %s", circular_ids)
    notified = log_repo.get_notified_circular_ids(circular_ids)
    log.info("[NOTIFICATION DEBUG] Already notified UUIDs: %s", notified)

    grace_hours = Config.SUMMARY_GRACE_PERIOD_HOURS

    result = []
    for record in fetched:
        record_id_str = str(record.id)
        log.info(
            "[NOTIFICATION DEBUG] Checking record.id=%s in notified=%s -> %s",
            record_id_str, notified, record_id_str in notified,
        )
        if record_id_str in notified:
            log.info(
                "[NOTIFICATION DEBUG]   SKIP (already notified): %s",
                record.full_reference,
            )
            continue
        log.info("[NOTIFICATION DEBUG]   INCLUDE: %s", record.full_reference)

        age_hours = (
            datetime.now(timezone.utc) - record.created_at
        ).total_seconds() / 3600.0
        grace_expired = age_hours >= grace_hours

        # Try to fetch the AI summary. Treat missing / empty / failed
        # downloads as "pending" so the template can show the fallback.
        summary_html: str | None = None
        summary_pending = grace_expired
        try:
            summary_text = summary_repo.get_summary_text(record.id)
        except Exception:
            log.exception(
                "Failed to download summary for circular_id=%s", record.id
            )
            summary_text = None

        if summary_text and summary_text.strip():
            summary_html = md.markdown(
                summary_text,
                extensions=["extra", "sane_lists"],
            )
            summary_pending = False
        elif not summary_pending:
            summary_pending = True

        result.append({
            "id": record.id,
            "circular_id": record.full_reference,
            "circular_uuid": str(record.id),
            "title": record.title,
            "department": record.department,
            "issue_date": str(record.issue_date) if record.issue_date else "",
            "source": record.source,
            "url": f"{Config.FRONTEND_BASE_URL}/circular/{record.id}",
            "pdf_url": record.pdf_url,
            "applicable_to_nse": record.applicable_to_nse,
            "summary_html": summary_html,
            "summary_pending": summary_pending,
        })
    log.info(
        "[NOTIFICATION DEBUG] Final pending list: %d circulars", len(result)
    )
    return result


def _send_circular_batch(service, recipients: tuple[str, ...]) -> tuple[int, int]:
    """Send notifications for all pending circulars. Returns (sent, failed)."""
    log = logging.getLogger(__name__)
    if not recipients:
        return (0, 0)

    db_pool = get_postgres_client().get_pool()
    pending = _prepare_pending_circulars(db_pool)
    if not pending:
        return (0, 0)

    channel = service.supported_channels()[0]
    sent = failed = 0
    for c in pending:
        variables = {
            "full_reference": c["circular_id"],
            "circular_uuid": c["circular_uuid"],
            "title": c["title"],
            "department": c["department"],
            "issue_date": c["issue_date"],
            "source": c["source"],
            "url": c["url"],
            "applicable_to_nse": bool(c.get("applicable_to_nse", False)),
            "summary_html": c.get("summary_html"),
            "summary_pending": bool(c.get("summary_pending", False)),
        }
        subject = _build_subject(c["source"], c["circular_id"])
        request = NotificationRequest(
            channel=channel,
            recipients=recipients,
            template_name="circular_notification.html",
            subject=subject,
            variables=variables,
            correlation_id=f"circular:{c['circular_uuid']}",
        )
        result = service.send(request)
        if result.success:
            sent += 1
        else:
            failed += 1
            log.warning(
                "circular delivery failed uuid=%s code=%s detail=%s",
                c["circular_uuid"],
                result.error_code,
                result.error_detail,
            )
    return (sent, failed)


def _send_mention_batch(service) -> tuple[int, int]:
    """Send notifications for all pending @mentions. Returns (sent, failed)."""
    log = logging.getLogger(__name__)
    db_pool = get_postgres_client().get_pool()
    mention_repo = MentionNotificationsRepository(db_pool)
    pending = mention_repo.list_pending(limit=200)
    if not pending:
        return (0, 0)

    channel = service.supported_channels()[0]
    sent = failed = 0
    for row in pending:
        comment_dt = row.get("created_at")
        comment_created_at = (
            comment_dt.strftime("%d %b %Y, %H:%M")
            if hasattr(comment_dt, "strftime")
            else ""
        )
        variables = {
            "mentioned_by_user_id": row["mentioned_by_user_id"],
            "mentioned_by_name": row.get("mentioned_by_name"),
            "target_label": row["target_label"],
            "comment_text": row["text"],
            "expert_name": row["expert_name"],
            "comment_url": (
                f"{Config.FRONTEND_BASE_URL}/taskview"
                f"?id={row['circular_uuid']}&expertId={row['expert_id']}"
                f"#comment-{row['comment_id']}"
            ),
            "comment_created_at": comment_created_at,
            "circular_title": row.get("circular_title", ""),
            "circular_full_reference": row.get("circular_full_reference", ""),
        }
        subject = (
            f"You were mentioned in a comment by @{row['mentioned_by_user_id']}"
        )
        request = NotificationRequest(
            channel=channel,
            recipients=(row["recipient_email"],),
            template_name="mention_notification.html",
            subject=subject,
            variables=variables,
            correlation_id=f"mention:{row['id']}",
        )
        result = service.send(request)
        if result.success:
            sent += 1
        else:
            failed += 1
            log.warning(
                "mention delivery failed id=%s code=%s detail=%s",
                row.get("id"),
                result.error_code,
                result.error_detail,
            )
    return (sent, failed)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the notification worker.")
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Path to rotating log file (e.g. /var/log/notification.log).",
    )
    args = parser.parse_args()

    _setup_logging(args.log_file)

    log = logging.getLogger(__name__)
    log.info("Notification worker starting")

    try:
        service = get_notification_service()
        recipients = tuple(Config.NOTIFICATION_RECIPIENTS)
        cs, cf = _send_circular_batch(service, recipients)
        ms, mf = _send_mention_batch(service)
    except Exception:
        log.exception("Notification worker aborted with an unexpected error")
        return 2

    sent, failed = cs + ms, cf + mf
    if failed:
        log.warning(
            "Notification worker finished: %d sent, %d failed", sent, failed
        )
        return 1
    log.info("Notification worker finished: %d sent, 0 failed", sent)
    return 0


if __name__ == "__main__":
    sys.exit(main())