"""
Immediate fan-out for source subscriptions — one outbox job per circular
per subscribed user (never a digest). Delivery is the outbox worker's job;
this module only snapshots the email into notification_outbox.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import markdown as md

from app.notifications.topics.user_source_subscriptions_repository import (
    UserSourceSubscriptionsRepository,
)
from config import Config
from ingestion.repository.circular_repository import CircularRepository
from ingestion.repository.summary_repository import SummaryRepository

if TYPE_CHECKING:
    from app.notifications.producer import Ctx

log = logging.getLogger(__name__)


def _build_subject(source: str | None, full_reference: str) -> str:
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


def _summary_params(record: Any, summary_repo: Any) -> tuple[str | None, bool]:
    """(summary_html, summary_pending) — missing/empty/failed summaries count
    as pending once SUMMARY_GRACE_PERIOD_HOURS has passed since the circular
    landed (same rules as the circular notification emails)."""
    grace_expired = (
        datetime.now(timezone.utc) - record.created_at
    ).total_seconds() / 3600.0 >= Config.SUMMARY_GRACE_PERIOD_HOURS

    summary_html = None
    summary_pending = grace_expired
    try:
        summary_text = summary_repo.get_summary_text(record.id)
    except Exception:
        log.exception("failed to download summary for circular_id=%s", record.id)
        summary_text = None

    if summary_text and summary_text.strip():
        summary_html = md.markdown(summary_text, extensions=["extra", "sane_lists"])
        summary_pending = False
    elif not summary_pending:
        summary_pending = True
    return summary_html, summary_pending


def run_source_circulars(ctx: "Ctx", now: datetime) -> int:
    """Jobs enqueued — 0 on re-runs: the idempotency key collides."""
    circular_repo = CircularRepository(ctx.pool)
    summary_repo = SummaryRepository(ctx.pool)
    subs_repo = UserSourceSubscriptionsRepository(ctx.pool)

    fetched = circular_repo.list_recent_fetched_circulars_for_notification(hours=24)
    enqueued = 0

    for record in fetched:
        subscribers = subs_repo.list_subscribers_for_source(record.source or "")
        if not subscribers:
            continue

        summary_html, summary_pending = _summary_params(record, summary_repo)
        params = {
            "full_reference": record.full_reference,
            "circular_uuid": str(record.id),
            "title": record.title,
            "department": record.department,
            "issue_date": str(record.issue_date) if record.issue_date else "",
            "source": record.source,
            "url": f"{Config.FRONTEND_BASE_URL}/circular/{record.id}",
            "pdf_url": record.pdf_url,
            "applicable_to_nse": bool(record.applicable_to_nse),
            "summary_html": summary_html,
            "summary_pending": bool(summary_pending),
            # Subject lives in params — the outbox table has no subject
            # column; the worker reads params["subject"].
            "subject": _build_subject(record.source, record.full_reference),
        }

        with ctx.pool.acquire() as conn:
            cur = conn.cursor()
            for sub in subscribers:
                if not sub.email:
                    continue
                job = ctx.outbox.enqueue(
                    cur,
                    user_id=sub.user_db_id,
                    recipient_email=sub.email,
                    channel="EMAIL",
                    template="circular_notification.html",
                    params=params,
                    idempotency_key=f"circular:{record.id}:{sub.user_db_id}",
                )
                enqueued += job is not None
            conn.commit()

    return enqueued