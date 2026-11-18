"""
Circular digest fan-out — one email per opted-in user per cadence carrying
their persona's upcoming deadlines (T..T+window IST). The email is a carrier
for sections that grow over time; deadlines is the first. Delivery is the
outbox worker's job; this module only snapshots into notification_outbox.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from app.notifications.topics.user_notification_prefs_repository import (
    UserNotificationPrefsRepository,
)
from app.services.personas import get_personas
from config import Config
from ingestion.repository.action_item_repository import ActionItemRepository

if TYPE_CHECKING:
    from app.notifications.producer import Ctx

IST = ZoneInfo("Asia/Kolkata")
log = logging.getLogger(__name__)

WINDOW_PHRASES = {"daily": "the next week", "weekly": "the next two weeks"}


def _persona_label(slug: str) -> str:
    return next((p.name for p in get_personas() if p.slug == slug), slug)


def run_circular_digest(
    ctx: "Ctx",
    now: datetime,
    *,
    cadence: str,
    window_days: int,
) -> int:
    """Jobs enqueued — 0 on re-runs: the idempotency key collides."""
    prefs_repo = UserNotificationPrefsRepository(ctx.pool)
    items_repo = ActionItemRepository(ctx.pool)

    recipients = prefs_repo.list_users_by_frequency(cadence.upper())
    if not recipients:
        return 0

    today = now.astimezone(IST).date()
    deadline_cache: dict[str, list] = {}

    enqueued = 0
    for user_id, email, persona_slug in recipients:
        if persona_slug not in deadline_cache:
            rows, _total = items_repo.get_deadlines(
                persona_slug=persona_slug,
                from_date=today,
                to_date=today + timedelta(days=window_days),
            )
            deadline_cache[persona_slug] = rows
        rows = deadline_cache[persona_slug]

        deadlines = [
            {
                "deadline": str(deadline),
                "action_item": action_item,
                "source": source,
                "circular_id": str(circular_id),
                "title": title,
            }
            for (_item_id, deadline, action_item, source, circular_id, title) in rows
        ]

        with ctx.pool.acquire() as conn:
            job = ctx.outbox.enqueue(
                conn.cursor(),
                user_id=user_id,
                recipient_email=email,
                channel="EMAIL",
                template="circular_digest.html",
                params={
                    # Subject lives in params — the outbox table has no subject
                    # column; the worker reads params["subject"].
                    "subject": f"Your {cadence} circular digest — CircularHub",
                    "base_url": Config.FRONTEND_BASE_URL,
                    "persona_label": _persona_label(persona_slug),
                    "cadence": cadence,
                    "window_days": window_days,
                    "window_phrase": WINDOW_PHRASES[cadence],
                    "deadlines": deadlines,
                },
                idempotency_key=f"{cadence}-digest:{user_id}:{today.isoformat()}",
            )
            conn.commit()
        enqueued += job is not None

    return enqueued