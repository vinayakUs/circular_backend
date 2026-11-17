"""
Topic digest fan-out — one outbox job per user holding a topic at the due
cadence, carrying a snapshot of what matched. The email body is assembled
at send time from params; the worker stays a pure renderer + sender.
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID
from zoneinfo import ZoneInfo

from app.topics.user_topics_repository import UserTopic

if TYPE_CHECKING:
    from app.notifications.producer import Ctx

IST = ZoneInfo("Asia/Kolkata")


def run_topic_digest(
    ctx: "Ctx",
    now: datetime,
    *,
    alert_level: str,
    window_days: int,
) -> int:
    """Jobs enqueued — 0 on re-runs: the idempotency key collides."""
    owned = ctx.topics.list_with_owner_email(alert_level)
    if not owned:
        return 0

    by_user: dict[UUID, list[tuple[UserTopic, str]]] = {}
    for topic, email in owned:
        by_user.setdefault(topic.user_id, []).append((topic, email))

    enqueued = 0
    run_date = now.astimezone(IST).date().isoformat()

    for user_id, entries in by_user.items():
        matched_topics = []
        for topic, _ in entries:
            hits = ctx.matcher.match_window(topic, now=now, window_days=window_days)
            if hits:
                matched_topics.append({
                    "topic_id": str(topic.id),
                    "topic_name": topic.topic_name,
                    "circulars": [h.to_params() for h in hits],
                })
        if not matched_topics:
            continue

        from pprint import pprint
        pprint(matched_topics, indent=4, width=80)


        total = sum(len(t["circulars"]) for t in matched_topics)
        with ctx.pool.acquire() as conn:
            job = ctx.outbox.enqueue(
                conn.cursor(),
                user_id=user_id,
                recipient_email=entries[0][1],
                channel="EMAIL",
                template="topic_digest.html",
                params={
                    # Subject lives in params — the outbox table has no subject
                    # column; the worker reads params["subject"].
                    "subject": (
                        f"{alert_level.title()} topic digest: {total} circular"
                        f"{'s' if total != 1 else ''} matched your topics"
                    ),
                    "cadence": alert_level,
                    "window_days": window_days,
                    "total": total,
                    "topics": matched_topics,
                },
                idempotency_key=f"topic-digest:{user_id}:{alert_level}:{run_date}",
            )
            conn.commit()
        enqueued += job is not None

    return enqueued
