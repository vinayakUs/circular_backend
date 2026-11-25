"""
Notification producer — the single timing entry point for all scheduled
notification jobs.

Ops surface = one crontab line:
    */5 * * * *  cd /root/circular_backend && .venv/bin/python -m app.notifications.producer

Timing is data (SCHEDULE below) — not separate files or separate crons.
tick() is safe to call at any frequency: every job carries a deterministic
idempotency key, so repeated or overlapping runs cannot double-send.
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from functools import partial
from logging.handlers import TimedRotatingFileHandler
from typing import Any, Callable
from zoneinfo import ZoneInfo

from app.notifications.circular_digest import run_circular_digest
from app.notifications.matching import TopicMatcher
from app.notifications.source_circulars import run_source_circulars
from app.notifications.topic_digest import run_topic_digest
from app.notifications.outbox.notification_outbox_repository import (
    NotificationOutboxRepository,
)
from app.notifications.topics.user_topics_repository import UserTopicRepository
from config import Config

IST = ZoneInfo("Asia/Kolkata")
log = logging.getLogger(__name__)


# ── Due predicates: pure functions of the clock, no state table ───────

DuePredicate = Callable[[datetime], bool]


def daily_ist(hhmm: str) -> DuePredicate:
    """Due between hhmm and hhmm+2h IST. The window absorbs tick jitter and
    short outages; the idempotency key absorbs the resulting re-runs."""
    start = time.fromisoformat(hhmm)
    end = (datetime.combine(datetime.min, start) + timedelta(hours=2)).time()

    def due(now: datetime) -> bool:
        local = now.astimezone(IST).time()
        return start <= local < end

    return due


def weekly_ist(weekday: int, hhmm: str) -> DuePredicate:
    """Due on `weekday` (Mon=0) between hhmm and hhmm+2h IST — same jitter
    window and idempotency absorption as daily_ist."""
    start = time.fromisoformat(hhmm)
    end = (datetime.combine(datetime.min, start) + timedelta(hours=2)).time()

    def due(now: datetime) -> bool:
        local = now.astimezone(IST)
        return local.weekday() == weekday and start <= local.time() < end

    return due


def always() -> DuePredicate:
    """Due every tick — fan-out jobs dedupe by idempotency key, not by time window."""
    return lambda now: True


# ── Job registry ────────────────────────────────────────────────────

@dataclass(slots=True, frozen=True)
class Job:
    name: str
    due: DuePredicate
    run: Callable[["Ctx", datetime], int]   # (ctx, now) -> jobs enqueued


SCHEDULE: list[Job] = [
    Job("source-circulars", always(), run_source_circulars),
    Job("daily-digest", daily_ist("08:00"),
        partial(run_circular_digest, cadence="daily", window_days=7)),
    Job("weekly-digest", weekly_ist(0, "08:00"),          # Mondays
        partial(run_circular_digest, cadence="weekly", window_days=14)),
    Job("topic-digest-daily", daily_ist("08:00"),
        partial(run_topic_digest, alert_level="DAILY", window_days=10000)),
    Job("topic-digest-weekly", weekly_ist(0, "08:00"),          # Mondays
        partial(run_topic_digest, alert_level="WEEKLY", window_days=10000)),
]


# ── Orchestration ───────────────────────────────────────────────────

def tick(now: datetime, ctx: "Ctx", *, force: bool = False) -> dict[str, int]:
    """Run every job that is due. One job failing never blocks the rest.
    force runs every job regardless of the due window (testing)."""
    results: dict[str, int] = {}
    for job in SCHEDULE:
        if not force and not job.due(now):
            continue
        try:
            results[job.name] = job.run(ctx, now)
        except Exception:
            log.exception("job %s failed", job.name)
            results[job.name] = -1
    return results


@dataclass(slots=True)
class Ctx:
    pool: Any
    outbox: NotificationOutboxRepository
    topics: UserTopicRepository
    matcher: TopicMatcher


def build_ctx() -> Ctx:
    from db.postgres_client import get_postgres_client
    from ingestion.indexer.es_provider import get_es_client
    from ingestion.repository.circular_repository import CircularRepository

    pool = get_postgres_client().get_pool()
    return Ctx(
        pool=pool,
        outbox=NotificationOutboxRepository(pool),
        topics=UserTopicRepository(pool),
        matcher=TopicMatcher(get_es_client(), CircularRepository(pool)),
    )


def _setup_logging(log_file: str | None) -> None:
    """Configure console and optional file logging."""
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    root = logging.getLogger()
    root.setLevel(getattr(logging, Config.LOG_LEVEL, logging.INFO))
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Run due notification jobs.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="run every SCHEDULE job regardless of the due window (testing)",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Path to rotating log file (e.g. /root/circular_backend/logs/producer.log).",
    )
    args = parser.parse_args()

    _setup_logging(args.log_file)
    results = tick(datetime.now(timezone.utc), build_ctx(), force=args.force)
    log.info("tick done: %s", results)
    return 1 if any(v == -1 for v in results.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
