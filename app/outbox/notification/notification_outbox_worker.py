
"""
Notification outbox worker.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler
from os import getpid
from socket import gethostname

from app.outbox.notification.notification_outbox_repository import (
    NotificationOutboxJob,
    NotificationOutboxRepository,
)
from app.services.notifications.make_service import get_notification_service
from app.services.notifications.models import NotificationRequest
from config import Config
from db.postgres_client import get_postgres_client

BATCH_SIZE = 50
MAX_BATCHES_PER_RUN = 20
# Maximum time a job can remain locked by a worker.
LOCK_TIMEOUT = timedelta(minutes=10)


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


def _service_channel(service, outbox_channel: str) -> str:
    """Map outbox channel codes to registered service channel names."""
    if outbox_channel == "EMAIL":
        for name in service.supported_channels():
            if name.endswith("_email"):
                return name
    return outbox_channel.lower()


def _build_request(service, job: NotificationOutboxJob) -> NotificationRequest:
    # Subject is stored in params because the outbox table has no subject column.
    params = dict(job.params)
    subject = params.get("subject")
    return NotificationRequest(
        channel=_service_channel(service, job.channel),
        recipients=(job.recipient_email,),
        template_name=job.template,
        variables=params,
        subject=subject,
        correlation_id=f"outbox:{job.id}",
    )


def process_queue(service, repo, *, worker_id: str) -> dict[str, int]:
    """Process pending notification jobs in batches."""
    log = logging.getLogger(__name__)
    tallies = {"sent": 0, "retried": 0, "failed": 0, "superseded": 0}

    for _ in range(MAX_BATCHES_PER_RUN):
        released = repo.release_stale(
            stale_before=datetime.now(timezone.utc) - LOCK_TIMEOUT
        )
        if released:
            log.warning(
                "released %d stale PROCESSING job(s) from dead workers", released
            )

        jobs = repo.claim(worker_id, limit=BATCH_SIZE)
        if not jobs:
            break

        for job in jobs:
            result = service.send(_build_request(service, job))

            if result.success:
                if repo.mark_sent(job.id):
                    tallies["sent"] += 1
                    log.info(
                        "sent job=%s template=%s to=%s",
                        job.id, job.template, job.recipient_email,
                    )
                else:
                    tallies["superseded"] += 1
                    log.info(
                        "job=%s already completed by another worker", job.id
                    )
                continue

            error = f"{result.error_code}: {result.error_detail}"
            status = repo.mark_failed(job.id, error)
            if status == "FAILED":
                tallies["failed"] += 1
                log.warning(
                    "job=%s TERMINALLY FAILED after %d attempt(s): %s",
                    job.id, job.attempt_count, error,
                )
            else:
                tallies["retried"] += 1
                log.warning(
                    "job=%s delivery failed (attempt %d/%d), will retry: %s",
                    job.id, job.attempt_count, job.max_attempts, error,
                )

    return tallies


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the notification outbox worker."
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Path to rotating log file (e.g. /var/log/notification.log).",
    )
    args = parser.parse_args()

    _setup_logging(args.log_file)

    log = logging.getLogger(__name__)
    worker_id = f"{gethostname()}:{getpid()}"
    log.info("Notification outbox worker starting worker_id=%s", worker_id)

    try:
        service = get_notification_service()
        repo = NotificationOutboxRepository(get_postgres_client().get_pool())
        tallies = process_queue(service, repo, worker_id=worker_id)
    except Exception:
        log.exception("Notification outbox worker aborted with an unexpected error")
        return 2

    log.info(
        "Notification outbox worker finished: %d processed — %d sent, "
        "%d retrying, %d terminal FAILED, %d superseded",
        sum(tallies.values()), tallies["sent"], tallies["retried"],
        tallies["failed"], tallies["superseded"],
    )
    return 1 if tallies["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
