"""
Cron-friendly notification worker.

Processes circulars in 'FETCHED' status that have not been notified yet,
sends one BCC email per circular to all configured recipients, and exits
with a status code suitable for cron monitoring.

Run with:  python -m services.notification_worker

Exit codes:
  0  success (including "nothing to do")
  1  at least one notification failed to send
  2  unexpected error (raised exception)

Cron example (every 5 minutes):
  */5 * * * * cd /path/to/circular_backend && /path/to/venv/bin/python -m services.notification_worker >> /var/log/circular_notifications.log 2>&1
"""

"""
Cron-friendly notification worker.

Processes circulars in 'FETCHED' status that have not been notified yet,
sends one BCC email per circular to all configured recipients, and exits
with a status code suitable for cron monitoring.

Run with:  python -m services.notification_worker [--log-file /var/log/notifications.log]

Exit codes:
  0  success (including "nothing to do")
  1  at least one notification failed to send
  2  unexpected error (raised exception)

Cron example (every 5 minutes):
  */5 * * * * cd /path/to/circular_backend && /path/to/venv/bin/python -m services.notification_worker --log-file /var/log/notification.log >> /dev/null 2>&1
"""

import argparse
import logging
import sys
from logging.handlers import TimedRotatingFileHandler

from db.postgres_client import get_postgres_client
from ingestion.repository.mention_notifications_repository import MentionNotificationsRepository
from services.notification_service import EmailService


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

    logger = logging.getLogger(__name__)
    logger.info("Notification worker starting")
    email_svc = EmailService()
    sent = 0
    failed = 0

    try:
        # 1) existing circular pipeline
        cs, cf = email_svc.send_pending_notifications()
        sent += cs
        failed += cf

        # 2) new mention pipeline
        db_pool = get_postgres_client().get_pool()
        mention_repo = MentionNotificationsRepository(db_pool)
        pending = mention_repo.list_pending(limit=200)
        for row in pending:
            try:
                ok, _ = email_svc.send_mention_notification(row)
            except Exception:
                logger.exception("mention notification failed for id=%s", row.get("id"))
                ok = False
            if ok:
                sent += 1
            else:
                failed += 1
    except Exception:
        logger.exception("Notification worker aborted with an unexpected error")
        return 2

    if failed:
        logger.warning("Notification worker finished: %d sent, %d failed", sent, failed)
        return 1
    logger.info("Notification worker finished: %d sent, 0 failed", sent)
    return 0


if __name__ == "__main__":
    sys.exit(main())


def main() -> int:
    logger.info("Notification worker starting")
    email_svc = EmailService()
    sent = 0
    failed = 0

    try:
        # 1) existing circular pipeline
        cs, cf = email_svc.send_pending_notifications()
        sent += cs
        failed += cf

        # 2) new mention pipeline
        db_pool = get_postgres_client().get_pool()
        mention_repo = MentionNotificationsRepository(db_pool)
        pending = mention_repo.list_pending(limit=200)
        for row in pending:
            try:
                ok, _ = email_svc.send_mention_notification(row)
            except Exception:
                logger.exception("mention notification failed for id=%s", row.get("id"))
                ok = False
            if ok:
                sent += 1
            else:
                failed += 1
    except Exception:
        logger.exception("Notification worker aborted with an unexpected error")
        return 2

    if failed:
        logger.warning("Notification worker finished: %d sent, %d failed", sent, failed)
        return 1
    logger.info("Notification worker finished: %d sent, 0 failed", sent)
    return 0


if __name__ == "__main__":
    sys.exit(main())
