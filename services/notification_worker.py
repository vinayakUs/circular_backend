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

import logging
import sys

from services.notification_service import EmailService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> int:
    logger.info("Notification worker starting")
    email_svc = EmailService()

    try:
        sent, failed = email_svc.send_pending_notifications()
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
