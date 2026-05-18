"""
Background notification worker.

Run with: python -m services.notification_worker
"""

import logging
import time

from config import Config
from services.notification_service import EmailService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting notification worker...")
    email_svc = EmailService()

    while True:
        try:
            count = email_svc.send_pending_notifications()
            if count > 0:
                logger.info(f"Sent {count} notification(s)")
            else:
                logger.debug("No pending notifications")
        except Exception as e:
            logger.error(f"Error in notification worker: {e}")

        time.sleep(300)  # Check every 5 minutes


if __name__ == "__main__":
    main()