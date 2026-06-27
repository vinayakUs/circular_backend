from __future__ import annotations

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Any

from jinja2 import Template

from config import Config
from db.postgres_client import get_postgres_client
from ingestion.repository.circular_repository import CircularRepository
from services.notification_log_repository import NotificationLogRepository


class EmailService:
    def __init__(self):
        self.host = Config.SMTP_HOST
        self.port = Config.SMTP_PORT
        self.username = Config.SMTP_USERNAME
        self.password = Config.SMTP_PASSWORD
        self.use_tls = Config.SMTP_USE_TLS
        self.from_email = Config.SMTP_FROM_EMAIL
        self.from_name = Config.SMTP_FROM_NAME
        self.log_repo = NotificationLogRepository()
        self.template_dir = Path(__file__).parent / "templates"

    def _render_template(self, template_name: str, variables: dict[str, Any]) -> str:
        """Render a Jinja2 template with the given variables."""
        template_path = self.template_dir / template_name
        template = Template(template_path.read_text(encoding="utf-8"))
        return template.render(**variables)

    def _send_bcc_email(self, recipients: list[str], subject: str, html_body: str) -> tuple[bool, str | None]:
        """Send email with all recipients in BCC."""
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"{self.from_name} <{self.from_email}>"
            msg["BCC"] = ", ".join(recipients)
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(self.host, self.port) as server:
                if self.use_tls:
                    server.starttls()
                if self.username and self.password:
                    server.login(self.username, self.password)
                server.send_message(msg, to_addrs=recipients)
            return True, None
        except Exception as e:
            return False, str(e)

    def send_email(self, to_email: str, subject: str, html_body: str) -> tuple[bool, str | None]:
        """Send email via SMTP. Returns (success, error_message)."""
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"{self.from_name} <{self.from_email}>"
            msg["To"] = to_email
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(self.host, self.port) as server:
                if self.use_tls:
                    server.starttls()
                if self.username and self.password:
                    server.login(self.username, self.password)
                server.send_message(msg)
            return True, None
        except Exception as e:
            return False, str(e)

    def send_circular_notification(self, to_email: str, circular_data: dict[str, Any]) -> tuple[bool, str | None]:
        """Send circular notification email with logging."""
        template_name = "circular_notification.html"
        variables = {
            "circular_id": circular_data.get("circular_id", ""),
            "title": circular_data.get("title", ""),
            "department": circular_data.get("department", ""),
            "issue_date": str(circular_data.get("issue_date", "")),
            "source": circular_data.get("source", ""),
            "url": circular_data.get("url", ""),
        }
        subject = f"Regulatory Circular: {circular_data.get('circular_id', '')}"

        # Create log entry before sending
        log_id = self.log_repo.create_log(template_name, to_email, subject, variables)

        # Render template
        html = self._render_template(template_name, variables)

        # Send email
        success, error = self.send_email(to_email, subject, html)

        # Update log status
        if success:
            self.log_repo.mark_sent(log_id)
        else:
            self.log_repo.mark_failed(log_id, error or "Unknown error")

        return success, error

    def send_notification_for_circular_to_all_bcc(self, circular_uuid: str, circular_data: dict[str, Any]) -> tuple[bool, str | None]:
        """Send notification for a specific circular to all configured recipients in BCC."""
        recipients = Config.NOTIFICATION_RECIPIENTS
        if not recipients:
            return False, "No notification recipients configured"

        template_name = "circular_notification.html"
        variables = {
            "circular_id": circular_data.get("circular_id", ""),
            "circular_uuid": circular_uuid,
            "title": circular_data.get("title", ""),
            "department": circular_data.get("department", ""),
            "issue_date": str(circular_data.get("issue_date", "")),
            "source": circular_data.get("source", ""),
            "url": circular_data.get("url", ""),
        }
        subject = f"Regulatory Circular: {circular_data.get('circular_id', '')}"
        html = self._render_template(template_name, variables)

        # Create log entry before sending
        log_id = self.log_repo.create_log(
            template_name, ",".join(recipients), subject, variables
        )

        # Send one email with all recipients in BCC
        success, error = self._send_bcc_email(recipients, subject, html)

        if success:
            self.log_repo.mark_sent(log_id)
        else:
            self.log_repo.mark_failed(log_id, error or "Unknown error")

        return success, error

    def get_pending_circulars(self) -> list[dict[str, Any]]:
        """Get circulars with status FETCHED (within 24h) that haven't been notified yet."""
        import logging
        logger = logging.getLogger(__name__)

        db_pool = get_postgres_client().get_pool()
        circular_repo = CircularRepository(db_pool)

        # Get recently fetched circulars (last 24h)
        fetched = circular_repo.list_recent_fetched_circulars_for_notification(hours=24)
        logger.info(f"[NOTIFICATION DEBUG] Fetched circulars: {len(fetched)}")
        for f in fetched:
            logger.info(f"[NOTIFICATION DEBUG]   - id={f.id} full_reference={f.full_reference} status={f.status}")

        # Filter out already notified
        circular_ids = [str(r.id) for r in fetched]
        logger.info(f"[NOTIFICATION DEBUG] circular_ids (as strings): {circular_ids}")

        notified = self.log_repo.get_notified_circular_ids(circular_ids)
        logger.info(f"[NOTIFICATION DEBUG] Already notified UUIDs: {notified}")

        result = []
        for record in fetched:
            record_id_str = str(record.id)
            logger.info(f"[NOTIFICATION DEBUG] Checking record.id={record_id_str} in notified={notified} -> {record_id_str in notified}")
            if record_id_str in notified:
                logger.info(f"[NOTIFICATION DEBUG]   SKIP (already notified): {record.full_reference}")
                continue
            logger.info(f"[NOTIFICATION DEBUG]   INCLUDE: {record.full_reference}")
            result.append({
                "id": record.id,
                "circular_id": record.full_reference,
                "circular_uuid": str(record.id),
                "title": record.title,
                "department": record.department,
                "issue_date": str(record.issue_date) if record.issue_date else "",
                "source": record.source,
                "url": f"https://abc.com/circular/{record.id}",
                "pdf_url": record.pdf_url,
            })
        logger.info(f"[NOTIFICATION DEBUG] Final pending list: {len(result)} circulars")
        return result

    def send_pending_notifications(self) -> tuple[int, int]:
        """Send one email per pending circular to all recipients in BCC.
        Returns (sent_count, failed_count) so callers can detect partial failure.
        """
        recipients = Config.NOTIFICATION_RECIPIENTS
        if not recipients:
            return 0, 0

        pending = self.get_pending_circulars()
        if not pending:
            return 0, 0

        template_name = "circular_notification.html"
        sent_count = 0
        failed_count = 0

        for circular in pending:
            variables = {
                "full_reference": circular["circular_id"],
                "circular_uuid": circular["circular_uuid"],
                "title": circular["title"],
                "department": circular["department"],
                "issue_date": circular["issue_date"],
                "source": circular["source"],
                "url": circular["url"],
            }
            subject = f"Regulatory Circular: {circular['circular_id']}"
            html = self._render_template(template_name, variables)

            # Create single log entry for all recipients (BCC)
            log_id = self.log_repo.create_log(
                template_name, ",".join(recipients), subject, variables
            )

            # Send one email with all recipients in BCC
            success, error = self._send_bcc_email(recipients, subject, html)

            # Mark log as sent or failed
            if success:
                self.log_repo.mark_sent(log_id)
                sent_count += 1
            else:
                self.log_repo.mark_failed(log_id, error or "Unknown error")
                failed_count += 1

        return sent_count, failed_count