from __future__ import annotations

import logging
import smtplib
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Any

import markdown as md
from jinja2 import Template

from config import Config
from db.postgres_client import get_postgres_client
from ingestion.repository.circular_repository import CircularRepository
from ingestion.repository.mention_notifications_repository import MentionNotificationsRepository
from ingestion.repository.summary_repository import SummaryRepository
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
        self.mention_repo = MentionNotificationsRepository(db_pool=None)
        self.template_dir = Path(__file__).parent / "templates"

    def _render_template(self, template_name: str, variables: dict[str, Any]) -> str:
        """Render a Jinja2 template with the given variables."""
        template_path = self.template_dir / template_name
        template = Template(template_path.read_text(encoding="utf-8"))
        return template.render(**variables)

    def _build_subject(self, source: str | None, full_reference: str) -> str:
        """Build the email subject line from the circular's source and reference.

        - SEBI        → 'SEBI Circular: <ref>'
        - SEBI_MASTER → 'Sebi Master Circular: <ref>'   (distinct from regular SEBI)
        - NSE         → 'NSE Circular: <ref>'
        - anything else → 'CircularHub Circular: <ref>'
        """
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
            "full_reference": circular_data.get("circular_id", ""),
            "title": circular_data.get("title", ""),
            "department": circular_data.get("department", ""),
            "issue_date": str(circular_data.get("issue_date", "")),
            "source": circular_data.get("source", ""),
            "url": circular_data.get("url", ""),
            "applicable_to_nse": bool(circular_data.get("applicable_to_nse", False)),
            "summary_html": circular_data.get("summary_html"),
            "summary_pending": bool(circular_data.get("summary_pending", False)),
        }
        subject = self._build_subject(
            circular_data.get("source"),
            circular_data.get("circular_id", ""),
        )

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
            "full_reference": circular_data.get("circular_id", ""),
            "circular_uuid": circular_uuid,
            "title": circular_data.get("title", ""),
            "department": circular_data.get("department", ""),
            "issue_date": str(circular_data.get("issue_date", "")),
            "source": circular_data.get("source", ""),
            "url": circular_data.get("url", ""),
            "applicable_to_nse": bool(circular_data.get("applicable_to_nse", False)),
            "summary_html": circular_data.get("summary_html"),
            "summary_pending": bool(circular_data.get("summary_pending", False)),
        }
        subject = self._build_subject(
            circular_data.get("source"),
            circular_data.get("circular_id", ""),
        )
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

    def send_mention_notification(self, row: dict) -> tuple[bool, str | None]:
        """row comes from MentionNotificationsRepository.list_pending()."""
        print(row)
        template_name = "mention_notification.html"
        # Format the comment timestamp for display; fall back to blank if missing.
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
            "expert_name":  row["expert_name"],
            "comment_url":  f"{Config.FRONTEND_BASE_URL}/taskview?id={row['circular_uuid']}&expertId={row['expert_id']}#comment-{row['comment_id']}",
            "comment_created_at": comment_created_at,
            "circular_title": row.get("circular_title", ""),
            "circular_full_reference": row.get("circular_full_reference", ""),
        }
        subject = f"You were mentioned in a comment by @{row['mentioned_by_user_id']}"
        html = self._render_template(template_name, variables)

        log_id = self.log_repo.create_log(
            template_name, row["recipient_email"], subject, variables
        )
        success, error = self.send_email(row["recipient_email"], subject, html)

        if success:
            self.log_repo.mark_sent(log_id)
            self.mention_repo.mark_sent(row["id"], log_id)
        else:
            self.log_repo.mark_failed(log_id, error or "Unknown error")
            self.mention_repo.mark_failed(row["id"], error or "Unknown error")
        return success, error

    def get_pending_circulars(self) -> list[dict[str, Any]]:
        """Get circulars with status FETCHED (within 24h) that haven't been notified yet.

        Each result dict also carries two summary fields:
        - ``summary_html``: rendered HTML string of the AI summary, or None
        - ``summary_pending``: True when the circular is being sent without a
          summary (either because the 10h grace period expired or the S3
          download failed / returned empty text). Drives the email template's
          "AI summary pending" fallback message.
        """
        logger = logging.getLogger(__name__)

        db_pool = get_postgres_client().get_pool()
        circular_repo = CircularRepository(db_pool)
        summary_repo = SummaryRepository(db_pool)

        # Get recently fetched circulars (last 24h), already filtered by
        # summary readiness / 10h grace at the SQL level.
        fetched = circular_repo.list_recent_fetched_circulars_for_notification(hours=24)
        logger.info(f"[NOTIFICATION DEBUG] Fetched circulars: {len(fetched)}")
        for f in fetched:
            logger.info(f"[NOTIFICATION DEBUG]   - id={f.id} full_reference={f.full_reference} status={f.status}")

        # Filter out already notified
        circular_ids = [str(r.id) for r in fetched]
        logger.info(f"[NOTIFICATION DEBUG] circular_ids (as strings): {circular_ids}")

        notified = self.log_repo.get_notified_circular_ids(circular_ids)
        logger.info(f"[NOTIFICATION DEBUG] Already notified UUIDs: {notified}")

        grace_hours = Config.SUMMARY_GRACE_PERIOD_HOURS

        result = []
        for record in fetched:
            record_id_str = str(record.id)
            logger.info(f"[NOTIFICATION DEBUG] Checking record.id={record_id_str} in notified={notified} -> {record_id_str in notified}")
            if record_id_str in notified:
                logger.info(f"[NOTIFICATION DEBUG]   SKIP (already notified): {record.full_reference}")
                continue
            logger.info(f"[NOTIFICATION DEBUG]   INCLUDE: {record.full_reference}")

            # Was the 10h grace period the reason this circular is eligible?
            age_hours = (datetime.now(timezone.utc) - record.created_at).total_seconds() / 3600.0
            grace_expired = age_hours >= grace_hours

            # Try to fetch the summary. Treat missing / empty / failed downloads
            # as "pending" so the template can show the fallback message.
            summary_html: str | None = None
            summary_pending = grace_expired  # already true if 10h expired
            try:
                summary_text = summary_repo.get_summary_text(record.id)
            except Exception:
                logger.exception(
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
                # Summary row exists (SQL eligibility passed via EXISTS) but the
                # S3 download returned nothing useful — still treat as pending.
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
                "applicable_to_nse": circular.get("applicable_to_nse", False),
                "summary_html": circular.get("summary_html"),
                "summary_pending": bool(circular.get("summary_pending", False)),
            }
            subject = self._build_subject(circular["source"], circular["circular_id"])
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