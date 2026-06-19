"""Repository for managing document summaries stored in S3."""
# /root/circular_backend/ingestion/repository/summary_repository.py
import logging
from datetime import date
from typing import Any
from uuid import UUID

from storage.s3_client import S3StorageClient
from utils.s3_utils import build_safe_filename


class SummaryRepository:
    """Repository for storing summary metadata and S3 upload."""

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("SummaryRepository requires db_pool")
        self.logger = logging.getLogger(__name__)
        self.db_pool = db_pool
        self.s3_client = S3StorageClient()

    def _build_summary_key(
        self, source: str, source_item_key: str, issue_date: date, summary_filename: str = "summary.md"
    ) -> str:
        """Build full S3 URL for summary file following scrapper conventions."""
        safe_key = build_safe_filename(source_item_key)
        key = (
            f"{source.upper()}/"
            f"{issue_date:%Y}/{issue_date:%m}/"
            f"{safe_key}/summary/{summary_filename}"
        )
        return f"s3://{self.s3_client.bucket}/{key}"

    def upload_and_store_summary(
        self, circular_id: UUID, source: str, source_item_key: str, issue_date: date, summary_text: str
    ) -> str:
        """Upload summary to S3 and store the key in the database.

        Returns the S3 key where the summary was stored.
        """
        # Build S3 key and upload
        summary_key = self._build_summary_key(source, source_item_key, issue_date)
        content_bytes = summary_text.encode("utf-8")
        self.s3_client.upload_bytes(summary_key, content_bytes)
        self.logger.info(
            "Uploaded summary to S3 bucket=%s key=%s size_bytes=%s",
            self.s3_client.bucket,
            summary_key,
            len(content_bytes),
        )

        # Delete existing summary record if present (idempotent)
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM summaries WHERE circular_id = %s", (str(circular_id),))
            cursor.execute(
                """
                INSERT INTO summaries (circular_id, summary_key)
                VALUES (%s, %s)
                """,
                (str(circular_id), summary_key),
            )
            conn.commit()

        return summary_key

    def get_summary_key(self, circular_id: UUID) -> str | None:
        """Get the S3 key for a summary by circular_id."""
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT summary_key FROM summaries WHERE circular_id = %s",
                (str(circular_id),),
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def delete_summary_for_circular(self, circular_id: UUID) -> None:
        """Delete summary S3 object and DB record (idempotent)."""
        # Get the key first
        summary_key = self.get_summary_key(circular_id)

        # Delete from S3 if exists
        if summary_key:
            try:
                self.s3_client.delete(summary_key)
                self.logger.info("Deleted summary from S3 key=%s", summary_key)
            except Exception:
                self.logger.warning("Failed to delete S3 object key=%s", summary_key)

        # Delete DB record
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM summaries WHERE circular_id = %s", (str(circular_id),))

    def get_summary_text(self, circular_id: UUID) -> str | None:
        """Get summary text directly from S3 by circular_id."""
        summary_key = self.get_summary_key(circular_id)
        if not summary_key:
            return None
        try:
            return self.s3_client.download_as_text(summary_key)
        except Exception:
            self.logger.warning("Failed to download summary from S3 key=%s", summary_key)
            return None
