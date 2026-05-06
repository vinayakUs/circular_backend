"""Repository for managing document summaries stored in S3."""

import logging
import re
from datetime import date
from typing import Any
from uuid import UUID

from storage.s3_client import S3StorageClient


class SummaryRepository:
    """Repository for storing summary metadata and S3 upload."""

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("SummaryRepository requires db_pool")
        self.logger = logging.getLogger(__name__)
        self.db_pool = db_pool
        self.s3_client = S3StorageClient()

    def _build_safe_filename(self, value: str) -> str:
        """Sanitize a string for use in S3 keys."""
        safe_value = re.sub(r'[<>:"/\\|?*]+', "_", value).strip()
        safe_value = re.sub(r"\s+", "_", safe_value)
        safe_value = re.sub(r"_+", "_", safe_value).strip("._")
        return safe_value or "document"

    def _build_summary_key(
        self, source: str, circular_id: str, issue_date: date, summary_filename: str = "summary.md"
    ) -> str:
        """Build full S3 URL for summary file following existing conventions."""
        safe_circular_id = self._build_safe_filename(circular_id)
        key = (
            f"{source.upper()}/"
            f"{issue_date:%Y}/{issue_date:%m}/"
            f"{safe_circular_id}/summary/{summary_filename}"
        )
        return f"s3://{self.s3_client.bucket}/{key}"

    def _ensure_schema(self) -> None:
        """Ensure the summaries table exists."""
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS summaries (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    circular_id UUID NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
                    summary_key TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (circular_id)
                )
                """
            )

    def upload_and_store_summary(
        self, circular_id: UUID, source: str, circular_ref: str, issue_date: date, summary_text: str
    ) -> str:
        """Upload summary to S3 and store the key in the database.

        Returns the S3 key where the summary was stored.
        """
        self._ensure_schema()

        # Build S3 key and upload
        summary_key = self._build_summary_key(source, circular_ref, issue_date)
        content_bytes = summary_text.encode("utf-8")
        s3_url = self.s3_client.upload_bytes(summary_key, content_bytes)
        self.logger.info(
            "Uploaded summary to S3 bucket=%s key=%s size_bytes=%s",
            self.s3_client.bucket,
            summary_key,
            len(content_bytes),
        )

        # Delete existing summary record if present (idempotent)
        with self.db_pool.connection() as conn:
            conn.execute("DELETE FROM summaries WHERE circular_id = %s", (circular_id,))
            conn.execute(
                """
                INSERT INTO summaries (circular_id, summary_key)
                VALUES (%s, %s)
                """,
                (circular_id, summary_key),
            )

        return summary_key

    def get_summary_key(self, circular_id: UUID) -> str | None:
        """Get the S3 key for a summary by circular_id."""
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            row = conn.execute(
                "SELECT summary_key FROM summaries WHERE circular_id = %s",
                (circular_id,),
            ).fetchone()
            return row[0] if row else None

    def delete_summary_for_circular(self, circular_id: UUID) -> None:
        """Delete summary S3 object and DB record (idempotent)."""
        self._ensure_schema()

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
        with self.db_pool.connection() as conn:
            conn.execute("DELETE FROM summaries WHERE circular_id = %s", (circular_id,))