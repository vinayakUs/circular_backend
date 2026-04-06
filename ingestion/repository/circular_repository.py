from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
import logging
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from ingestion.scrapper.dto import Circular


POSTGRES_SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS circulars (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    circular_id VARCHAR(50) NOT NULL,
    source VARCHAR(20) NOT NULL,
    full_reference TEXT NOT NULL,
    department VARCHAR(50),
    title TEXT NOT NULL,
    issue_date DATE NOT NULL,
    effective_date DATE,
    url TEXT,
    pdf_url TEXT,
    file_path VARCHAR(500),
    content_hash VARCHAR(64),
    status VARCHAR(20) NOT NULL DEFAULT 'DISCOVERED',
    error_message TEXT,
    detected_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source, circular_id)
);

CREATE INDEX IF NOT EXISTS idx_circulars_status ON circulars(status);
CREATE INDEX IF NOT EXISTS idx_circulars_source ON circulars(source);
CREATE INDEX IF NOT EXISTS idx_circulars_issue_date ON circulars(issue_date DESC);

CREATE TABLE IF NOT EXISTS scraper_checkpoints (
    source VARCHAR(20) PRIMARY KEY,
    last_run_date DATE NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


@dataclass(slots=True)
class CircularRecord:
    id: UUID
    source: str
    circular_id: str
    full_reference: str
    department: str
    title: str
    issue_date: date
    effective_date: date | None
    url: str
    pdf_url: str
    status: str
    file_path: str | None
    content_hash: str | None
    error_message: str | None
    detected_at: datetime
    created_at: datetime
    updated_at: datetime


class CircularRepository:
    """Repository for circular records and scraper checkpoints."""

    def __init__(self, db_pool: Any = None) -> None:
        self.logger = logging.getLogger(__name__)
        self.db_pool = db_pool
        self._schema_initialized = False
        self._records_by_key: dict[tuple[str, str], CircularRecord] = {}
        self._records_by_id: dict[UUID, CircularRecord] = {}
        self._checkpoints: dict[str, date] = {}

    def upsert_circular(self, circular: Circular) -> tuple[UUID, bool]:
        if self.db_pool is not None:
            return self._upsert_circular_db(circular)

        key = self._build_key(circular.source, circular.circular_id)
        now = datetime.now(timezone.utc)

        if key in self._records_by_key:
            existing = self._records_by_key[key]
            updated = replace(
                existing,
                full_reference=circular.full_reference,
                department=circular.department,
                title=circular.title,
                issue_date=circular.issue_date,
                effective_date=circular.effective_date,
                url=circular.url,
                pdf_url=circular.pdf_url,
                detected_at=circular.detected_at,
                updated_at=now,
            )
            self._store_record(updated)
            self.logger.info(
                "Circular upserted existing record source=%s circular_id=%s record_id=%s",
                updated.source,
                updated.circular_id,
                updated.id,
            )
            return updated.id, False

        record = CircularRecord(
            id=uuid4(),
            source=circular.source.upper(),
            circular_id=circular.circular_id.upper(),
            full_reference=circular.full_reference,
            department=circular.department,
            title=circular.title,
            issue_date=circular.issue_date,
            effective_date=circular.effective_date,
            url=circular.url,
            pdf_url=circular.pdf_url,
            status="DISCOVERED",
            file_path=None,
            content_hash=None,
            error_message=None,
            detected_at=circular.detected_at,
            created_at=now,
            updated_at=now,
        )
        self._store_record(record)
        self.logger.info(
            "Circular inserted source=%s circular_id=%s record_id=%s",
            record.source,
            record.circular_id,
            record.id,
        )
        return record.id, True

    def update_file_path(
        self, record_id: UUID, file_path: str, content_hash: str | None = None
    ) -> None:
        if self.db_pool is not None:
            self._update_file_path_db(record_id, file_path, content_hash)
            return

        record = self._records_by_id[record_id]
        updated = replace(
            record,
            file_path=file_path,
            content_hash=content_hash,
            updated_at=datetime.now(timezone.utc),
        )
        self._store_record(updated)
        self.logger.info(
            "Circular file path updated record_id=%s file_path=%s content_hash=%s",
            updated.id,
            updated.file_path,
            updated.content_hash,
        )

    def update_status(
        self, record_id: UUID, status: str, error_message: str | None = None
    ) -> None:
        if self.db_pool is not None:
            self._update_status_db(record_id, status, error_message)
            return

        record = self._records_by_id[record_id]
        updated = replace(
            record,
            status=status,
            error_message=error_message,
            updated_at=datetime.now(timezone.utc),
        )
        self._store_record(updated)
        self.logger.info(
            "Circular status updated record_id=%s status=%s error=%s",
            updated.id,
            updated.status,
            updated.error_message,
        )

    def get_checkpoint(self, source: str) -> date | None:
        if self.db_pool is not None:
            return self._get_checkpoint_db(source)

        checkpoint = self._checkpoints.get(source.upper())
        self.logger.debug(
            "Checkpoint lookup source=%s last_run_date=%s",
            source.upper(),
            checkpoint,
        )
        return checkpoint

    def set_checkpoint(self, source: str, run_date: date) -> None:
        if self.db_pool is not None:
            self._set_checkpoint_db(source, run_date)
            return

        self._checkpoints[source.upper()] = run_date
        self.logger.info(
            "Checkpoint updated source=%s last_run_date=%s",
            source.upper(),
            run_date,
        )

    def exists_by_source_and_id(self, source: str, circular_id: str) -> bool:
        if self.db_pool is not None:
            return self.get_record(source, circular_id) is not None

        key = self._build_key(source, circular_id)
        return key in self._records_by_key

    def get_record(self, source: str, circular_id: str) -> CircularRecord | None:
        if self.db_pool is not None:
            self._ensure_schema()
            source_name, normalized_id = self._build_key(source, circular_id)
            with self.db_pool.connection() as conn:
                row = conn.execute(
                    """
                    SELECT id, source, circular_id, full_reference, department, title,
                           issue_date, effective_date, url, pdf_url, status, file_path,
                           content_hash, error_message, detected_at, created_at, updated_at
                    FROM circulars
                    WHERE source = %s AND circular_id = %s
                    """,
                    (source_name, normalized_id),
                ).fetchone()
            return self._row_to_record(row)

        key = self._build_key(source, circular_id)
        return self._records_by_key.get(key)

    def list_records(self) -> list[CircularRecord]:
        if self.db_pool is not None:
            self._ensure_schema()
            with self.db_pool.connection() as conn:
                rows = conn.execute(
                    """
                    SELECT id, source, circular_id, full_reference, department, title,
                           issue_date, effective_date, url, pdf_url, status, file_path,
                           content_hash, error_message, detected_at, created_at, updated_at
                    FROM circulars
                    ORDER BY source, circular_id
                    """
                ).fetchall()
            return [record for row in rows if (record := self._row_to_record(row))]

        return sorted(
            self._records_by_id.values(),
            key=lambda record: (record.source, record.circular_id),
        )

    def schema_sql(self) -> str:
        return POSTGRES_SCHEMA_SQL.strip()

    def export_schema(self, target_path: str | Path) -> Path:
        path = Path(target_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{self.schema_sql()}\n")
        return path

    def _build_key(self, source: str, circular_id: str) -> tuple[str, str]:
        return source.upper(), circular_id.upper()

    def _store_record(self, record: CircularRecord) -> None:
        key = self._build_key(record.source, record.circular_id)
        self._records_by_key[key] = record
        self._records_by_id[record.id] = record

    def _ensure_schema(self) -> None:
        if self._schema_initialized or self.db_pool is None:
            return

        with self.db_pool.connection() as conn:
            conn.execute(self.schema_sql())
        self._schema_initialized = True
        self.logger.info("Repository schema initialized")

    def _upsert_circular_db(self, circular: Circular) -> tuple[UUID, bool]:
        self._ensure_schema()
        source_name, normalized_id = self._build_key(circular.source, circular.circular_id)
        now = datetime.now(timezone.utc)

        with self.db_pool.connection() as conn:
            row = conn.execute(
                """
                INSERT INTO circulars (
                    source, circular_id, full_reference, department, title,
                    issue_date, effective_date, url, pdf_url, status,
                    file_path, content_hash, error_message, detected_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, circular_id) DO UPDATE
                SET full_reference = EXCLUDED.full_reference,
                    department = EXCLUDED.department,
                    title = EXCLUDED.title,
                    issue_date = EXCLUDED.issue_date,
                    effective_date = EXCLUDED.effective_date,
                    url = EXCLUDED.url,
                    pdf_url = EXCLUDED.pdf_url,
                    detected_at = EXCLUDED.detected_at,
                    updated_at = NOW()
                RETURNING id, source, circular_id, full_reference, department, title,
                          issue_date, effective_date, url, pdf_url, status, file_path,
                          content_hash, error_message, detected_at, created_at, updated_at,
                          (xmax = 0) AS inserted
                """,
                (
                    source_name,
                    normalized_id,
                    circular.full_reference,
                    circular.department,
                    circular.title,
                    circular.issue_date,
                    circular.effective_date,
                    circular.url,
                    circular.pdf_url,
                    "DISCOVERED",
                    None,
                    None,
                    None,
                    circular.detected_at or now,
                ),
            ).fetchone()

        record = self._row_to_record(row)
        if record is None:
            raise RuntimeError("Failed to upsert circular record")

        inserted = bool(row[-1])
        self.logger.info(
            "Circular %s source=%s circular_id=%s record_id=%s",
            "inserted" if inserted else "upserted existing record",
            record.source,
            record.circular_id,
            record.id,
        )
        return record.id, inserted

    def _update_file_path_db(
        self, record_id: UUID, file_path: str, content_hash: str | None = None
    ) -> None:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                UPDATE circulars
                SET file_path = %s,
                    content_hash = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (file_path, content_hash, record_id),
            )
        self.logger.info(
            "Circular file path updated record_id=%s file_path=%s content_hash=%s",
            record_id,
            file_path,
            content_hash,
        )

    def _update_status_db(
        self, record_id: UUID, status: str, error_message: str | None = None
    ) -> None:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                UPDATE circulars
                SET status = %s,
                    error_message = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (status, error_message, record_id),
            )
        self.logger.info(
            "Circular status updated record_id=%s status=%s error=%s",
            record_id,
            status,
            error_message,
        )

    def _get_checkpoint_db(self, source: str) -> date | None:
        self._ensure_schema()
        source_name = source.upper()
        with self.db_pool.connection() as conn:
            row = conn.execute(
                """
                SELECT last_run_date
                FROM scraper_checkpoints
                WHERE source = %s
                """,
                (source_name,),
            ).fetchone()
        checkpoint = row[0] if row else None
        self.logger.debug(
            "Checkpoint lookup source=%s last_run_date=%s",
            source_name,
            checkpoint,
        )
        return checkpoint

    def _set_checkpoint_db(self, source: str, run_date: date) -> None:
        self._ensure_schema()
        source_name = source.upper()
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO scraper_checkpoints (source, last_run_date)
                VALUES (%s, %s)
                ON CONFLICT (source) DO UPDATE
                SET last_run_date = EXCLUDED.last_run_date,
                    updated_at = NOW()
                """,
                (source_name, run_date),
            )
        self.logger.info(
            "Checkpoint updated source=%s last_run_date=%s",
            source_name,
            run_date,
        )

    def _row_to_record(self, row: Any) -> CircularRecord | None:
        if row is None:
            return None

        return CircularRecord(
            id=row[0],
            source=row[1],
            circular_id=row[2],
            full_reference=row[3],
            department=row[4] or "",
            title=row[5],
            issue_date=row[6],
            effective_date=row[7],
            url=row[8] or "",
            pdf_url=row[9] or "",
            status=row[10],
            file_path=row[11],
            content_hash=row[12],
            error_message=row[13],
            detected_at=row[14],
            created_at=row[15],
            updated_at=row[16],
        )
