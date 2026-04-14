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
    source_item_key TEXT,
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
    es_indexed_at TIMESTAMPTZ,
    es_chunk_count INT,
    es_index_name VARCHAR(100),
    UNIQUE (source, circular_id)
);

CREATE INDEX IF NOT EXISTS idx_circulars_status ON circulars(status);
CREATE INDEX IF NOT EXISTS idx_circulars_source ON circulars(source);
CREATE INDEX IF NOT EXISTS idx_circulars_issue_date ON circulars(issue_date DESC);
CREATE INDEX IF NOT EXISTS idx_circulars_es_pending ON circulars(status, es_indexed_at) WHERE file_path IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_circulars_source_item_key ON circulars(source, source_item_key);

CREATE TABLE IF NOT EXISTS scraper_checkpoints (
    source VARCHAR(20) PRIMARY KEY,
    last_run_date DATE NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    es_bloom_filter BYTEA,
    es_last_run_at TIMESTAMPTZ,
    es_records_processed INT DEFAULT 0
);
"""


@dataclass(slots=True)
class CircularRecord:
    id: UUID
    source: str
    circular_id: str
    source_item_key: str
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
    es_indexed_at: datetime | None = None
    es_chunk_count: int | None = None
    es_index_name: str | None = None


class CircularRepository:
    """Repository for circular records and scraper checkpoints."""

    def __init__(self, db_pool: Any = None) -> None:
        self.logger = logging.getLogger(__name__)
        self.db_pool = db_pool
        self._schema_initialized = False
        self._records_by_key: dict[tuple[str, str], CircularRecord] = {}
        self._records_by_source_item_key: dict[tuple[str, str], CircularRecord] = {}
        self._records_by_id: dict[UUID, CircularRecord] = {}
        self._checkpoints: dict[str, date] = {}

    def list_pending_es_records(self, limit: int = 100) -> list[CircularRecord]:
        if self.db_pool is not None:
            return self._list_pending_es_records_db(limit)

        pending = [
            record
            for record in self._records_by_id.values()
            if record.status == "FETCHED"
            and record.file_path is not None
            and record.es_indexed_at is None
        ]
        pending.sort(key=lambda record: (record.issue_date, record.created_at, record.id))
        return pending[:limit]

    def get_source_counts(
        self, sources: list[str] | tuple[str, ...]
    ) -> dict[str, int]:
        normalized_sources = tuple(source.upper() for source in sources)
        if self.db_pool is not None:
            return self._get_source_counts_db(normalized_sources)

        counts = {source: 0 for source in normalized_sources}
        for record in self._records_by_id.values():
            if record.source in counts:
                counts[record.source] += 1
        return counts

    def mark_es_indexed(
        self, record_id: UUID, chunk_count: int, index_name: str
    ) -> None:
        if self.db_pool is not None:
            self._mark_es_indexed_db(record_id, chunk_count, index_name)
            return

        record = self._records_by_id[record_id]
        updated = replace(
            record,
            es_indexed_at=datetime.now(timezone.utc),
            es_chunk_count=chunk_count,
            es_index_name=index_name,
            updated_at=datetime.now(timezone.utc),
        )
        self._store_record(updated)
        self.logger.info(
            "Circular ES metadata updated record_id=%s chunk_count=%s index_name=%s",
            record_id,
            chunk_count,
            index_name,
        )

    def clear_es_index_state(self, record_id: UUID) -> None:
        if self.db_pool is not None:
            self._clear_es_index_state_db(record_id)
            return

        record = self._records_by_id[record_id]
        updated = replace(
            record,
            es_indexed_at=None,
            es_chunk_count=None,
            es_index_name=None,
            updated_at=datetime.now(timezone.utc),
        )
        self._store_record(updated)
        self.logger.info("Cleared ES metadata record_id=%s", record_id)

    def clear_all_es_index_state(self) -> None:
        if self.db_pool is not None:
            self._clear_all_es_index_state_db()
            return

        now = datetime.now(timezone.utc)
        for record in list(self._records_by_id.values()):
            self._store_record(
                replace(
                    record,
                    es_indexed_at=None,
                    es_chunk_count=None,
                    es_index_name=None,
                    updated_at=now,
                )
            )
        self.logger.info("Cleared ES metadata for all circular records")

    def reset_bloom_state(self) -> None:
        if self.db_pool is not None:
            self._reset_bloom_state_db()
            return

        self.logger.info("Reset bloom/checkpoint state for all sources")

    def upsert_circular(self, circular: Circular) -> tuple[UUID, bool]:
        if self.db_pool is not None:
            return self._upsert_circular_db(circular)

        key = self._build_key(circular.source, circular.circular_id)
        source_item_key = self._normalize_source_item_key(
            circular.source_item_key or circular.url or circular.circular_id
        )
        now = datetime.now(timezone.utc)

        existing = self._records_by_source_item_key.get(
            self._build_source_item_key(circular.source, source_item_key)
        )
        if existing is None:
            existing = self._records_by_key.get(key)

        if existing is not None:
            updated = replace(
                existing,
                circular_id=circular.circular_id.upper(),
                source_item_key=source_item_key,
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
            source_item_key=source_item_key,
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
            es_indexed_at=None,
            es_chunk_count=None,
            es_index_name=None,
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
                    SELECT id, source, circular_id, source_item_key, full_reference,
                           department, title, issue_date, effective_date, url, pdf_url,
                           status, file_path, content_hash, error_message, detected_at,
                           created_at, updated_at, es_indexed_at, es_chunk_count,
                           es_index_name
                    FROM circulars
                    WHERE source = %s AND circular_id = %s
                    """,
                    (source_name, normalized_id),
                ).fetchone()
            return self._row_to_record(row)

        key = self._build_key(source, circular_id)
        return self._records_by_key.get(key)

    def get_record_by_id(self, record_id: UUID) -> CircularRecord | None:
        if self.db_pool is not None:
            self._ensure_schema()
            with self.db_pool.connection() as conn:
                row = conn.execute(
                    """
                    SELECT id, source, circular_id, source_item_key, full_reference,
                           department, title, issue_date, effective_date, url, pdf_url,
                           status, file_path, content_hash, error_message, detected_at,
                           created_at, updated_at, es_indexed_at, es_chunk_count,
                           es_index_name
                    FROM circulars
                    WHERE id = %s
                    """,
                    (record_id,),
                ).fetchone()
            return self._row_to_record(row)

        return self._records_by_id.get(record_id)

    def list_records(self) -> list[CircularRecord]:
        if self.db_pool is not None:
            self._ensure_schema()
            with self.db_pool.connection() as conn:
                rows = conn.execute(
                    """
                    SELECT id, source, circular_id, source_item_key, full_reference,
                           department, title, issue_date, effective_date, url, pdf_url,
                           status, file_path, content_hash, error_message, detected_at,
                           created_at, updated_at, es_indexed_at, es_chunk_count,
                           es_index_name
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

    def _build_source_item_key(
        self, source: str, source_item_key: str
    ) -> tuple[str, str]:
        return source.upper(), self._normalize_source_item_key(source_item_key)

    def _normalize_source_item_key(self, source_item_key: str) -> str:
        return source_item_key.strip()

    def _store_record(self, record: CircularRecord) -> None:
        previous = self._records_by_id.get(record.id)
        if previous is not None:
            old_key = self._build_key(previous.source, previous.circular_id)
            self._records_by_key.pop(old_key, None)
            old_source_item_key = self._build_source_item_key(
                previous.source, previous.source_item_key
            )
            self._records_by_source_item_key.pop(old_source_item_key, None)

        key = self._build_key(record.source, record.circular_id)
        source_item_key = self._build_source_item_key(
            record.source, record.source_item_key
        )
        self._records_by_key[key] = record
        self._records_by_source_item_key[source_item_key] = record
        self._records_by_id[record.id] = record

    def _ensure_schema(self) -> None:
        if self._schema_initialized or self.db_pool is None:
            return

        with self.db_pool.connection() as conn:
            conn.execute(self.schema_sql())
            conn.execute(
                """
                ALTER TABLE circulars
                ADD COLUMN IF NOT EXISTS source_item_key TEXT
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_circulars_source_item_key
                ON circulars(source, source_item_key)
                """
            )
        self._schema_initialized = True
        self.logger.info("Repository schema initialized")

    def _upsert_circular_db(self, circular: Circular) -> tuple[UUID, bool]:
        self._ensure_schema()
        source_name, normalized_id = self._build_key(circular.source, circular.circular_id)
        normalized_source_item_key = self._normalize_source_item_key(
            circular.source_item_key or circular.url or circular.circular_id
        )
        now = datetime.now(timezone.utc)

        with self.db_pool.connection() as conn:
            row = conn.execute(
                """
                SELECT id
                FROM circulars
                WHERE source = %s
                  AND (source_item_key = %s OR circular_id = %s)
                ORDER BY CASE
                    WHEN source_item_key = %s THEN 0
                    ELSE 1
                END
                LIMIT 1
                """,
                (
                    source_name,
                    normalized_source_item_key,
                    normalized_id,
                    normalized_source_item_key,
                ),
            ).fetchone()

            if row is not None:
                row = conn.execute(
                    """
                    UPDATE circulars
                    SET circular_id = %s,
                        source_item_key = %s,
                        full_reference = %s,
                        department = %s,
                        title = %s,
                        issue_date = %s,
                        effective_date = %s,
                        url = %s,
                        pdf_url = %s,
                        detected_at = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING id, source, circular_id, source_item_key, full_reference,
                              department, title, issue_date, effective_date, url, pdf_url,
                              status, file_path, content_hash, error_message, detected_at,
                              created_at, updated_at, es_indexed_at, es_chunk_count,
                              es_index_name
                    """,
                    (
                        normalized_id,
                        normalized_source_item_key,
                        circular.full_reference,
                        circular.department,
                        circular.title,
                        circular.issue_date,
                        circular.effective_date,
                        circular.url,
                        circular.pdf_url,
                        circular.detected_at or now,
                        row[0],
                    ),
                ).fetchone()
                inserted = False
            else:
                row = conn.execute(
                    """
                    INSERT INTO circulars (
                        source, circular_id, source_item_key, full_reference, department,
                        title, issue_date, effective_date, url, pdf_url, status, file_path,
                        content_hash, error_message, detected_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, source, circular_id, source_item_key, full_reference,
                              department, title, issue_date, effective_date, url, pdf_url,
                              status, file_path, content_hash, error_message, detected_at,
                              created_at, updated_at, es_indexed_at, es_chunk_count,
                              es_index_name
                    """,
                    (
                        source_name,
                        normalized_id,
                        normalized_source_item_key,
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
                inserted = True

        record = self._row_to_record(row)
        if record is None:
            raise RuntimeError("Failed to upsert circular record")

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
            source_item_key=row[3] or "",
            full_reference=row[4],
            department=row[5] or "",
            title=row[6],
            issue_date=row[7],
            effective_date=row[8],
            url=row[9] or "",
            pdf_url=row[10] or "",
            status=row[11],
            file_path=row[12],
            content_hash=row[13],
            error_message=row[14],
            detected_at=row[15],
            created_at=row[16],
            updated_at=row[17],
            es_indexed_at=row[18] if len(row) > 18 else None,
            es_chunk_count=row[19] if len(row) > 19 else None,
            es_index_name=row[20] if len(row) > 20 else None,
        )

    def _list_pending_es_records_db(self, limit: int) -> list[CircularRecord]:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, effective_date, url, pdf_url,
                       status, file_path, content_hash, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name
                FROM circulars
                WHERE status = 'FETCHED'
                  AND file_path IS NOT NULL
                  AND es_indexed_at IS NULL
                ORDER BY issue_date ASC, created_at ASC, id ASC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return [record for row in rows if (record := self._row_to_record(row))]

    def _get_source_counts_db(self, sources: tuple[str, ...]) -> dict[str, int]:
        self._ensure_schema()
        counts = {source: 0 for source in sources}
        if not sources:
            return counts

        with self.db_pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT source, COUNT(*)
                FROM circulars
                WHERE source = ANY(%s)
                GROUP BY source
                """,
                (list(sources),),
            ).fetchall()

        for source, count in rows:
            counts[source] = count
        return counts

    def _mark_es_indexed_db(
        self, record_id: UUID, chunk_count: int, index_name: str
    ) -> None:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                UPDATE circulars
                SET es_indexed_at = NOW(),
                    es_chunk_count = %s,
                    es_index_name = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (chunk_count, index_name, record_id),
            )
        self.logger.info(
            "Circular ES metadata updated record_id=%s chunk_count=%s index_name=%s",
            record_id,
            chunk_count,
            index_name,
        )

    def _clear_es_index_state_db(self, record_id: UUID) -> None:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                UPDATE circulars
                SET es_indexed_at = NULL,
                    es_chunk_count = NULL,
                    es_index_name = NULL,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (record_id,),
            )
        self.logger.info("Cleared ES metadata record_id=%s", record_id)

    def _clear_all_es_index_state_db(self) -> None:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                UPDATE circulars
                SET es_indexed_at = NULL,
                    es_chunk_count = NULL,
                    es_index_name = NULL,
                    updated_at = NOW()
                """
            )
        self.logger.info("Cleared ES metadata for all circular records")

    def _reset_bloom_state_db(self) -> None:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                UPDATE scraper_checkpoints
                SET es_bloom_filter = NULL,
                    es_last_run_at = NULL,
                    es_records_processed = 0,
                    updated_at = NOW()
                """
            )
        self.logger.info("Reset bloom/checkpoint state for all sources")
