from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import logging
from pathlib import Path
import threading
from typing import Any
from uuid import UUID
from utils.utils import render_sql

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
    applicable_to_nse BOOLEAN NOT NULL DEFAULT FALSE,
    url TEXT,
    pdf_url TEXT,
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
CREATE UNIQUE INDEX IF NOT EXISTS idx_circulars_source_item_key ON circulars(source, source_item_key);

CREATE TABLE IF NOT EXISTS circular_assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    circular_id UUID NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
    asset_role VARCHAR(30) NOT NULL,
    file_path VARCHAR(500) NOT NULL,
    content_hash VARCHAR(64),
    mime_type VARCHAR(100),
    archive_member_path TEXT,
    file_size_bytes BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_circular_assets_circular_id
    ON circular_assets(circular_id);
CREATE INDEX IF NOT EXISTS idx_circular_assets_circular_role
    ON circular_assets(circular_id, asset_role);
CREATE UNIQUE INDEX IF NOT EXISTS idx_circular_assets_identity
    ON circular_assets(circular_id, asset_role, COALESCE(archive_member_path, ''));

CREATE TABLE IF NOT EXISTS scraper_checkpoints (
    source VARCHAR(20) PRIMARY KEY,
    last_run_date DATE NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    es_bloom_filter BYTEA,
    es_last_run_at TIMESTAMPTZ,
    es_records_processed INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS processing_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    circular_id UUID NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
    processor_name VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(circular_id, processor_name)
);

CREATE TABLE IF NOT EXISTS action_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    circular_id UUID NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
    action_item TEXT NOT NULL,
    deadline DATE,
    priority VARCHAR(20),
    persona VARCHAR(50),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS circular_references (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_circular_id UUID NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
    reference_circular_no VARCHAR(100) NOT NULL,
    reference_circular_id UUID,
    relationship_nature VARCHAR(50) NOT NULL,
    ref_circular_exist BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(source_circular_id, reference_circular_no)
);

CREATE INDEX IF NOT EXISTS idx_circular_refs_source
    ON circular_references(source_circular_id);
CREATE INDEX IF NOT EXISTS idx_circular_refs_ref_id
    ON circular_references(reference_circular_id);
CREATE INDEX IF NOT EXISTS idx_circular_refs_nature
    ON circular_references(relationship_nature);

-- Generic properties table for departments, categories, regions, and other entity types
CREATE TABLE IF NOT EXISTS properties (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    archived BOOLEAN NOT NULL DEFAULT FALSE,
    archived_at TIMESTAMPTZ,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_properties_type ON properties(type);
CREATE INDEX IF NOT EXISTS idx_properties_type_name ON properties(type, name) WHERE archived = FALSE;
CREATE INDEX IF NOT EXISTS idx_properties_metadata_gin ON properties USING gin (metadata jsonb_path_ops);

-- Many-to-many mapping between circulars and departments/experts
CREATE TABLE IF NOT EXISTS circular_department_mapping (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    circular_id UUID NOT NULL REFERENCES circulars(id) ON DELETE CASCADE,
    department_id UUID NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
    expert_name VARCHAR(255) NOT NULL,
    highlight_text TEXT NOT NULL,
    highlights JSONB DEFAULT '[]',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (circular_id, department_id, expert_name)
);

CREATE INDEX IF NOT EXISTS idx_cdm_circular_id ON circular_department_mapping(circular_id);
CREATE INDEX IF NOT EXISTS idx_cdm_department_id ON circular_department_mapping(department_id);
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
    applicable_to_nse: bool
    url: str
    pdf_url: str
    status: str
    content_hash: str | None
    error_message: str | None
    detected_at: datetime
    created_at: datetime
    updated_at: datetime
    es_indexed_at: datetime | None = None
    es_chunk_count: int | None = None
    es_index_name: str | None = None


@dataclass(slots=True)
class CircularAsset:
    asset_role: str
    file_path: str
    content_hash: str | None = None
    mime_type: str | None = None
    archive_member_path: str | None = None
    file_size_bytes: int | None = None


@dataclass(slots=True)
class CircularAssetRecord:
    id: UUID
    circular_id: UUID
    asset_role: str
    file_path: str
    content_hash: str | None
    mime_type: str | None
    archive_member_path: str | None
    file_size_bytes: int | None
    created_at: datetime
    updated_at: datetime


class CircularRepository:
    """Repository for circular records and scraper checkpoints."""

    _schema_initialized: bool = False
    _schema_init_lock: threading.Lock = threading.Lock()

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("CircularRepository requires db_pool")
        self.logger = logging.getLogger(__name__)
        self.db_pool = db_pool

    def list_pending_es_records(self, limit: int = 100) -> list[CircularRecord]:
        return self._list_pending_es_records_db(limit)

    def get_source_counts(
        self, sources: list[str] | tuple[str, ...]
    ) -> dict[str, int]:
        normalized_sources = tuple(source.upper() for source in sources)
        return self._get_source_counts_db(normalized_sources)

    def mark_es_indexed(
        self, record_id: UUID, chunk_count: int, index_name: str
    ) -> None:
        self._mark_es_indexed_db(record_id, chunk_count, index_name)

    def clear_es_index_state(self, record_id: UUID) -> None:
        self._clear_es_index_state_db(record_id)

    def clear_all_es_index_state(self) -> None:
        self._clear_all_es_index_state_db()

    def reset_bloom_state(self) -> None:
        self._reset_bloom_state_db()

    def upsert_circular(self, circular: Circular) -> tuple[UUID, bool]:
        return self._upsert_circular_db(circular)

    def replace_assets(
        self, circular_id: UUID, assets: list[CircularAsset]
    ) -> list[CircularAssetRecord]:
        return self._replace_assets_db(circular_id, assets)

    def list_assets(self, circular_id: UUID) -> list[CircularAssetRecord]:
        return self._list_assets_db(circular_id)

    def get_primary_asset(self, circular_id: UUID) -> CircularAssetRecord | None:
        return self._get_primary_asset_db(circular_id)

    def update_status(
        self, record_id: UUID, status: str, error_message: str | None = None
    ) -> None:
        self._update_status_db(record_id, status, error_message)

    def update_applicable_to_nse(self, record_id: UUID, applicable: bool) -> None:
        self._update_applicable_to_nse_db(record_id, applicable)

    def get_checkpoint(self, source: str) -> date | None:
        return self._get_checkpoint_db(source)

    def set_checkpoint(self, source: str, run_date: date) -> None:
        self._set_checkpoint_db(source, run_date)

    def exists_by_source_and_id(self, source: str, circular_id: str) -> bool:
        return self.get_record(source, circular_id) is not None

    def get_record(self, source: str, circular_id: str) -> CircularRecord | None:
        self._ensure_schema()
        source_name, normalized_id = self._build_key(source, circular_id)
        with self.db_pool.connection() as conn:
            row = conn.execute(
                """
                SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, applicable_to_nse, url, pdf_url,
                       status, content_hash, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name
                FROM circulars
                WHERE source = %s AND circular_id = %s
                """,
                (source_name, normalized_id),
            ).fetchone()
        return self._row_to_record(row)

    def get_record_by_id(self, record_id: UUID) -> CircularRecord | None:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            row = conn.execute(
                """
                SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, applicable_to_nse, url, pdf_url,
                       status, content_hash, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name
                FROM circulars
                WHERE id = %s
                """,
                (record_id,),
            ).fetchone()
        return self._row_to_record(row)

    def get_record_by_circular_id(
        self, circular_id: str, source: str | None = None
    ) -> CircularRecord | None:
        self._ensure_schema()
        normalized_id = circular_id.upper()
        normalized_source = source.upper() if source else None

        with self.db_pool.connection() as conn:
            if normalized_source:
                row = conn.execute(
                    """
                    SELECT id, source, circular_id, source_item_key, full_reference,
                           department, title, issue_date, applicable_to_nse, url, pdf_url,
                           status, content_hash, error_message, detected_at,
                           created_at, updated_at, es_indexed_at, es_chunk_count,
                           es_index_name
                    FROM circulars
                    WHERE circular_id = %s AND source = %s
                    ORDER BY issue_date DESC, updated_at DESC, created_at DESC
                    LIMIT 1
                    """,
                    (normalized_id, normalized_source),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT id, source, circular_id, source_item_key, full_reference,
                           department, title, issue_date, applicable_to_nse, url, pdf_url,
                           status, content_hash, error_message, detected_at,
                           created_at, updated_at, es_indexed_at, es_chunk_count,
                           es_index_name
                    FROM circulars
                    WHERE circular_id = %s
                    ORDER BY issue_date DESC, updated_at DESC, created_at DESC
                    LIMIT 1
                    """,
                    (normalized_id,),
                ).fetchone()
        return self._row_to_record(row)

    def get_record_by_full_reference(self, reference: str) -> CircularRecord | None:
        """Look up a circular by full_reference.

        Returns CircularRecord if found, None if not found.
        """
        self._ensure_schema()
        normalized = reference.upper()
        self.logger.info('''SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, applicable_to_nse, url, pdf_url,
                       status, content_hash, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name
                FROM circulars
                WHERE UPPER(full_reference) = %s
                LIMIT 1''', normalized)

        with self.db_pool.connection() as conn:
            row = conn.execute(
                """
                SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, applicable_to_nse, url, pdf_url,
                       status, content_hash, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name
                FROM circulars
                WHERE UPPER(full_reference) = %s
                LIMIT 1
                """,
                (normalized,),
            ).fetchone()
            if row:
                return self._row_to_record(row)
            return None

    def list_records(self) -> list[CircularRecord]:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, applicable_to_nse, url, pdf_url,
                       status, content_hash, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name
                FROM circulars
                ORDER BY source, circular_id
                """
            ).fetchall()
        return [record for row in rows if (record := self._row_to_record(row))]

    def list_paginated(
        self,
        limit: int = 20,
        offset: int = 0,
        source: str | None = None,
        from_date: date | None = None,
        to_date: date | None = None,
        applicable_to_nse: bool | None = None,
    ) -> tuple[list[CircularRecord], int]:
        self._ensure_schema()
        args: list = []
        where_clauses = []
        if source:
            where_clauses.append("source = %s")
            args.append(source.upper())
        if from_date:
            where_clauses.append("issue_date >= %s")
            args.append(from_date)
        if to_date:
            where_clauses.append("issue_date <= %s")
            args.append(to_date)
        if applicable_to_nse is not None:
            where_clauses.append("applicable_to_nse = %s")
            args.append(applicable_to_nse)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        count_sql = f"SELECT COUNT(*) FROM circulars WHERE {where_sql}"
        total_sql = f"""
            SELECT id, source, circular_id, source_item_key, full_reference,
                   department, title, issue_date, applicable_to_nse, url, pdf_url,
                   status, content_hash, error_message, detected_at,
                   created_at, updated_at, es_indexed_at, es_chunk_count,
                   es_index_name
            FROM circulars
            WHERE {where_sql}
            ORDER BY issue_date DESC, created_at DESC, id DESC
            LIMIT %s OFFSET %s
        """
        args_with_pagination = [*args, limit, offset]

        
        with self.db_pool.connection() as conn:
            self.logger.debug(
                "Executing paginated list query: %s \nArgs: %s",
                render_sql(count_sql, args),
                args) 
        
            total_row = conn.execute(count_sql, args).fetchone()
            total = total_row[0] if total_row else 0
            self.logger.debug(
                "Executing paginated list query: %s \nArgs: %s",
                render_sql(total_sql, args_with_pagination),
                args_with_pagination) 
            rows = conn.execute(total_sql, args_with_pagination).fetchall()
            records = [record for row in rows if (record := self._row_to_record(row))]

        return records, total

    def schema_sql(self) -> str:
        return POSTGRES_SCHEMA_SQL.strip()

    def export_schema(self, target_path: str | Path) -> Path:
        path = Path(target_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{self.schema_sql()}\n")
        return path

    def _build_key(self, source: str, circular_id: str) -> tuple[str, str]:
        return source.upper(), circular_id.upper()

    def _normalize_source_item_key(self, source_item_key: str) -> str:
        return source_item_key.strip()

    def _ensure_schema(self) -> None:
        if CircularRepository._schema_initialized:
            return

        with CircularRepository._schema_init_lock:
            if CircularRepository._schema_initialized:
                return

            with self.db_pool.connection() as conn:
                conn.execute(self.schema_sql())
            CircularRepository._schema_initialized = True
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
                        applicable_to_nse = %s,
                        url = %s,
                        pdf_url = %s,
                        detected_at = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING id, source, circular_id, source_item_key, full_reference,
                              department, title, issue_date, applicable_to_nse, url, pdf_url,
                              status, content_hash, error_message, detected_at,
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
                        circular.applicable_to_nse,
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
                        title, issue_date, applicable_to_nse, url, pdf_url, status,
                        content_hash, error_message, detected_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, source, circular_id, source_item_key, full_reference,
                              department, title, issue_date, applicable_to_nse, url, pdf_url,
                              status, content_hash, error_message, detected_at,
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
                        circular.applicable_to_nse,
                        circular.url,
                        circular.pdf_url,
                        "DISCOVERED",
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

    def _replace_assets_db(
        self, circular_id: UUID, assets: list[CircularAsset]
    ) -> list[CircularAssetRecord]:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                DELETE FROM circular_assets
                WHERE circular_id = %s
                """,
                (circular_id,),
            )
            for asset in assets:
                conn.execute(
                    """
                    INSERT INTO circular_assets (
                        circular_id,
                        asset_role,
                        file_path,
                        content_hash,
                        mime_type,
                        archive_member_path,
                        file_size_bytes
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        circular_id,
                        asset.asset_role,
                        asset.file_path,
                        asset.content_hash,
                        asset.mime_type,
                        asset.archive_member_path,
                        asset.file_size_bytes,
                    ),
                )
        self.logger.info(
            "Replaced circular assets circular_id=%s asset_count=%s",
            circular_id,
            len(assets),
        )
        return self._list_assets_db(circular_id)

    def _list_assets_db(self, circular_id: UUID) -> list[CircularAssetRecord]:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, circular_id, asset_role, file_path, content_hash, mime_type,
                       archive_member_path, file_size_bytes, created_at, updated_at
                FROM circular_assets
                WHERE circular_id = %s
                ORDER BY
                    CASE asset_role
                        WHEN 'original_pdf' THEN 0
                        WHEN 'original_zip' THEN 1
                        WHEN 'extracted_pdf' THEN 2
                        ELSE 9
                    END,
                    COALESCE(archive_member_path, ''),
                    file_path
                """,
                (circular_id,),
            ).fetchall()
        return [
            asset
            for row in rows
            if (asset := self._row_to_asset_record(row)) is not None
        ]

    def _get_primary_asset_db(self, circular_id: UUID) -> CircularAssetRecord | None:
        assets = self._list_assets_db(circular_id)
        if not assets:
            return None
        return assets[0]

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
            applicable_to_nse=row[8],
            url=row[9] or "",
            pdf_url=row[10] or "",
            status=row[11],
            content_hash=row[12],
            error_message=row[13],
            detected_at=row[14],
            created_at=row[15],
            updated_at=row[16],
            es_indexed_at=row[17] if len(row) > 17 else None,
            es_chunk_count=row[18] if len(row) > 18 else None,
            es_index_name=row[19] if len(row) > 19 else None,
        )

    def _row_to_asset_record(self, row: Any) -> CircularAssetRecord | None:
        if row is None:
            return None

        return CircularAssetRecord(
            id=row[0],
            circular_id=row[1],
            asset_role=row[2],
            file_path=row[3],
            content_hash=row[4],
            mime_type=row[5],
            archive_member_path=row[6],
            file_size_bytes=row[7],
            created_at=row[8],
            updated_at=row[9],
        )

    def _list_pending_es_records_db(self, limit: int) -> list[CircularRecord]:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, applicable_to_nse, url, pdf_url,
                       status, content_hash, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name
                FROM circulars
                WHERE status = 'FETCHED'
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

    def save_expert_mapping(
        self,
        circular_id: UUID,
        dept_id: UUID,
        title: str,
        text: str,
        highlights: list[dict],
    ) -> UUID:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            row = conn.execute(
                """
                INSERT INTO circular_department_mapping
                    (circular_id, department_id, expert_name, highlight_text, highlights)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (circular_id, dept_id, title, text, json.dumps(highlights)),
            ).fetchone()
        return row[0]

    def update_expert_mapping(
        self,
        row_id: UUID,
        dept_id: UUID,
        title: str,
        text: str,
        highlights: list[dict],
    ) -> bool:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            row = conn.execute(
                """
                UPDATE circular_department_mapping
                SET department_id = %s,
                    expert_name = %s,
                    highlight_text = %s,
                    highlights = %s,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING id
                """,
                (dept_id, title, text, json.dumps(highlights), row_id),
            ).fetchone()
        return row is not None

    def delete_expert_mapping(self, row_id: UUID) -> bool:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            row = conn.execute(
                """
                DELETE FROM circular_department_mapping
                WHERE id = %s
                RETURNING id
                """,
                (row_id,),
            ).fetchone()
        return row is not None

    def get_expert_mappings_for_circular(self, circular_id: UUID) -> list[dict]:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, circular_id, department_id, expert_name, highlight_text, highlights, created_at, updated_at
                FROM circular_department_mapping
                WHERE circular_id = %s
                ORDER BY created_at
                """,
                (circular_id,),
            ).fetchall()
        return [
            {
                "id": str(r[0]),
                "circular_id": str(r[1]),
                "dept_id": str(r[2]),
                "title": r[3],
                "text": r[4],
                "highlights": r[5] or [],
                "created_at": r[6].isoformat() if r[6] else None,
                "updated_at": r[7].isoformat() if r[7] else None,
            }
            for r in rows
        ]

    def get_experts_by_department(
        self,
        department_id: UUID | None,
        source: str | None,
        from_date: date | None,
        to_date: date | None,
        full_circular_no: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict], int]:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            conditions = []
            params = []

            if department_id:
                conditions.append("cdm.department_id = %s")
                params.append(department_id)

            if source:
                conditions.append("c.source = %s")
                params.append(source)

            if from_date:
                conditions.append("c.issue_date >= %s")
                params.append(from_date)

            if to_date:
                conditions.append("c.issue_date <= %s")
                params.append(to_date)

            if full_circular_no:
                conditions.append("UPPER(c.full_reference) LIKE UPPER(%s)")
                params.append(f"%{full_circular_no}%")

            where_clause = " AND ".join(conditions) if conditions else "1=1"

            count_query = f"""
                SELECT COUNT(*)
                FROM circular_department_mapping cdm
                JOIN circulars c ON cdm.circular_id = c.id
                WHERE {where_clause}
            """
            total_row = conn.execute(count_query, tuple(params)).fetchone()
            total = total_row[0] if total_row else 0

            query = f"""
                SELECT
                    cdm.id,
                    cdm.expert_name,
                    cdm.highlight_text,
                    c.id as circ_id,
                    c.full_reference,
                    c.source,
                    c.issue_date,
                    c.title
                FROM circular_department_mapping cdm
                JOIN circulars c ON cdm.circular_id = c.id
                WHERE {where_clause}
                ORDER BY c.issue_date DESC
                LIMIT %s OFFSET %s
            """
            params_extended = list(params) + [limit, offset]
            rows = conn.execute(query, tuple(params_extended)).fetchall()

            experts = [
                {
                    "id": str(r[0]),
                    "expert_name": r[1],
                    "highlight_text": r[2],
                    "circular": {
                        "id": str(r[3]),
                        "full_reference": r[4],
                        "source": r[5],
                        "issue_date": r[6].isoformat() if r[6] else None,
                        "title": r[7],
                    },
                }
                for r in rows
            ]
            return experts, total

    def _update_applicable_to_nse_db(self, record_id: UUID, applicable: bool) -> None:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            conn.execute(
                """
                UPDATE circulars
                SET applicable_to_nse = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (applicable, record_id),
            )
        self.logger.info("Updated applicable_to_nse record_id=%s applicable=%s", record_id, applicable)
