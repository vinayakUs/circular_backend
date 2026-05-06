from __future__ import annotations

import logging
import threading
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID


@dataclass(slots=True)
class PropertyRecord:
    id: UUID
    name: str
    type: str
    archived: bool
    archived_at: datetime | None
    metadata: dict
    created_at: datetime
    updated_at: datetime


class PropertiesRepository:
    """Repository for generic properties (departments, categories, etc.)."""

    _schema_initialized: bool = False
    _schema_init_lock: threading.Lock = threading.Lock()

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("PropertiesRepository requires db_pool")
        self.logger = logging.getLogger(__name__)
        self.db_pool = db_pool

    def create(self, name: str, type: str, metadata: dict | None = None) -> PropertyRecord | None:
        self._ensure_schema()
        metadata = metadata or {}
        with self.db_pool.connection() as conn:
            existing = conn.execute(
                """
                SELECT id FROM properties
                WHERE name = %s AND type = %s AND archived = FALSE
                """,
                (name, type),
            ).fetchone()
            if existing:
                return None
            row = conn.execute(
                """
                INSERT INTO properties (name, type, metadata)
                VALUES (%s, %s, %s::jsonb)
                RETURNING id, name, type, archived, archived_at, metadata, created_at, updated_at
                """,
                (name, type, json.dumps(metadata)),
            ).fetchone()
        return self._row_to_record(row)

    def get_by_id(self, id: UUID) -> PropertyRecord | None:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            row = conn.execute(
                """
                SELECT id, name, type, archived, archived_at, metadata, created_at, updated_at
                FROM properties
                WHERE id = %s
                """,
                (id,),
            ).fetchone()
        return self._row_to_record(row)

    def list_by_type(self, type: str, include_archived: bool = False) -> list[PropertyRecord]:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            if include_archived:
                rows = conn.execute(
                    """
                    SELECT id, name, type, archived, archived_at, metadata, created_at, updated_at
                    FROM properties
                    WHERE type = %s
                    ORDER BY name
                    """,
                    (type,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, name, type, archived, archived_at, metadata, created_at, updated_at
                    FROM properties
                    WHERE type = %s AND archived = FALSE
                    ORDER BY name
                    """,
                    (type,),
                ).fetchall()
        return [record for row in rows if (record := self._row_to_record(row))]

    def archive(self, id: UUID) -> PropertyRecord | None:
        self._ensure_schema()
        with self.db_pool.connection() as conn:
            row = conn.execute(
                """
                UPDATE properties
                SET archived = TRUE, archived_at = NOW()
                WHERE id = %s
                RETURNING id, name, type, archived, archived_at, metadata, created_at, updated_at
                """,
                (id,),
            ).fetchone()
        return self._row_to_record(row)

    def _ensure_schema(self) -> None:
        if PropertiesRepository._schema_initialized:
            return

        with PropertiesRepository._schema_init_lock:
            if PropertiesRepository._schema_initialized:
                return

            with self.db_pool.connection() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS properties (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        name VARCHAR(255) NOT NULL,
                        type VARCHAR(50) NOT NULL,
                        archived BOOLEAN NOT NULL DEFAULT FALSE,
                        archived_at TIMESTAMPTZ,
                        metadata JSONB DEFAULT '{}',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                conn.execute("CREATE INDEX IF NOT EXISTS idx_properties_type ON properties(type)")
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_properties_type_name ON properties(type, name) WHERE archived = FALSE"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_properties_metadata_gin ON properties USING gin (metadata jsonb_path_ops)"
                )
            PropertiesRepository._schema_initialized = True
            self.logger.info("PropertiesRepository schema initialized")

    def _row_to_record(self, row: Any) -> PropertyRecord | None:
        if row is None:
            return None
        return PropertyRecord(
            id=row[0],
            name=row[1],
            type=row[2],
            archived=row[3],
            archived_at=row[4],
            metadata=row[5] if isinstance(row[5], dict) else {},
            created_at=row[6],
            updated_at=row[7],
        )