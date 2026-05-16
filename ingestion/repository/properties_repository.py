from __future__ import annotations

import logging
import threading
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from ingestion.repository.circular_repository import _raw_to_uuid, _uuid_to_raw


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
        with self.db_pool.acquire_connection() as conn:
            existing = conn.execute(
                """
                SELECT id FROM properties
                WHERE name = :1 AND type = :2 AND archived = 0
                """,
                (name, type),
            ).fetchone()
            if existing:
                return None
            new_id = _uuid_to_raw(UUID.uuid4())
            row = conn.execute(
                """
                INSERT INTO properties (id, name, type, metadata)
                VALUES (:1, :2, :3, :4)
                """,
                (new_id, name, type, json.dumps(metadata)),
            )
            conn.commit()
            row = conn.execute(
                """
                SELECT id, name, type, archived, archived_at, metadata, created_at, updated_at
                FROM properties WHERE id = :1
                """,
                (new_id,),
            ).fetchone()
        return self._row_to_record(row)

    def get_by_id(self, id: UUID) -> PropertyRecord | None:
        self._ensure_schema()
        id_raw = _uuid_to_raw(id)
        with self.db_pool.acquire_connection() as conn:
            row = conn.execute(
                """
                SELECT id, name, type, archived, archived_at, metadata, created_at, updated_at
                FROM properties
                WHERE id = :1
                """,
                (id_raw,),
            ).fetchone()
        return self._row_to_record(row)

    def list_by_type(self, type: str, include_archived: bool = False) -> list[PropertyRecord]:
        self._ensure_schema()
        with self.db_pool.acquire_connection() as conn:
            if include_archived:
                rows = conn.execute(
                    """
                    SELECT id, name, type, archived, archived_at, metadata, created_at, updated_at
                    FROM properties
                    WHERE type = :1
                    ORDER BY name
                    """,
                    (type,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, name, type, archived, archived_at, metadata, created_at, updated_at
                    FROM properties
                    WHERE type = :1 AND archived = 0
                    ORDER BY name
                    """,
                    (type,),
                ).fetchall()
        return [record for row in rows if (record := self._row_to_record(row))]

    def archive(self, id: UUID) -> PropertyRecord | None:
        self._ensure_schema()
        id_raw = _uuid_to_raw(id)
        with self.db_pool.acquire_connection() as conn:
            conn.execute(
                """
                UPDATE properties
                SET archived = 1, archived_at = SYSDATE
                WHERE id = :1
                """,
                (id_raw,),
            )
            conn.commit()
            row = conn.execute(
                """
                SELECT id, name, type, archived, archived_at, metadata, created_at, updated_at
                FROM properties WHERE id = :1
                """,
                (id_raw,),
            ).fetchone()
        return self._row_to_record(row)

    def _ensure_schema(self) -> None:
        if PropertiesRepository._schema_initialized:
            return

        with PropertiesRepository._schema_init_lock:
            if PropertiesRepository._schema_initialized:
                return

            with self.db_pool.acquire_connection() as conn:
                conn.execute(
                    """
                    CREATE TABLE properties (
                        id RAW(16) PRIMARY KEY,
                        name VARCHAR2(255) NOT NULL,
                        type VARCHAR2(50) NOT NULL,
                        archived NUMBER(1) NOT NULL DEFAULT 0,
                        archived_at TIMESTAMP WITH TIME ZONE,
                        metadata CLOB DEFAULT EMPTY_CLOB(),
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT SYSDATE,
                        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT SYSDATE
                    )
                    """
                )
                conn.execute("CREATE INDEX idx_properties_type ON properties(type)")
                conn.execute(
                    "CREATE INDEX idx_properties_type_name ON properties(type, name)"
                )
            PropertiesRepository._schema_initialized = True
            self.logger.info("PropertiesRepository schema initialized")

    def _row_to_record(self, row: Any) -> PropertyRecord | None:
        if row is None:
            return None
        metadata = row[5]
        if isinstance(metadata, str):
            metadata = json.loads(metadata) if metadata else {}
        elif metadata is None:
            metadata = {}
        return PropertyRecord(
            id=_raw_to_uuid(row[0]),
            name=row[1],
            type=row[2],
            archived=bool(row[3]),
            archived_at=row[4],
            metadata=metadata,
            created_at=row[6],
            updated_at=row[7],
        )