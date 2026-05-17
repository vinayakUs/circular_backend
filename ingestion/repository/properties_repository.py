from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
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

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("PropertiesRepository requires db_pool")
        self.logger = __import__("logging").getLogger(__name__)
        self.db_pool = db_pool

    def create(self, name: str, type: str, metadata: dict | None = None) -> PropertyRecord | None:
        metadata = metadata or {}
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM properties WHERE name = :1 AND type = :2 AND archived = 0",
                (name, type),
            )
            if cursor.fetchone():
                return None
            cursor.execute(
                "INSERT INTO properties (name, type, metadata) VALUES (:1, :2, :3)",
                (name, type, json.dumps(metadata)),
            )
            conn.commit()
            cursor.execute(
                "SELECT id, name, type, archived, archived_at, metadata, created_at, updated_at "
                "FROM properties WHERE name = :1 AND type = :2",
                (name, type),
            )
            row = cursor.fetchone()
        return self._row_to_record(row)

    def get_by_id(self, id: UUID) -> PropertyRecord | None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, name, type, archived, archived_at, metadata, created_at, updated_at "
                "FROM properties WHERE id = :1",
                (_uuid_to_raw(id),),
            )
            row = cursor.fetchone()
        return self._row_to_record(row)

    def list_by_type(self, type: str, include_archived: bool = False) -> list[PropertyRecord]:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            if include_archived:
                cursor.execute(
                    "SELECT id, name, type, archived, archived_at, metadata, created_at, updated_at "
                    "FROM properties WHERE type = :1 ORDER BY name",
                    (type,),
                )
            else:
                cursor.execute(
                    "SELECT id, name, type, archived, archived_at, metadata, created_at, updated_at "
                    "FROM properties WHERE type = :1 AND archived = 0 ORDER BY name",
                    (type,),
                )
            rows = cursor.fetchall()
        return [r for row in rows if (r := self._row_to_record(row))]

    def archive(self, id: UUID) -> PropertyRecord | None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE properties SET archived = 1, archived_at = SYSTIMESTAMP WHERE id = :1",
                (_uuid_to_raw(id),),
            )
            conn.commit()
            cursor.execute(
                "SELECT id, name, type, archived, archived_at, metadata, created_at, updated_at "
                "FROM properties WHERE id = :1",
                (_uuid_to_raw(id),),
            )
            row = cursor.fetchone()
        return self._row_to_record(row)

    def _row_to_record(self, row: Any) -> PropertyRecord | None:
        if row is None:
            return None
        return PropertyRecord(
            id=_raw_to_uuid(row[0]),
            name=row[1],
            type=row[2],
            archived=bool(row[3]),
            archived_at=row[4],
            metadata=row[5] if isinstance(row[5], dict) else {},
            created_at=row[6],
            updated_at=row[7],
        )