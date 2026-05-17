from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from ingestion.repository._uuid_utils import _raw_to_uuid, _uuid_to_raw


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


class AssetRepository:

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("AssetRepository requires db_pool")
        self.logger = __import__("logging").getLogger(__name__)
        self.db_pool = db_pool

    def replace_assets(self, circular_id: UUID, assets: list[CircularAsset]) -> list[CircularAssetRecord]:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM circular_assets WHERE circular_id = :1",
                (_uuid_to_raw(circular_id),),
            )
            for asset in assets:
                cursor.execute(
                    """
                    INSERT INTO circular_assets (
                        circular_id, asset_role, file_path, content_hash,
                        mime_type, archive_member_path, file_size_bytes
                    )
                    VALUES (:1, :2, :3, :4, :5, :6, :7)
                    """,
                    (
                        _uuid_to_raw(circular_id), asset.asset_role, asset.file_path,
                        asset.content_hash, asset.mime_type,
                        asset.archive_member_path, asset.file_size_bytes,
                    ),
                )
            conn.commit()
        self.logger.info("Replaced circular assets circular_id=%s asset_count=%s", circular_id, len(assets))
        return self.list_assets(circular_id)

    def list_assets(self, circular_id: UUID) -> list[CircularAssetRecord]:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, circular_id, asset_role, file_path, content_hash, mime_type,
                       archive_member_path, file_size_bytes, created_at, updated_at
                FROM circular_assets
                WHERE circular_id = :1
                ORDER BY CASE asset_role
                        WHEN 'original_pdf' THEN 0
                        WHEN 'original_zip' THEN 1
                        WHEN 'extracted_pdf' THEN 2
                        ELSE 9
                    END,
                    NVL(archive_member_path, ''), file_path
                """,
                (_uuid_to_raw(circular_id),),
            )
            rows = cursor.fetchall()
        return [a for row in rows if (a := self._row_to_asset_record(row))]

    def get_primary_asset(self, circular_id: UUID) -> CircularAssetRecord | None:
        assets = self.list_assets(circular_id)
        return assets[0] if assets else None

    def _row_to_asset_record(self, row: Any) -> CircularAssetRecord | None:
        if row is None:
            return None
        return CircularAssetRecord(
            id=_raw_to_uuid(row[0]),
            circular_id=_raw_to_uuid(row[1]),
            asset_role=row[2],
            file_path=row[3],
            content_hash=row[4],
            mime_type=row[5],
            archive_member_path=row[6],
            file_size_bytes=int(row[7]) if row[7] is not None else None,
            created_at=row[8],
            updated_at=row[9],
        )