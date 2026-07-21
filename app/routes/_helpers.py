"""
Cross-cutting serializers shared by route modules.

Anything reused across modules (e.g. record → dict) lives here so that
individual route files stay self-contained.
"""
from __future__ import annotations

from typing import Any


def serialize_circular_record(record: Any) -> dict[str, Any]:
    return {
        "circular_id": record.circular_id,
        "department": record.department,
        "id": str(record.id),
        "issue_date": record.issue_date.isoformat(),
        "full_reference": record.full_reference,
        "url": record.url,
        "source": record.source,
        "title": record.title,
        "status": record.status,
        "applicable_to_nse": record.applicable_to_nse,
    }


def serialize_circular_asset(asset: Any) -> dict[str, Any]:
    return {
        "id": str(asset.id),
        "circular_id": str(asset.circular_id),
        "asset_role": asset.asset_role,
        "file_path": asset.file_path,
        "content_hash": asset.content_hash,
        "mime_type": asset.mime_type,
        "archive_member_path": asset.archive_member_path,
        "file_size_bytes": asset.file_size_bytes,
        "created_at": asset.created_at.isoformat(),
        "updated_at": asset.updated_at.isoformat(),
    }