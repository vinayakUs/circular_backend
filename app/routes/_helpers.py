"""
Cross-cutting serializers shared by route modules.

Anything reused across modules (e.g. record → dict) lives here so that
individual route files stay self-contained.
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any, Optional


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
        "is_active": record.is_active
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


def _parse_iso_date(
    raw: Optional[str], field_name: str
) -> Optional[date] | tuple[dict, int]:
    """Parse a YYYY-MM-DD string into a date, or return a (body, 400) tuple.

    Returns ``None`` when ``raw`` is None/empty. Returns the
    ``({"error": ...}, 400)`` tuple when ``raw`` is non-empty but
    malformed, so the caller can ``return`` it directly.
    """
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return {"error": f"{field_name} must be YYYY-MM-DD"}, 400


def _parse_highlights(value: Any) -> list[dict]:
    """Normalize a ``highlights`` payload to a list of dicts.

    Accepts either:
    - A JSON string (decoded via ``json.loads``).
    - An already-parsed list (returned as-is, or ``[]`` if the list is empty/None).

    Returns an empty list when ``value`` is None or missing. Other malformed
    inputs (e.g. a non-list, non-string) bubble up as a ``json.JSONDecodeError``
    or ``TypeError`` — the caller should catch at the request boundary.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return json.loads(value)
    return value
