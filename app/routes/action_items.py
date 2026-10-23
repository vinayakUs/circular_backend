"""
Action items routes: list action items extracted from circulars.

GET /api/action-items — paginated list with filters
"""
from __future__ import annotations

import logging
from datetime import date
from uuid import UUID

from flask import request

from db.postgres_client import get_postgres_client
from ingestion.repository.action_item_repository import (
    ActionItemRepository,
)
from app.dto.action_item_dto import (
    EnrichedActionItemDTO,
    EnrichedActionItemListResponseDTO,
)
from utils.relative_deadline import compute_relative_deadline

logger = logging.getLogger(__name__)


def _parse_int_arg(name: str, default: int) -> int:
    """Parse an int query param. Empty/missing → default. Bad value → ValueError."""
    raw = request.args.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {raw!r}")


def _parse_date_arg(name: str) -> date | None:
    """Parse an ISO date query param. Empty/missing → None. Bad value → ValueError."""
    raw = request.args.get(name, "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        raise ValueError(f"{name} must be ISO format YYYY-MM-DD, got {raw!r}")


def register_routes(app) -> None:

    @app.get("/api/action-items")
    def list_action_items():
        """List action items with optional filters.

        Query params (all optional):
            circular_id: filter to one circular (UUID)
            start_date:  ISO date (YYYY-MM-DD) — inclusive lower bound on deadline
            end_date:    ISO date (YYYY-MM-DD) — inclusive upper bound on deadline
            limit:       page size (default 20, max 200)
            offset:      pagination offset (default 0)

        Returns: {action_items: [...], total, limit, offset}
        """
        # ── Parse ints (handles "offset=" empty as 0) ──────────
        try:
            limit = _parse_int_arg("limit", default=20)
            offset = _parse_int_arg("offset", default=0)
        except ValueError as e:
            return {"error": str(e)}, 400

        if limit < 1 or limit > 200:
            return {"error": "limit must be between 1 and 200."}, 400
        if offset < 0:
            return {"error": "offset must be >= 0."}, 400

        # ── Parse UUID ─────────────────────────────────────────
        raw_circular_id = request.args.get("circular_id", "").strip() or None
        circular_id: UUID | None = None
        if raw_circular_id:
            try:
                circular_id = UUID(raw_circular_id)
            except ValueError:
                return {"error": "Invalid circular_id format. Must be a UUID."}, 400

        # ── Parse dates ────────────────────────────────────────
        try:
            start_date = _parse_date_arg("start_date")
            end_date = _parse_date_arg("end_date")
        except ValueError as e:
            return {"error": str(e)}, 400

        # ── Query ──────────────────────────────────────────────
        repo = ActionItemRepository(db_pool=get_postgres_client().get_pool())
        try:
            items, total = repo.get_action_items(
                circular_id=circular_id,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                offset=offset,
            )
        except ValueError as e:
            return {"error": str(e)}, 400

        # ── Enrich with relative-deadline fields (presentation layer) ──
        enriched = [
            EnrichedActionItemDTO.from_repository(item, *compute_relative_deadline(item.deadline))
            for item in items
        ]

        return EnrichedActionItemListResponseDTO(
            action_items=enriched,
            total=total,
            limit=limit,
            offset=offset,
        ).model_dump()