"""Cross-persona deadlines route.

GET /api/deadlines — fetch action_item deadlines across ALL personas / sources.
                      Used by the "Upcoming Changes" page where deadlines are
                      intentionally not scoped to one persona.

Returns the same shape as /api/personas/<slug>/deadlines so the frontend
can use one parser for both.
"""
from __future__ import annotations

from datetime import date, timedelta
import logging

from flask import request

from db.postgres_client import get_postgres_client
from ingestion.repository.action_item_repository import ActionItemRepository

logger = logging.getLogger(__name__)


def register_routes(app) -> None:

    @app.get("/api/deadlines")
    def list_global_deadlines():
        """Fetch action_item deadlines across all personas. Unpaginated by default.

        Query params (all optional):
            from:      ISO date YYYY-MM-DD (default: today)
            to:        ISO date YYYY-MM-DD (default: from + 60 days)
            exchange:  CSV (NSE,BSE,MCX,SEBI,MSE,NSDL,CDSL,...)
            limit:     int — OMIT (the default) to get every match in the
                       date range; set it to turn pagination on
            offset:    int (default 0, min 0)

        Returns: {items: [...], total, limit, offset}
            `limit` echoes the requested page size, or null when none was
            requested; without pagination `total` is simply len(items).
        """
        # ── 1. Parse date range (defaults: today → today+60d) ───
        raw_from = request.args.get("from", "").strip()
        if raw_from:
            try:
                d_from = date.fromisoformat(raw_from)
            except ValueError:
                return {"error": f"from must be YYYY-MM-DD, got {raw_from!r}"}, 400
        else:
            d_from = date.today()

        raw_to = request.args.get("to", "").strip()
        if raw_to:
            try:
                d_to = date.fromisoformat(raw_to)
            except ValueError:
                return {"error": f"to must be YYYY-MM-DD, got {raw_to!r}"}, 400
        else:
            d_to = d_from + timedelta(days=60)

        if d_to < d_from:
            return {"error": "to must be >= from"}, 400

        # ── 2. Normalize CSV filter (UPPER exchange) ─
        exchange_csv = (
            request.args.get("exchange", "")
            .replace(" ", "")
            .upper()
            or None
        )

        # ── 3. Pagination (optional) ──────────────────────────
        # Same rule as /api/personas/<slug>/deadlines: omitting `limit`
        # means no pagination — everything matching the date range comes
        # back. A positive limit opts into page-by-page results.
        raw_limit  = request.args.get("limit",  "").strip()
        raw_offset = request.args.get("offset", "").strip()
        if raw_limit:
            try:
                limit = int(raw_limit)
            except ValueError:
                return {"error": "limit and offset must be integers"}, 400
            if limit < 1:
                return {"error": "limit must be an integer of at least 1"}, 400
        else:
            limit = None

        try:
            offset = int(raw_offset) if raw_offset else 0
        except ValueError:
            return {"error": "limit and offset must be integers"}, 400
        if offset < 0:
            offset = 0

        # ── 4. Query (no persona_slug → cross-persona) ──────
        repo = ActionItemRepository(db_pool=get_postgres_client().get_pool())
        try:
            rows, total = repo.get_deadlines(
                persona_slug=None,
                from_date=d_from,
                to_date=d_to,
                exchange_csv=exchange_csv,
                limit=limit,
                offset=offset,
            )
        except ValueError as e:
            return {"error": str(e)}, 400

        # ── 5. Shape response (same shape as persona deadlines) ─
        items = [
            {
                "id":                 str(row[0]),
                "date":               row[1].isoformat(),
                "title":              row[2],
                "exchange":           row[3],
                "url":                f"/circular/{row[4]}",
                "source_circular_id": str(row[4]),
                "circular_title":     row[5],
            }
            for row in rows
        ]

        return {
            "items": items,
            "total": total,
            "limit": limit,
            "offset": offset,
        }
