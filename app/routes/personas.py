"""Personas routes — list personas + fetch deadlines per persona.

GET /api/personas                  — list all personas with filter definitions
GET /api/personas/<slug>/deadlines — fetch action_item deadlines for a persona
"""
from __future__ import annotations

from datetime import date, timedelta
import logging

from flask import request

from app.services.personas import get_personas
from db.postgres_client import get_postgres_client
from ingestion.repository.action_item_repository import ActionItemRepository

logger = logging.getLogger(__name__)


def register_routes(app) -> None:
    @app.get("/api/personas")
    def list_personas():
        """Return all configured personas with their filter definitions."""
        return [p.model_dump() for p in get_personas()]


    @app.get("/api/personas/<slug>/deadlines")
    def get_persona_deadlines(slug):
        """Fetch action_item deadlines for a persona. Paginated.

        Query params (all optional):
            from:      ISO date YYYY-MM-DD (default: today)
            to:        ISO date YYYY-MM-DD (default: from + 60 days)
            priority:  CSV (critical,high,medium,low)
            exchange:  CSV (NSE,SEBI,...)
            limit:     int (default 100, min 1)
            offset:    int (default 0, min 0)

        Returns: {persona_slug, items: [...], total, limit, offset}
        """
        # ── 1. Validate persona ─────────────────────────────
        persona = next((p for p in get_personas() if p.slug == slug), None)
        if not persona:
            return {"error": f"Unknown persona: '{slug}'"}, 404

        # ── 2. Parse date range ────────────────────────────
        raw_from = request.args.get("from", "").strip()
        if raw_from:
            try:
                d_from = date.fromisoformat(raw_from)
            except ValueError:
                return {"error": f"from must be YYYY-MM-DD, got {raw_from!r}"}, 400
        else:
            d_from = date.today() - timedelta(days=30)

        raw_to = request.args.get("to", "").strip()
        if raw_to:
            try:
                d_to = date.fromisoformat(raw_to)
            except ValueError:
                return {"error": f"to must be YYYY-MM-DD, got {raw_to!r}"}, 400
        else:
            d_to = date.today() + timedelta(days=335)

        if d_to < d_from:
            return {"error": "to must be >= from"}, 400

        # ── 3. Parse filters + pagination ──────────────────
        # Normalize CSVs: strip whitespace, normalize case
        # (priority is lowercase, exchange is uppercase)
        priority_csv = (
            request.args.get("priority", "")
            .replace(" ", "")
            .lower()
            or None
        )
        exchange_csv = (
            request.args.get("exchange", "")
            .replace(" ", "")
            .upper()
            or None
        )

        try:
            limit  = int(request.args.get("limit",  "100"))
            offset = int(request.args.get("offset", "0"))
        except ValueError:
            return {"error": "limit and offset must be integers"}, 400
        if limit  < 1: limit  = 100
        if offset < 0: offset = 0
        # No max — pagination clients control the size

        # ── 4. Query ───────────────────────────────────────
        repo = ActionItemRepository(db_pool=get_postgres_client().get_pool())
        try:
            rows, total = repo.get_deadlines(
                persona_slug=slug,
                from_date=d_from,
                to_date=d_to,
                priority_csv=priority_csv,
                exchange_csv=exchange_csv,
                limit=limit,
                offset=offset,
            )
        except ValueError as e:
            return {"error": str(e)}, 400

        # ── 5. Shape response ──────────────────────────────
        items = [
            {
                "id":                 str(row[0]),
                "date":               row[1].isoformat(),
                "title":              row[2],
                "exchange":           row[3],
                "priority":           row[4],
                "url":                f"/circular/{row[5]}",
                "source_circular_id": str(row[5]),
            }
            for row in rows
        ]

        return {
            "persona_slug": slug,
            "items":        items,
            "total":        total,
            "limit":        limit,
            "offset":       offset,
        }