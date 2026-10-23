"""Personas routes — list personas, fetch deadlines and circulars per persona.

GET /api/personas                  — list all personas with filter definitions
GET /api/personas/<slug>/deadlines — fetch action_item deadlines for a persona
GET /api/personas/<slug>/circulars — fetch the circulars matched to a persona
"""
from __future__ import annotations

from datetime import date, timedelta
import logging

from flask import request

from app.dto.circular_dto import (
    CircularListDataDTO,
    CircularListResponseDTO,
    CircularSummaryDTO,
    PaginationDTO,
    SignatoryDTO,
)
from app.services.personas import get_personas
from db.postgres_client import get_postgres_client
from ingestion.repository.action_item_repository import ActionItemRepository
from ingestion.repository.circular_repository import CircularRepository
from utils.exchange_source import ExchangeSource

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
        # (exchange is uppercase)
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
                "url":                f"/circular/{row[4]}",
                "source_circular_id": str(row[4]),
                "circular_title":     row[5],
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

    @app.get("/api/personas/<slug>/circulars")
    def get_persona_circulars(slug):
        """Fetch the circulars matched to a persona, newest first. Paginated.

        Query params (all optional):
            from:    ISO date YYYY-MM-DD (default: today - 30 days)
            to:      ISO date YYYY-MM-DD (default: today)
            source:  repeated, e.g. ?source=NSE&source=SEBI. There is no
                     ALL sentinel — omitting the param already means
                     "no source filter".
            limit:   int (default 10, clamped to 1..100)
            offset:  int (default 0, min 0)

        Returns the same shape as /api/circulars:
            {data: {circulars: [...]}, pagination: {limit, offset, total,
                                                    hasNext, hasPrev}}
        """
        # ── 1. Validate persona ─────────────────────────────
        if not any(p.slug == slug for p in get_personas()):
            return {"error": f"Unknown persona: '{slug}'"}, 404

        # ── 2. Parse date range (defaults: today-30d → today) ─
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
            d_to = date.today()

        if d_to < d_from:
            return {"error": "to must be >= from"}, 400

        # ── 3. Parse the repeated `source` params ───────────
        # Same vocabulary and same "no ALL sentinel" rule as
        # /api/circulars, so the two endpoints cannot drift apart.
        raw_sources = [
            value.strip().upper()
            for value in request.args.getlist("source")
            if value.strip()
        ]
        try:
            sources = ExchangeSource.parse_list(raw_sources)
        except ValueError as e:
            return {"error": str(e)}, 400

        # ── 4. Parse + clamp pagination ─────────────────────
        try:
            limit  = int(request.args.get("limit",  "10"))
            offset = int(request.args.get("offset", "0"))
        except ValueError:
            return {"error": "limit and offset must be integers"}, 400
        limit  = max(1, min(limit, 100))
        offset = max(0, offset)

        # ── 5. Query ───────────────────────────────────────
        repo = CircularRepository(db_pool=get_postgres_client().get_pool())
        records, total = repo.list_for_persona(
            persona_slug=slug,
            sources=sources,
            from_date=d_from,
            to_date=d_to,
            limit=limit,
            offset=offset,
        )

        # ── 6. Shape response ──────────────────────────────
        # Item mapping is deliberately identical to /api/circulars so the
        # two endpoints can never drift.
        items = [
            CircularSummaryDTO(
                id=r.id,
                source=r.source,
                circular_id=r.circular_id,
                full_reference=r.full_reference,
                department=r.department or None,
                title=r.title,
                issue_date=r.issue_date,
                applicable_to_nse=r.applicable_to_nse,
                status=r.status,
                url=r.url or None,
                signatories=[
                    SignatoryDTO(name=s["name"], designation=s["designation"])
                    for s in (r.signatory or [])
                ],
                is_active=r.is_active,
            )
            for r in records
        ]

        return CircularListResponseDTO(
            data=CircularListDataDTO(circulars=items),
            pagination=PaginationDTO(
                limit=limit,
                offset=offset,
                total=total,
                hasNext=offset + limit < total,
                hasPrev=offset > 0,
            ),
        ).model_dump()
