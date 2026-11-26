from __future__ import annotations

import logging
import time

from elastic_transport import ConnectionTimeout
from flask import request

from app.services.search_engine.wiring import get_engine

logger = logging.getLogger(__name__)


def register_routes(app) -> None:
    @app.post("/api/search")
    def search():
        body = request.get_json() or {}
        q = body.get("q", "").strip()
        if not q:
            return {"error": "Query parameter 'q' is required.", "query": q}, 400

        summaries = bool(body.get("summaries", False))
        started = time.monotonic()
        try:
            out = get_engine(include_summaries=summaries).run(q, tags=["api"])
        except (RuntimeError, ConnectionTimeout, OSError):
            logger.exception("search engine unavailable")
            return {"error": "Search service is temporarily unavailable.", "query": q}, 503
        except Exception:
            logger.exception("search failed")
            return {"error": "Search service encountered an error.", "query": q}, 500

        logger.info("search ok route=%s steps=%s elapsed=%.1fs",
                    out.get("route"), out.get("steps"), time.monotonic() - started)
        return {"plan": out["plan"], "response": out["response"],
                "route": out.get("route"), "steps": out.get("steps", 0)}
