from dataclasses import dataclass
from datetime import date
from typing import Any
from uuid import UUID

from flask import Flask, request

from db import get_db_client
from ingestion.indexer.es_provider import get_es_client
from ingestion.repository import CircularRepository

try:
    from elastic_transport import ConnectionTimeout
except ImportError:  # pragma: no cover - dependency is installed in runtime
    ConnectionTimeout = TimeoutError


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.from_object("config.Config")

    @app.get("/")
    def health_check():
        return {"message": "Flask project initialized successfully."}

    @app.get("/api/circulars/counts")
    def get_circular_counts():
        db_client = get_db_client()
        repository = CircularRepository(db_pool=db_client.get_pool())
        counts = repository.get_source_counts(("NSE", "SEBI"))
        nse_count = counts.get("NSE", 0)
        sebi_count = counts.get("SEBI", 0)
        return {
            "nse": nse_count,
            "sebi": sebi_count,
            "total": nse_count + sebi_count,
        }

    @app.get("/api/circulars/search")
    def search_circulars():

        query = request.args.get("q", "").strip()
        raw_exchange = request.args.get("exchange", "")
        raw_exchange = [ex.strip().upper() for ex in raw_exchange.split(",") if ex]

        if not query:
            return {"error": "Query parameter 'q' is required."}, 400

        try:
            exchange = {"source":raw_exchange}
            results = get_es_client().search(query,exchange)
        except ConnectionTimeout:
            return {
                "error": "Search service is temporarily unavailable.",
                "query": query,
                "results": [],
            }, 503

        return {"query": query, "results": [result.to_dict(query=query) for result in results]}

    return app
