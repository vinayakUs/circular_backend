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


def _serialize_circular_record(record: Any) -> dict[str, Any]:
    return {
        "id": str(record.id),
        "source": record.source,
        "circular_id": record.circular_id,
        "source_item_key": record.source_item_key,
        "full_reference": record.full_reference,
        "department": record.department,
        "title": record.title,
        "issue_date": record.issue_date.isoformat(),
        "effective_date": (
            record.effective_date.isoformat() if record.effective_date is not None else None
        ),
        "url": record.url,
        "pdf_url": record.pdf_url,
        "status": record.status,
        "file_path": record.file_path,
        "content_hash": record.content_hash,
        "error_message": record.error_message,
        "detected_at": record.detected_at.isoformat(),
        "created_at": record.created_at.isoformat(),
        "updated_at": record.updated_at.isoformat(),
        "es_indexed_at": (
            record.es_indexed_at.isoformat() if record.es_indexed_at is not None else None
        ),
        "es_chunk_count": record.es_chunk_count,
        "es_index_name": record.es_index_name,
    }


def _serialize_circular_asset(asset: Any) -> dict[str, Any]:
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

    # @app.get("/api/circulars/<string:circular_id>")
    # def get_circular_details(circular_id: str):
    #     db_client = get_db_client()
    #     repository = CircularRepository(db_pool=db_client.get_pool())
    #     source = request.args.get("source", "").strip() or None
    #     record = repository.get_record_by_circular_id(circular_id, source=source)

    #     if record is None:
    #         return {
    #             "error": "Circular not found.",
    #             "circular_id": circular_id,
    #             "source": source,
    #         }, 404

    #     assets = repository.list_assets(record.id)
    #     return {
    #         "circular": _serialize_circular_record(record),
    #         "assets": [_serialize_circular_asset(asset) for asset in assets],
    #     }

    @app.get("/api/circulars/record/<uuid:record_id>")
    def get_circular_details(record_id):
        db_client = get_db_client()
        repository = CircularRepository(db_pool=db_client.get_pool())

        record = repository.get_record_by_id(record_id)
        if record is None:
            return {"error": "Circular not found.", "record_id": str(record_id)}, 404

        assets = repository.list_assets(record.id)
        return {
            "circular": _serialize_circular_record(record),
            "assets": [_serialize_circular_asset(asset) for asset in assets],
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
