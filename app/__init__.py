from dataclasses import dataclass
from datetime import date
import logging
from typing import Any
from uuid import UUID

from flask import Flask, request
from flask_cors import CORS

from config import Config
from db import get_db_client
from ingestion.indexer.es_provider import get_es_client
from ingestion.repository import CircularRepository
from ingestion.repository.action_item_repository import ActionItemRepository
from app.dto.action_item_dto import ActionItemListResponseDTO
from app.dto.circular_dto import CircularListResponseDTO, CircularSummaryDTO
from services.rag.answer_generator import RAGAnswerGenerator

try:
    from elastic_transport import ConnectionTimeout
except ImportError:  # pragma: no cover - dependency is installed in runtime
    ConnectionTimeout = TimeoutError

'''
GET /api/circulars/search?q=what are regulations for trading members&strategy=vector
GET /api/circulars/search?q=margin&strategy=bm25
'''



logger = logging.getLogger(__name__)


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
    logging.basicConfig(
        level=Config.LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("numba.core").setLevel(logging.WARNING)

    app = Flask(__name__)
    app.config.from_object("config.Config")
    CORS(app, origins="*")

    rag_generator = RAGAnswerGenerator()

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



    @app.post("/api/circulars/search")
    def search_circulars():
        body = request.get_json() or {}
        query = body.get("q", "").strip()
        strategy = body.get("strategy", Config.ES_SEARCH_DEFAULT_STRATEGY).strip().lower()

        raw_source = body.get("source", "").strip().upper()
        if raw_source == "ALL":
            raw_source = ""
        from_date = body.get("from_date") or None
        to_date = body.get("to_date") or None

        if not query:
            return {"error": "Query parameter 'q' is required."}, 400

        if strategy not in {"bm25", "vector", "hybrid"}:
            return {"error": "Unsupported search strategy."}, 400

        search_metadata = {}
        if raw_source:
            search_metadata["source"] = [raw_source]
        if from_date:
            search_metadata["from_date"] = from_date
        if to_date:
            search_metadata["to_date"] = to_date

        logger.info(
            "Search request: query=%r, strategy=%s, source=%s, from_date=%s, to_date=%s",
            query,
            strategy,
            raw_source or "ALL",
            from_date,
            to_date,
        )

        try:
            import time
            search_start = time.perf_counter()
            results = get_es_client().search(query, search_metadata, strategy=strategy)
            search_elapsed_ms = (time.perf_counter() - search_start) * 1000

            result_count = len(results)
            logger.info(
                "Search completed: query=%r, strategy=%s, results=%d, duration_ms=%.2f",
                query,
                strategy,
                result_count,
                search_elapsed_ms,
            )
        except ConnectionTimeout:
            logger.warning("Search timeout: query=%r, strategy=%s", query, strategy)
            return {
                "error": "Search service is temporarily unavailable.",
                "query": query,
                "results": [],
            }, 503
        except Exception as e:
            logger.error("Search failed: query=%r, strategy=%s, error=%s", query, strategy, e)
            return {
                "error": "Search service encountered an error.",
                "query": query,
                "results": [],
            }, 500

        if strategy == "bm25":
            return {
                "query": query,
                "strategy": strategy,
                "results": [result.to_dict(query=query) for result in results],
            }
        else:
            try:
                rag_start = time.perf_counter()
                logger.info("Executing LLM search: strategy=%s", strategy)
                rag_answer = rag_generator.generate_answer(query, results)
                rag_elapsed_ms = (time.perf_counter() - rag_start) * 1000

                logger.info(
                    "RAG answer generated: query=%r, strategy=%s, answer_length=%d, refs=%d, duration_ms=%.2f",
                    query,
                    strategy,
                    len(rag_answer.answer) if rag_answer.answer else 0,
                    len(rag_answer.references),
                    rag_elapsed_ms,
                )
                return {
                    "query": query,
                    "strategy": strategy,
                    "answer": rag_answer.answer,
                    "references": [ref.to_dict() for ref in rag_answer.references],
                    "snippets": rag_answer.snippets,
                }
            except Exception as e:
                logger.warning("RAG failed, returning raw chunks: query=%r, error=%s", query, e)
                return {
                    "query": query,
                    "strategy": strategy,
                    "results": [result.to_dict(query=query) for result in results],
                    "rag_error": str(e),
                }

    @app.get("/api/action-items")
    def get_action_items():
        raw_circular_id = request.args.get("circular_id", "").strip() or None

        if not raw_circular_id:
            return {"error": "circular_id is required."}, 400

        try:
            circular_id = UUID(raw_circular_id)
        except ValueError:
            return {"error": "Invalid circular_id format."}, 400

        db_client = get_db_client()
        repository = ActionItemRepository(db_pool=db_client.get_pool())

        action_items, total = repository.get_action_items(circular_id=circular_id)

        response = ActionItemListResponseDTO(
            action_items=action_items,
            total=total,
            limit=total,
            offset=0,
        )
        return response.model_dump()

    @app.get("/api/circulars")
    def list_circulars():
        raw_limit = request.args.get("limit", "20").strip()
        raw_offset = request.args.get("offset", "0").strip()
        raw_source = request.args.get("source", "").strip() or None

        try:
            limit = max(1, min(int(raw_limit), 100))
        except ValueError:
            return {"error": "limit must be an integer between 1 and 100."}, 400

        try:
            offset = max(0, int(raw_offset))
        except ValueError:
            return {"error": "offset must be a non-negative integer."}, 400

        normalized_source = raw_source.upper() if raw_source else None
        if normalized_source == "ALL":
            normalized_source = None
        elif normalized_source and normalized_source not in {"NSE", "SEBI"}:
            return {"error": "source must be 'NSE', 'SEBI', or 'ALL'."}, 400

        raw_from_date = request.args.get("from_date", "").strip() or None
        raw_to_date = request.args.get("to_date", "").strip() or None

        from_date = None
        to_date = None
        if raw_from_date:
            try:
                from_date = date.fromisoformat(raw_from_date)
            except ValueError:
                return {"error": "from_date must be YYYY-MM-DD."}, 400
        if raw_to_date:
            try:
                to_date = date.fromisoformat(raw_to_date)
            except ValueError:
                return {"error": "to_date must be YYYY-MM-DD."}, 400

        db_client = get_db_client()
        repository = CircularRepository(db_pool=db_client.get_pool())

        records, total = repository.list_paginated(
            limit=limit,
            offset=offset,
            source=normalized_source,
            from_date=from_date,
            to_date=to_date,
        )

        items = [
            CircularSummaryDTO(
                id=r.id,
                source=r.source,
                circular_id=r.circular_id,
                full_reference=r.full_reference,
                department=r.department or None,
                title=r.title,
                issue_date=r.issue_date,
                effective_date=r.effective_date,
                status=r.status,
                url=r.url or None,
            )
            for r in records
        ]

        response = CircularListResponseDTO(
            items=items,
            total=total,
            limit=limit,
            offset=offset,
        )
        return response.model_dump()

    return app
