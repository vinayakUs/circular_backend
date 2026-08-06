"""
Circular routes: list, lookup, content (PDF), summary, signatories,
BM25 / BM25v2 / hybrid search, and RAG-augmented answers.

The hybrid search endpoint consumes the shared ``rag_generator`` passed in
from ``create_app()``; everything else is self-contained.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any
from uuid import UUID

from elastic_transport import ConnectionTimeout
from flask import request

from app.dto.circular_dto import CircularListResponseDTO, CircularSummaryDTO, SignatoryDTO
from app.dto.search_result_dto import search_hit_to_dict
from app.routes._helpers import serialize_circular_record
from db.postgres_client import get_postgres_client
from ingestion.indexer.es_provider import get_es_client
from ingestion.repository import AssetRepository, CircularRepository

logger = logging.getLogger(__name__)


def register_routes(app, *, rag_generator) -> None:
    # ── counts ──────────────────────────────────────────────────
    @app.get("/api/circulars/counts")
    def get_circular_counts():
        db_client = get_postgres_client()
        repository = CircularRepository(db_pool=db_client.get_pool())
        counts = repository.get_source_counts(("NSE", "SEBI"))
        nse_count = counts.get("NSE", 0)
        sebi_count = counts.get("SEBI", 0)
        return {
            "nse": nse_count,
            "sebi": sebi_count,
            "total": nse_count + sebi_count,
        }

    # ── single-record lookup (by UUID) ──────────────────────────
    @app.get("/api/circulars/record/<uuid:record_id>")
    def get_circular_details(record_id):
        db_client = get_postgres_client()
        repository = CircularRepository(db_pool=db_client.get_pool())

        record = repository.get_record_by_id(record_id)
        if record is None:
            return {"error": "Circular not found.", "record_id": str(record_id)}, 404
        return serialize_circular_record(record)

    # ── text lookup (by circular_id / full_reference / title) ───
    @app.get("/api/circulars/lookup")
    def lookup_circulars():
        raw_q = request.args.get("q", "").strip()
        if not raw_q:
            return {"error": "q query parameter is required."}, 400

        raw_field = request.args.get("field", "").strip()
        if not raw_field:
            return {
                "error": (
                    "field query parameter is required. "
                    f"Must be one of {list(CircularRepository.LOOKUP_FIELDS)}."
                )
            }, 400
        if raw_field not in CircularRepository.LOOKUP_FIELDS:
            return {
                "error": (
                    f"Invalid field {raw_field!r}. "
                    f"Must be one of {list(CircularRepository.LOOKUP_FIELDS)}."
                )
            }, 400

        db_client = get_postgres_client()
        repository = CircularRepository(db_pool=db_client.get_pool())
        try:
            matches = repository.lookup(raw_q, field=raw_field, limit=100)
        except ValueError as exc:
            return {"error": str(exc)}, 400

        return {
            "query": raw_q,
            "field": raw_field,
            "matches": [
                {
                    "id": m["id"],
                    "matchedField": m["matched_field"],
                    "matchedValue": m["matched_value"],
                }
                for m in matches
            ],
            "count": len(matches),
        }

    # ── PDF content stream ──────────────────────────────────────
    @app.get("/api/circulars/<uuid:record_id>/content")
    def get_circular_content(record_id):
        db_client = get_postgres_client()
        asset_repository = AssetRepository(db_pool=db_client.get_pool())

        asset = asset_repository.get_primary_asset(record_id)
        if asset is None:
            return {
                "error": "No PDF asset found for this circular.",
                "record_id": str(record_id),
            }, 404

        file_path = asset.file_path

        if file_path.startswith("s3://"):
            from storage.s3_client import get_s3_client
            s3 = get_s3_client()
            try:
                file_content = s3.download_bytes(file_path)
            except Exception:
                return {
                    "error": "Failed to download PDF from storage.",
                    "record_id": str(record_id),
                }, 500

            parts = file_path.replace("s3://", "").split("/", 1)
            key = parts[1] if len(parts) > 1 else ""
            response = app.make_response((file_content, 200))
            response.content_type = asset.mime_type or "application/pdf"
            response.headers["Content-Disposition"] = f'inline; filename="{key.split("/")[-1]}"'
            return response

        # Local file path
        from flask import send_file
        return send_file(file_path, mimetype=asset.mime_type or "application/pdf")

    # ── search endpoints ────────────────────────────────────────
    @app.post("/api/circulars/search/bm25")
    def search_circulars_bm25():
        body = request.get_json() or {}
        query = body.get("q", "").strip()
        strategy = "bm25"

        raw_source = body.get("source", "").strip().upper()
        if raw_source == "ALL":
            raw_source = ""
        from_date = body.get("from_date") or None
        to_date = body.get("to_date") or None

        if not query:
            return {"error": "Query parameter 'q' is required."}, 400

        search_metadata: dict[str, Any] = {}
        if raw_source:
            search_metadata["source"] = [raw_source]
        if from_date:
            search_metadata["from_date"] = from_date
        if to_date:
            search_metadata["to_date"] = to_date
        applicable_to_nse = body.get("applicable_to_nse")
        if applicable_to_nse is not None:
            search_metadata["applicable_to_nse"] = applicable_to_nse

        logger.info(
            "BM25 search request: query=%r, source=%s, from_date=%s, to_date=%s, applicable_to_nse=%s",
            query, raw_source or "ALL", from_date, to_date, applicable_to_nse,
        )

        try:
            import time
            search_start = time.perf_counter()
            results = get_es_client().search(query, search_metadata, strategy=strategy, size=10000)
            search_elapsed_ms = (time.perf_counter() - search_start) * 1000

            logger.info(
                "BM25 search completed: query=%r, results=%d, duration_ms=%.2f",
                query, len(results), search_elapsed_ms,
            )
        except ConnectionTimeout:
            logger.warning("Search timeout: query=%r", query)
            return {"error": "Search service is temporarily unavailable.",
                    "query": query, "results": []}, 503
        except Exception as e:
            logger.error("Search failed: query=%r, error=%s", query, e)
            return {"error": "Search service encountered an error.",
                    "query": query, "results": []}, 500

        return {
            "query": query,
            "strategy": strategy,
            "results": [search_hit_to_dict(result, query) for result in results],
        }

    @app.post("/api/circulars/search/bm25v2")
    def search_circulars_bm25v2():
        body = request.get_json() or {}
        query = body.get("q", "").strip()
        raw_source = body.get("source", "").strip().upper()
        if raw_source == "ALL":
            raw_source = ""
        sort = body.get("sort", "score").strip().lower()

        if not query:
            return {"error": "Query parameter 'q' is required."}, 400
        if sort not in ("score", "date"):
            return {"error": "sort must be 'score' or 'date'."}, 400

        logger.info(
            "BM25v2 search request: query=%r, source=%s, sort=%s",
            query, raw_source or "ALL", sort,
        )

        try:
            results = get_es_client().search_bm25_v2(query, source=raw_source or None, sort=sort)
            if results:
                logger.info(
                    "BM25v2 first result (pretty):\n%s",
                    json.dumps(results[0], indent=2, ensure_ascii=False, default=str),
                )
            else:
                logger.info("BM25v2 returned 0 hits: query=%r", query)
        except ConnectionTimeout:
            logger.warning("BM25v2 search timeout: query=%r", query)
            return {"error": "Search service is temporarily unavailable.",
                    "query": query, "results": []}, 503
        except ValueError as e:
            logger.warning("BM25v2 search invalid arg: query=%r, error=%s", query, e)
            return {"error": str(e), "query": query, "results": []}, 400
        except Exception as e:
            logger.error("BM25v2 search failed: query=%r, error=%s", query, e)
            return {"error": "Search service encountered an error.",
                    "query": query, "results": []}, 500

        return {
            "query": query,
            "strategy": "bm25v2",
            "source": raw_source or None,
            "sort": sort,
            "results": [search_hit_to_dict(result, query) for result in results],
        }

    @app.post("/api/circulars/search/hybrid")
    def search_circulars_hybrid():
        body = request.get_json() or {}
        query = body.get("q", "").strip()
        strategy = "hybrid"

        raw_source = body.get("source", "").strip().upper()
        if raw_source == "ALL":
            raw_source = ""
        from_date = body.get("from_date") or None
        to_date = body.get("to_date") or None

        if not query:
            return {"error": "Query parameter 'q' is required."}, 400

        search_metadata: dict[str, Any] = {}
        if raw_source:
            search_metadata["source"] = [raw_source]
        if from_date:
            search_metadata["from_date"] = from_date
        if to_date:
            search_metadata["to_date"] = to_date
        applicable_to_nse = body.get("applicable_to_nse")
        if applicable_to_nse is not None:
            search_metadata["applicable_to_nse"] = applicable_to_nse

        logger.info(
            "Hybrid search request: query=%r, source=%s, from_date=%s, to_date=%s, applicable_to_nse=%s",
            query, raw_source or "ALL", from_date, to_date, applicable_to_nse,
        )

        try:
            import time
            search_start = time.perf_counter()
            results = get_es_client().search(query, search_metadata, strategy=strategy)
            search_elapsed_ms = (time.perf_counter() - search_start) * 1000
            logger.info(
                "Hybrid search completed: query=%r, results=%d, duration_ms=%.2f",
                query, len(results), search_elapsed_ms,
            )
        except ConnectionTimeout:
            logger.warning("Search timeout: query=%r", query)
            return {"error": "Search service is temporarily unavailable.",
                    "query": query, "results": []}, 503
        except Exception as e:
            logger.error("Search failed: query=%r, error=%s", query, e)
            return {"error": "Search service encountered an error.",
                    "query": query, "results": []}, 500

        try:
            import time
            rag_start = time.perf_counter()
            logger.info("Executing LLM search: strategy=%s", strategy)
            rag_answer = rag_generator.generate_answer(query, results)
            rag_elapsed_ms = (time.perf_counter() - rag_start) * 1000

            logger.info(
                "RAG answer generated: query=%r, strategy=%s, answer_length=%d, refs=%d, duration_ms=%.2f",
                query, strategy,
                len(rag_answer.answer) if rag_answer.answer else 0,
                len(rag_answer.references),
                rag_elapsed_ms,
            )
            return {
                "query": query,
                "strategy": strategy,
                "answer": rag_answer.answer,
                "references": rag_answer.references,
            }
        except Exception as e:
            logger.warning("RAG failed, returning raw chunks: query=%r, error=%s", query, e)
            return {
                "query": query,
                "strategy": strategy,
                "results": [search_hit_to_dict(result, query) for result in results],
                "rag_error": str(e),
            }

    # ── paginated list ──────────────────────────────────────────
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

        raw_signatory = [s for s in request.args.getlist("signatory") if s.strip()]
        raw_from_date = request.args.get("from_date", "").strip() or None
        raw_to_date = request.args.get("to_date", "").strip() or None
        raw_applicable_to_nse = request.args.get("applicable_to_nse", "").strip() or None
        raw_department = request.args.get("department", "").strip() or None

        raw_circular_nos = [s for s in request.args.getlist("circular_no") if s.strip()]
        circular_nos: list[str] = []
        for value in raw_circular_nos:
            try:
                circular_nos.append(str(UUID(value)))
            except ValueError:
                return {"error": f"Invalid circular_no: {value!r}"}, 400

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

        applicable_to_nse = None
        if raw_applicable_to_nse is not None:
            if raw_applicable_to_nse.lower() == "true":
                applicable_to_nse = True
            elif raw_applicable_to_nse.lower() == "false":
                applicable_to_nse = False
            else:
                return {"error": "applicable_to_nse must be 'true' or 'false'."}, 400

        db_client = get_postgres_client()
        repository = CircularRepository(db_pool=db_client.get_pool())

        records, total = repository.list_paginated(
            limit=limit,
            offset=offset,
            source=normalized_source,
            from_date=from_date,
            to_date=to_date,
            applicable_to_nse=applicable_to_nse,
            signatory=raw_signatory,
            circular_nos=circular_nos,
            department=raw_department,
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
                applicable_to_nse=r.applicable_to_nse,
                status=r.status,
                url=r.url or None,
                signatories=[
                    SignatoryDTO(name=s["name"], designation=s["designation"])
                    for s in (r.signatory or [])
                ],
            )
            for r in records
        ]

        response = CircularListResponseDTO(
            data={"circulars": items},
            pagination={
                "limit": limit,
                "offset": offset,
                "total": total,
                "hasNext": offset + limit < total,
                "hasPrev": offset > 0,
            },
        )
        return response.model_dump()

    # ── distinct departments for the list-page dropdown ────────
    @app.get("/api/circulars/departments")
    def list_circular_departments():
        raw_source = request.args.get("source", "").strip().upper()
        if raw_source == "ALL" or raw_source == "":
            normalized_source: str | None = None
        elif raw_source in {"NSE", "SEBI"}:
            normalized_source = raw_source
        else:
            return {"error": "source must be 'NSE', 'SEBI', or 'ALL'."}, 400

        db_client = get_postgres_client()
        repository = CircularRepository(db_pool=db_client.get_pool())
        return {
            "source": normalized_source,
            "items": [
                {"name": d} for d in repository.list_distinct_departments(normalized_source)
            ],
        }

    # ── summary + signatories for one circular ──────────────────
    @app.get("/api/circulars/<uuid:record_id>/summary")
    def get_circular_summary(record_id):
        from ingestion.repository.summary_repository import SummaryRepository
        db_client = get_postgres_client()
        repository = SummaryRepository(db_pool=db_client.get_pool())
        summary_text = repository.get_summary_text(record_id)
        if summary_text is None:
            return {"error": "Summary not found"}, 404
        return {"summary": summary_text}

    @app.get("/api/circulars/<uuid:record_id>/signatories")
    def get_circular_signatories(record_id):
        db_client = get_postgres_client()
        from ingestion.repository.circular_signatory_repository import CircularSignatoryRepository
        repo = CircularSignatoryRepository(db_client.get_pool())
        signatories = repo.get_signatories(record_id)
        return {
            "signatories": [
                {
                    "name": s.signatory_name,
                    "designation": s.signatory_designation,
                    "extracted_at": s.extracted_at.isoformat() if s.extracted_at else None,
                }
                for s in signatories
            ]
        }

    # ── reference graph (full reachable neighborhood) ──────────
    @app.get("/api/circulars/<uuid:record_id>/reference-graph")
    def get_circular_reference_graph(record_id):
        """Return the full reachable reference graph (nodes + links) for the
        given circular. Walks both outgoing and incoming edges, no depth cap,
        cycle-safe (PostgreSQL recursive CTEs)."""
        from app.services.reference_graph_service import ReferenceGraphService
        db_client = get_postgres_client()
        service = ReferenceGraphService(db_client.get_pool())
        graph = service.get_reference_graph(record_id)
        if graph is None:
            return {"error": "Circular not found.", "record_id": str(record_id)}, 404
        return graph