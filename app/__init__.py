from dataclasses import dataclass
from datetime import date
import logging
from typing import Any
import json
from uuid import UUID

from flask import Flask, request, g
from flask_cors import CORS

from config import Config
from db import get_db_client
from ingestion.indexer.es_provider import get_es_client
from ingestion.repository import AssetRepository, CircularRepository
from ingestion.repository.properties_repository import PropertiesRepository
from app.dto.circular_dto import CircularListResponseDTO, CircularSummaryDTO, SignatoryDTO
from app.dto.search_result_dto import search_hit_to_dict
from app.auth.ldap_auth import LDAPAuth, require_auth
from app.services.expert_service import ExpertService
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

    @app.post("/api/auth/login")
    def login():
        body = request.get_json() or {}
        username = body.get("username", "").strip()
        password = body.get("password", "")

        if not username or not password:
            return {"error": "Username and password are required."}, 400

        auth = LDAPAuth()
        try:
            auth.authenticate(username, password)
        except Exception as e:
            logging.getLogger(__name__).error("LDAP auth error: %s", e)
            error_msg = str(e).lower()
            if "invalid" in error_msg or "credentials" in error_msg or "_bind" in error_msg:
                return {"error": "Invalid credentials"}, 401
            return {"error": "Authentication failed"}, 500

        token = auth.create_token(username)
        return {"access_token": token, "token_type": "bearer"}

    @app.get("/api/auth/me")
    @require_auth
    def me():
        return {"username": g.current_user}

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

        return _serialize_circular_record(record)


    @app.get("/api/circulars/<uuid:record_id>/content")
    def get_circular_content(record_id):
        db_client = get_db_client()
        asset_repository = AssetRepository(db_pool=db_client.get_pool())

        asset = asset_repository.get_primary_asset(record_id)
        if asset is None:
            return {"error": "No PDF asset found for this circular.", "record_id": str(record_id)}, 404

        file_path = asset.file_path

        if file_path.startswith("s3://"):
            from storage.s3_client import S3StorageClient
            s3 = S3StorageClient()
            parts = file_path.replace("s3://", "").split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ""
            try:
                file_content = s3.download_bytes(file_path)
            except Exception:
                return {"error": "Failed to download PDF from storage.", "record_id": str(record_id)}, 500

            response = app.make_response((file_content, 200))
            response.content_type = asset.mime_type or "application/pdf"
            response.headers["Content-Disposition"] = f'inline; filename="{key.split("/")[-1]}"'
            return response

        # Local file path
        from flask import send_file
        return send_file(file_path, mimetype=asset.mime_type or "application/pdf")


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

        search_metadata = {}
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
            query,
            raw_source or "ALL",
            from_date,
            to_date,
            applicable_to_nse,
        )

        try:
            import time
            search_start = time.perf_counter()
            results = get_es_client().search(query, search_metadata, strategy=strategy)
            search_elapsed_ms = (time.perf_counter() - search_start) * 1000

            result_count = len(results)
            logger.info(
                "BM25 search completed: query=%r, results=%d, duration_ms=%.2f",
                query,
                result_count,
                search_elapsed_ms,
            )
        except ConnectionTimeout:
            logger.warning("Search timeout: query=%r", query)
            return {
                "error": "Search service is temporarily unavailable.",
                "query": query,
                "results": [],
            }, 503
        except Exception as e:
            logger.error("Search failed: query=%r, error=%s", query, e)
            return {
                "error": "Search service encountered an error.",
                "query": query,
                "results": [],
            }, 500

        return {
            "query": query,
            "strategy": strategy,
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

        search_metadata = {}
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
            query,
            raw_source or "ALL",
            from_date,
            to_date,
            applicable_to_nse,
        )

        try:
            import time
            search_start = time.perf_counter()
            results = get_es_client().search(query, search_metadata, strategy=strategy)
            search_elapsed_ms = (time.perf_counter() - search_start) * 1000

            result_count = len(results)
            logger.info(
                "Hybrid search completed: query=%r, results=%d, duration_ms=%.2f",
                query,
                result_count,
                search_elapsed_ms,
            )
        except ConnectionTimeout:
            logger.warning("Search timeout: query=%r", query)
            return {
                "error": "Search service is temporarily unavailable.",
                "query": query,
                "results": [],
            }, 503
        except Exception as e:
            logger.error("Search failed: query=%r, error=%s", query, e)
            return {
                "error": "Search service encountered an error.",
                "query": query,
                "results": [],
            }, 500

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
                "results": [search_hit_to_dict(result, query) for result in results],
                "rag_error": str(e),
            }

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

        raw_signatory = request.args.get("signatory", "").strip() or None
        raw_signatory = request.args.get("signatory", "").strip() or None
        raw_from_date = request.args.get("from_date", "").strip() or None
        raw_to_date = request.args.get("to_date", "").strip() or None
        raw_applicable_to_nse = request.args.get("applicable_to_nse", "").strip() or None

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

        db_client = get_db_client()
        repository = CircularRepository(db_pool=db_client.get_pool())

        records, total = repository.list_paginated(
            limit=limit,
            offset=offset,
            source=normalized_source,
            from_date=from_date,
            to_date=to_date,
            applicable_to_nse=applicable_to_nse,
            signatory=raw_signatory,
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
                    SignatoryDTO(name=s['name'], designation=s['designation'])
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

    @app.post("/api/circulars/<uuid:record_id>/experts")
    # @require_auth  # TEMP DISABLED FOR TESTING
    def save_circular_experts(record_id):
        body = request.get_json() or {}
        experts = body.get("experts", [])
        original_ids = body.get("original_ids", [])

        if not experts:
            return {"error": "experts list is required"}, 400

        db_client = get_db_client()
        service = ExpertService(db_pool=db_client.get_pool())
        result = service.save_experts(record_id, experts, original_ids)
        return result

    @app.get("/api/circulars/<uuid:record_id>/experts")
    def get_circular_experts(record_id):
        # TEST DATA - for testing highlight restoration
        # test_experts = [
        #     {
        #         "id": "c6948101-f4e9-447d-98fa-2307541c3eb4",
        #         "dept_id": "5240f8ed-b7f4-12a1-e063-050012acfc4e",
        #         "dept_name": "Compliance",
        #         "title": "Expert 1",
        #         "text": "ities of trading members",
        #         "highlights": json.dumps([
        #             {
        #                 "annotationType": 9,
        #                 "pageIndex": 0,
        #                 "rect": [251.65439200401306, 425.93759045004845, 268.3619921207428, 440.74799036979675],
        #                 "rotation": 0,
        #                 "structTreeParentId": "p4R_mc12",
        #                 "popupRef": "",
        #                 "color": [255, 255, 152],
        #                 "opacity": 1,
        #                 "thickness": 12,
        #                 "quadPoints": [252.27081298828125, 439.9396667480469, 267.711669921875, 439.9396667480469, 252.27081298828125, 426.7596435546875, 267.711669921875, 426.7596435546875],
        #                 "outlines": [[251.65439200401306, 425.93759045004845, 251.65439200401306, 440.74799036979675, 268.3619921207428, 440.74799036979675, 268.3619921207428, 425.93759045004845]],
        #                 "id": "pdfjs_internal_editor_2",
        #                 "isCopy": True
        #             }
        #         ])
        #     }
        # ]
        # return {"experts": test_experts}

        # REAL IMPLEMENTATION - uncomment for production
        db_client = get_db_client()
        service = ExpertService(db_pool=db_client.get_pool())
        experts = service.get_experts_for_circular(record_id)
        return {"experts": experts}

    @app.get("/api/circulars/<uuid:record_id>/signatories")
    def get_circular_signatories(record_id):
        db_client = get_db_client()
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

    @app.post("/api/properties")
    def create_property():
        body = request.get_json() or {}
        name = body.get("name", "").strip()
        prop_type = body.get("type", "").strip()
        metadata = body.get("metadata") or {}

        if not name:
            return {"error": "name is required"}, 400
        if not prop_type:
            return {"error": "type is required"}, 400

        db_client = get_db_client()
        repository = PropertiesRepository(db_pool=db_client.get_pool())
        record = repository.create(name, prop_type, metadata)
        if record is None:
            return {"error": "A property with this name and type already exists"}, 409
        return {
            "id": str(record.id),
            "name": record.name,
            "type": record.type,
            "metadata": record.metadata,
            "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat(),
        }, 201

    @app.get("/api/signatories")
    def list_signatories():
        db_client = get_db_client()
        from ingestion.repository.circular_signatory_repository import CircularSignatoryRepository
        repo = CircularSignatoryRepository(db_pool=db_client.get_pool())
        names = repo.list_distinct_names()
        return {"items": [{"name": n} for n in names]}

    @app.get("/api/properties/<string:prop_type>")
    def list_properties(prop_type: str):
        include_archived = request.args.get("include_archived", "false").strip().lower() == "true"
        db_client = get_db_client()
        repository = PropertiesRepository(db_pool=db_client.get_pool())
        records = repository.list_by_type(prop_type, include_archived=include_archived)
        return {
            "type": prop_type,
            "items": [
                {
                    "id": str(r.id),
                    "name": r.name,
                    "archived": r.archived,
                    "metadata": r.metadata,
                    "created_at": r.created_at.isoformat(),
                    "updated_at": r.updated_at.isoformat(),
                }
                for r in records
            ],
        }

    @app.get("/api/experts/by-department")
    def get_experts_by_department():
        raw_dept_id = request.args.get("department_id", "").strip() or None
        raw_source = request.args.get("source", "").strip().upper() or None
        raw_from_date = request.args.get("from_date", "").strip() or None
        raw_to_date = request.args.get("to_date", "").strip() or None
        raw_circular_no = request.args.get("full_circular_no", "").strip() or None

        page = request.args.get("page", "1").strip()
        page_size = request.args.get("page_size", "20").strip()

        try:
            page = max(1, int(page))
            page_size = min(100, max(1, int(page_size)))
        except ValueError:
            page = 1
            page_size = 20

        dept_uuid = None
        if raw_dept_id:
            try:
                dept_uuid = UUID(raw_dept_id)
            except ValueError:
                return {"error": "Invalid department_id format"}, 400

        from_date = date.fromisoformat(raw_from_date) if raw_from_date else None
        to_date = date.fromisoformat(raw_to_date) if raw_to_date else None

        db_client = get_db_client()
        service = ExpertService(db_pool=db_client.get_pool())
        experts_result = service.get_experts_by_department(
            department_id=dept_uuid,
            source=raw_source,
            from_date=from_date,
            to_date=to_date,
            full_circular_no=raw_circular_no,
            page=page,
            page_size=page_size,
        )

        return {
            "experts": experts_result,
            "filters_applied": {
                "department_id": raw_dept_id,
                "source": raw_source,
                "from_date": raw_from_date,
                "to_date": raw_to_date,
                "full_circular_no": raw_circular_no,
            },
        }

    return app
