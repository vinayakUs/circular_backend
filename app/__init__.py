from dataclasses import dataclass
from datetime import date
import logging
from typing import Any
import json
from uuid import UUID

from flask import Flask, request, g
from flask_cors import CORS

from config import Config
from db.postgres_client import get_postgres_client
from ingestion.indexer.es_provider import get_es_client
from ingestion.repository import AssetRepository, CircularRepository
from ingestion.repository.properties_repository import PropertiesRepository
from ingestion.repository.users_repository import UsersRepository
from app.dto.circular_dto import CircularListResponseDTO, CircularSummaryDTO, SignatoryDTO
from app.dto.search_result_dto import search_hit_to_dict
from app.auth.ldap_auth import LDAPAuth, require_auth
from app.services.expert_service import ExpertService
from app.services.comments_service import CommentsService
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
            if not auth.authenticate(username, password):
                return {"error": "Invalid credentials"}, 401
        except Exception as e:
            logging.getLogger(__name__).error("LDAP auth error: %s", e)
            error_msg = str(e).lower()
            if "invalid" in error_msg or "credentials" in error_msg or "_bind" in error_msg:
                return {"error": "Invalid credentials"}, 401
            return {"error": "Authentication failed"}, 500

        # Get user's database ID to include in JWT.
        # Narrow except clause: only DB connectivity / pool errors are recoverable
        # here. Programming bugs (AttributeError, KeyError, etc.) propagate so
        # they're visible in logs instead of being silently swallowed into a
        # half-broken token.
        user_db_id = None
        try:
            import psycopg2
            from ingestion.repository.users_repository import UsersRepository
            db_client = get_postgres_client()
            users_repo = UsersRepository(db_pool=db_client.get_pool())
            user_records = users_repo.get_departments_by_user(username)
            if user_records:
                user_db_id = str(user_records[0].id)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            logging.getLogger(__name__).error("DB unavailable during login for %s: %s", username, e)
            return {"error": "Service temporarily unavailable"}, 503

        # Explicit empty-result handling: if the LDAP-authenticated user has no
        # row in the users table, refuse to issue a token. require_auth blocks
        # every protected route on g.user_db_id, so a None-id token would let
        # the user click around and then 403 on the first real request.
        if not user_db_id:
            logging.getLogger(__name__).warning("LDAP user %s has no row in users table; refusing login", username)
            return {"error": "User not provisioned in DB"}, 403

        token = auth.create_token(username, user_db_id)
        return {"access_token": token, "token_type": "bearer"}

    @app.get("/api/auth/me")
    @require_auth
    def me():
        db_client = get_postgres_client()
        users_repo = UsersRepository(db_pool=db_client.get_pool())
        properties_repo = PropertiesRepository(db_pool=db_client.get_pool())

        user = users_repo.get_user(g.current_user)
        if not user:
            return {"username": g.current_user, "department": None}

        dept = properties_repo.get_by_id(user.department_id)
        return {
            "username": g.current_user,
            "user_db_id": str(user.id),
            "department_id": str(user.department_id),
            "department": dept.name if dept else None
        }

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

    # @app.get("/api/circulars/<string:circular_id>")
    # def get_circular_details(circular_id: str):
    #     db_client = get_postgres_client()
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
        db_client = get_postgres_client()
        repository = CircularRepository(db_pool=db_client.get_pool())

        record = repository.get_record_by_id(record_id)
        if record is None:
            return {"error": "Circular not found.", "record_id": str(record_id)}, 404

        return _serialize_circular_record(record)


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

    @app.get("/api/circulars/<uuid:record_id>/content")
    def get_circular_content(record_id):
        db_client = get_postgres_client()
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
            results = get_es_client().search(query, search_metadata, strategy=strategy,size=10000)
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
            query,
            raw_source or "ALL",
            sort,
        )

        try:
            results = get_es_client().search_bm25_v2(
                query,
                source=raw_source or None,
                sort=sort,
            )
            logger.info(
                "BM25v2 first result (pretty):\n%s",
                json.dumps(
                    results[0],
                    indent=2,
                    ensure_ascii=False,
                    default=str,
                ),
            )
            
        except ConnectionTimeout:
            logger.warning("BM25v2 search timeout: query=%r", query)
            return {
                "error": "Search service is temporarily unavailable.",
                "query": query,
                "results": [],
            }, 503
        except ValueError as e:
            logger.warning("BM25v2 search invalid arg: query=%r, error=%s", query, e)
            return {"error": str(e), "query": query, "results": []}, 400
        except Exception as e:
            logger.error("BM25v2 search failed: query=%r, error=%s", query, e)
            return {
                "error": "Search service encountered an error.",
                "query": query,
                "results": [],
            }, 500

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

        # raw_signatory = request.args.get("signatory", "").strip() or None
        raw_signatory = [s for s in request.args.getlist("signatory") if s.strip()]
        raw_from_date = request.args.get("from_date", "").strip() or None
        raw_to_date = request.args.get("to_date", "").strip() or None
        raw_applicable_to_nse = request.args.get("applicable_to_nse", "").strip() or None

        raw_circular_nos = [s for s in request.args.getlist("circular_no") if s.strip()]
        circular_nos: list[UUID] = []
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
            circular_nos=circular_nos
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
    @require_auth
    def save_circular_experts(record_id):
        body = request.get_json() or {}
        experts = body.get("experts", [])
        original_ids = body.get("original_ids", [])

        if not experts:
            return {"error": "experts list is required"}, 400

        db_client = get_postgres_client()
        users_repo = UsersRepository(db_pool=db_client.get_pool())
        user = users_repo.get_user(g.current_user)
        user_dep_id = user.department_id if user else None
        try:
            user_db_id = UUID(g.user_db_id) if g.user_db_id else None
        except (ValueError, TypeError):
            user_db_id = None

        service = ExpertService(db_pool=db_client.get_pool())
        result = service.save_experts(
            record_id, experts, original_ids, user_db_id, user_dep_id
        )
        return result

    @app.get("/api/circulars/<uuid:record_id>/experts")
    @require_auth
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
        db_client = get_postgres_client()
        service = ExpertService(db_pool=db_client.get_pool())
        experts = service.get_experts_for_circular(record_id)
        return {"experts": experts}

    @app.get("/api/circulars/<uuid:record_id>/experts/<uuid:expert_id>/comments")
    @require_auth
    def get_expert_comments(record_id, expert_id):
        service = CommentsService()
        comments = service.get_comments(expert_id)
        return {"comments": comments}

    @app.post("/api/circulars/<uuid:record_id>/experts/<uuid:expert_id>/comments")
    @require_auth
    def create_expert_comment(record_id, expert_id):
        body = request.get_json() or {}
        text = body.get("text", "").strip()
        if not text:
            return {"error": "text is required"}, 400

        service = CommentsService()
        comment = service.create_comment(expert_id, g.user_db_id, g.current_user, text)
        return {"comment": comment}, 201

    @app.patch("/api/circulars/<uuid:record_id>/experts/<uuid:expert_id>/status")
    @require_auth
    def update_expert_status(record_id, expert_id):
        body = request.get_json() or {}
        status = body.get("status", "").strip()
        if status not in ("open", "closed"):
            return {"error": "status must be 'open' or 'closed'"}, 400

        db_client = get_postgres_client()
        service = ExpertService(db_pool=db_client.get_pool())
        success = service.update_expert_status(expert_id, status)
        if not success:
            return {"error": "Expert not found"}, 404
        return {"success": True}

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

        db_client = get_postgres_client()
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
        db_client = get_postgres_client()
        from ingestion.repository.circular_signatory_repository import CircularSignatoryRepository
        repo = CircularSignatoryRepository(db_pool=db_client.get_pool())
        names = repo.list_distinct_names()
        return {"items": [{"name": n} for n in names]}
    

    @app.get("/api/departments")
    def list_departments():
        db_client = get_postgres_client()
        

    @app.get("/api/properties/<string:prop_type>")
    @require_auth
    def list_properties(prop_type: str):
        include_archived = request.args.get("include_archived", "false").strip().lower() == "true"
        db_client = get_postgres_client()
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

        db_client = get_postgres_client()
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

    # === Admin: User-Department Management ===

    @app.get("/api/admin/departments/<uuid:dept_id>/users")
    @require_auth
    def list_department_users(dept_id):
        """List all users in a department."""
        db_client = get_postgres_client()
        repository = UsersRepository(db_pool=db_client.get_pool())
        users = repository.get_users_by_department(dept_id)
        return {
            "users": [
                {
                    "id": str(u.id),
                    "user_id": u.user_id,
                    "department_id": str(u.department_id),
                    "created_at": u.created_at.isoformat(),
                    "created_by": u.created_by,
                    "updated_at": u.updated_at.isoformat() if u.updated_at else None,
                    "updated_by": u.updated_by,
                }
                for u in users
            ]
        }

    @app.post("/api/admin/departments/<uuid:dept_id>/users")
    @require_auth
    def add_user_to_department(dept_id):
        """Add a user to a department."""
        body = request.get_json() or {}
        user_id = body.get("user_id", "").strip()
        created_by = g.current_user  # Use authenticated user automatically

        if not user_id:
            return {"error": "user_id is required"}, 400

        db_client = get_postgres_client()
        repository = UsersRepository(db_pool=db_client.get_pool())

        # Verify department exists
        props_repo = PropertiesRepository(db_pool=db_client.get_pool())
        dept = props_repo.get_by_id(dept_id)
        if dept is None:
            return {"error": "Department not found"}, 404

        record = repository.add_user(user_id, dept_id, created_by)
        if record is None:
            return {"error": "User already exists in this or another department"}, 409

        return {
            "id": str(record.id),
            "user_id": record.user_id,
            "department_id": str(record.department_id),
            "created_at": record.created_at.isoformat(),
            "created_by": record.created_by,
        }, 201

    @app.delete("/api/admin/departments/<uuid:dept_id>/users/<user_id>")
    @require_auth
    def remove_user_from_department(dept_id, user_id):
        """Remove a user from a department."""
        db_client = get_postgres_client()
        repository = UsersRepository(db_pool=db_client.get_pool())
        deleted = repository.remove_user(user_id)
        if not deleted:
            return {"error": "User not found"}, 404
        return {"success": True}

    @app.get("/api/admin/users/<user_id>")
    @require_auth
    def get_user_details(user_id):
        """Get user details by user_id."""
        db_client = get_postgres_client()
        repository = UsersRepository(db_pool=db_client.get_pool())
        user = repository.get_user(user_id)
        if user is None:
            return {"error": "User not found"}, 404
        return {
            "id": str(user.id),
            "user_id": user.user_id,
            "department_id": str(user.department_id),
            "created_at": user.created_at.isoformat(),
            "created_by": user.created_by,
            "updated_at": user.updated_at.isoformat() if user.updated_at else None,
            "updated_by": user.updated_by,
        }

    return app
