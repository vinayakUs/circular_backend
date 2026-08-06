"""
Flask app factory.

The actual route handlers live in app/routes/* — this module wires them up.
Health check stays here because it's a single line that doesn't belong to
any one domain.
"""
from __future__ import annotations

import logging

from flask import Flask
from flask_cors import CORS

from config import Config
from services.rag.answer_generator import RAGAnswerGenerator

logger = logging.getLogger(__name__)


def _warmup_ranx_rrf() -> None:
    """Pre-compile ranx/numba's RRF JIT on startup so the first hybrid search
    doesn't pay a 30-60 second JIT compilation penalty.

    Without this warmup, the first call to ``ranx.fusion.rrf`` triggers a
    numba JIT compile that hangs the request thread. With the warmup, the
    JIT happens once at app start where latency doesn't matter.
    """
    try:
        from ranx import Run
        from ranx.fusion import rrf
    except ImportError:
        logger.warning("ranx not installed — skipping RRF warmup")
        return

    # ranx.__init__ forces `config.THREADING_LAYER = "workqueue"` (line 17-18 of
    # ranx/__init__.py), which is NOT thread-safe under gunicorn — concurrent
    # hybrid searches from multiple threads would trigger "Concurrent access
    # has been detected" and kill the worker. Override it AFTER ranx loads.
    try:
        import numba
        numba.config.THREADING_LAYER = "tbb"
        logger.info("Overrode numba threading layer from workqueue → tbb")
    except Exception as exc:
        logger.warning("Could not override numba threading layer: %s", exc)

    logger.info("Warming up ranx RRF (numba JIT compile, may take ~30s)...")
    try:
        run_a = Run({"q1": {"d1": 0.9, "d2": 0.7}}, name="a")
        run_b = Run({"q1": {"d1": 0.8, "d3": 0.6}}, name="b")
        rrf([run_a, run_b])
        logger.info("ranx RRF warmup complete")
    except Exception as exc:
        logger.warning("ranx RRF warmup failed: %s", exc)

    # Log which threading layer numba actually selected. numba.config.THREADING_LAYER
    # can report 'default' even when a specific layer is in use; numba.threading_layer()
    # reports the actual runtime selection. The workqueue layer is NOT thread-safe
    # under gunicorn — concurrent hybrid searches would kill the worker.
    try:
        selected = numba.threading_layer()
        configured = numba.config.THREADING_LAYER
        logger.info(
            "numba threading layer: configured=%s, selected=%s (must be 'tbb' for "
            "safe concurrent use; 'workqueue' will crash on concurrent hybrid searches)",
            configured, selected,
        )
        if selected == "workqueue":
            logger.warning(
                "numba is using the unsafe 'workqueue' layer. Hybrid search will "
                "crash under concurrent load. Fix: ensure libtbb.so is on the linker "
                "path and tbb<2022 is installed."
            )
    except Exception as exc:
        logger.warning("Could not determine numba threading layer: %s", exc)


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

    # Warm up the ranx JIT so the first hybrid search request isn't slow.
    # _warmup_ranx_rrf()

    @app.get("/")
    def health_check():
        """Liveness probe — confirms the process is up. Always returns 200
        if the worker thread is responsive. Use /healthz for readiness."""
        return {"message": "Flask project initialized successfully."}

    @app.get("/healthz")
    def healthz():
        """Readiness probe — confirms critical dependencies are reachable.
        Returns 200 if all checks pass, 503 if any critical check fails.
        Used by the load balancer to drain unhealthy instances.

        Critical: PostgreSQL (the app is useless without DB access).
        Informational: Elasticsearch (the search endpoints fail, but
        other endpoints still work). ES status is reported but does not
        flip the overall status.

        Response includes:
          - status: "ok" | "degraded"
          - checks: per-dependency status (postgres, elasticsearch, embedding_model)
          - config: active LLM provider/model, embedding model, RAG settings
          - runtime: process info (PID, numba threading layer)
        """
        import os as _os
        from db.postgres_client import get_postgres_client
        from ingestion.indexer.es_provider import get_es_client
        from config import Config

        checks: dict[str, str] = {}
        critical_ok = True

        # Postgres: critical. SELECT 1 round-trips to the DB to confirm
        # the connection pool can hand out a working connection.
        try:
            with get_postgres_client().get_pool().acquire() as conn:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchone()
            checks["postgres"] = "ok"
        except Exception as exc:
            checks["postgres"] = f"error: {type(exc).__name__}: {exc}"
            critical_ok = False

        # Elasticsearch: informational. If ES is down, hybrid search fails
        # but the rest of the API still works. The LB keeps this instance
        # in rotation; the client gets a 5xx only for search endpoints.
        try:
            es = get_es_client()
            if es.client.ping():
                checks["elasticsearch"] = "ok"
            else:
                checks["elasticsearch"] = "ping returned False"
        except Exception as exc:
            checks["elasticsearch"] = f"error: {type(exc).__name__}: {exc}"

        # Embedding model: report whether the BGE model is loaded into
        # memory. On cold start, it's None until the first embed call.
        try:
            from ingestion.indexer.es_provider import get_es_client
            es = get_es_client()
            provider = es.embedding_provider
            if not provider.is_enabled:
                checks["embedding_model"] = "disabled (vectors off)"
            else:
                model = getattr(provider, "_model", None)
                if model is not None:
                    checks["embedding_model"] = "loaded"
                else:
                    checks["embedding_model"] = "not loaded (will load on first embed call)"
        except Exception as exc:
            checks["embedding_model"] = f"error: {type(exc).__name__}: {exc}"

        # LLM provider: just report config — we don't make a real call
        # because LLM APIs are slow and a /healthz shouldn't cost money.
        # A real LLM call would also block for 10-30s and starve the LB.
        if not getattr(Config, "LLM_PROVIDER", None):
            checks["llm"] = "no provider configured"
        else:
            checks["llm"] = f"configured (provider={Config.LLM_PROVIDER})"

        # Active config (read-only snapshot — useful for debugging "why
        # is this server returning different results than the other one")
        # Read the embedding dimensions from the live provider so the value
        # reflects what's actually loaded (or None if not yet loaded).
        embedding_dim = None
        try:
            embedding_dim = getattr(es.embedding_provider, "_dimensions", None)
        except Exception:
            pass

        config_snapshot = {
            "llm_provider": getattr(Config, "LLM_PROVIDER", None),
            "rag_model": getattr(Config, "RAG_MODEL", None),
            "embedding_model": getattr(Config, "ES_EMBEDDING_MODEL_NAME", None),
            "embedding_dimensions": embedding_dim,
            "rag_max_chunks": getattr(Config, "RAG_MAX_CHUNKS", None),
            "rag_max_tokens": getattr(Config, "RAG_MAX_TOKENS", None),
            "elasticsearch_index": getattr(Config, "ELASTICSEARCH_INDEX_NAME", None),
        }

        # Runtime info (process-level — useful for "which worker am I talking to?")
        runtime = {
            "pid": _os.getpid(),
            "numba_threading_layer": _os.environ.get("NUMBA_THREADING_LAYER", "default"),
        }
        # Report the actual selected numba layer if numba is loaded
        try:
            import numba
            runtime["numba_selected_layer"] = numba.threading_layer()
        except Exception:
            pass

        status_code = 200 if critical_ok else 503
        return {
            "status": "ok" if critical_ok else "degraded",
            "checks": checks,
            "config": config_snapshot,
            "runtime": runtime,
        }, status_code

    # Wire up domain route modules.
    from app.routes import auth, circulars, experts, mentions, properties, admin

    auth.register_routes(app)
    circulars.register_routes(app, rag_generator=rag_generator)
    experts.register_routes(app)
    mentions.register_routes(app)
    properties.register_routes(app)
    admin.register_routes(app)

    return app
