"""Engine wiring: the search engine's only contact point with Postgres / Elasticsearch / the LLM."""
from __future__ import annotations

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.services.search_engine.graph import SearchEngine


def build_engine(include_summaries: bool = False) -> SearchEngine:
    """Wire the engine to the real Postgres / Elasticsearch / (optional) S3 summaries."""
    from db.postgres_client import get_postgres_client
    from ingestion.indexer.es_provider import get_es_client
    from ingestion.repository.circular_repository import CircularRepository
    from app.services.search_engine.graph import SearchEngine
    from app.services.search_engine.llm import build_models
    from app.services.search_engine.retrieval import Retrieval

    pool = get_postgres_client().get_pool()
    summary_repo = None
    if include_summaries:
        from ingestion.repository.summary_repository import SummaryRepository
        summary_repo = SummaryRepository(db_pool=pool)
    rt = Retrieval(repo=CircularRepository(db_pool=pool), es=get_es_client(), pool=pool, summary_repo=summary_repo)
    return SearchEngine(rt, build_models(), include_summaries=include_summaries)


_engines: dict[bool, SearchEngine] = {}
_lock = threading.Lock()


def get_engine(include_summaries: bool = False) -> SearchEngine:
    with _lock:
        engine = _engines.get(include_summaries)
        if engine is None:
            engine = build_engine(include_summaries=include_summaries)
            _engines[include_summaries] = engine
        return engine
