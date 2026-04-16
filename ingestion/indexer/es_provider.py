from __future__ import annotations

from config import Config
from ingestion.indexer.embedding_provider import build_embedding_provider
from ingestion.indexer.es_client import ElasticsearchClient


_shared_es_client: ElasticsearchClient | None = None


def get_es_client() -> ElasticsearchClient:
    global _shared_es_client

    if _shared_es_client is None:
        embedding_provider = build_embedding_provider(
            Config.ES_EMBEDDING_PROVIDER,
            enabled=Config.ES_ENABLE_VECTORS,
            dimensions=Config.ES_VECTOR_DIMS,
            model_name=Config.ES_EMBEDDING_MODEL_NAME,
            query_instruction=Config.ES_QUERY_EMBEDDING_INSTRUCTION,
        )
        _shared_es_client = ElasticsearchClient(
            url=Config.ELASTICSEARCH_URL,
            index_name=Config.ELASTICSEARCH_INDEX_NAME,
            request_timeout_seconds=Config.ES_REQUEST_TIMEOUT_SECONDS,
            username=Config.ELASTICSEARCH_USERNAME,
            password=Config.ELASTICSEARCH_PASSWORD,
            embedding_provider=embedding_provider,
        )
    return _shared_es_client
