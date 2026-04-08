from __future__ import annotations

from config import Config
from ingestion.indexer.es_client import ElasticsearchClient


_shared_es_client: ElasticsearchClient | None = None


def get_es_client() -> ElasticsearchClient:
    global _shared_es_client

    if _shared_es_client is None:
        _shared_es_client = ElasticsearchClient(
            url=Config.ELASTICSEARCH_URL,
            index_name=Config.ELASTICSEARCH_INDEX_NAME,
            request_timeout_seconds=Config.ES_REQUEST_TIMEOUT_SECONDS,
            username=Config.ELASTICSEARCH_USERNAME,
            password=Config.ELASTICSEARCH_PASSWORD,
        )
    return _shared_es_client
