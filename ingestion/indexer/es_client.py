from __future__ import annotations

from typing import Any

from ingestion.indexer.dto import IndexDocument, SearchHit
from ingestion.indexer.embedding_provider import EmbeddingProvider, NoOpEmbeddingProvider


DEFAULT_INDEX_MAPPING: dict[str, Any] = {
    "mappings": {
        "properties": {
            "chunk_id": {"type": "keyword"},
            "circular_db_id": {"type": "keyword"},
            "circular_id": {"type": "keyword"},
            "asset_id": {"type": "keyword"},
            "asset_role": {"type": "keyword"},
            "source": {"type": "keyword"},
            "title": {"type": "text"},
            "department": {"type": "keyword"},
            "issue_date": {"type": "date"},
            "effective_date": {"type": "date"},
            "full_reference": {"type": "text"},
            "url": {"type": "keyword", "index": False},
            "pdf_url": {"type": "keyword", "index": False},
            "file_path": {"type": "keyword"},
            "archive_member_path": {"type": "keyword"},
            "content_hash": {"type": "keyword"},
            "chunk_index": {"type": "integer"},
            "chunk_text": {"type": "text"},
            "embedding": {
                "type": "dense_vector",
                "dims": 256,
                "index": True,
                "similarity": "cosine",
            },
            "indexed_at": {"type": "date"},
        }
    }
}


class ElasticsearchClient:
    """Thin wrapper around the official Elasticsearch Python client."""

    def __init__(
        self,
        url: str,
        index_name: str,
        request_timeout_seconds: int = 30,
        username: str | None = None,
        password: str | None = None,
        embedding_provider: EmbeddingProvider | None = None,
        client: Any | None = None,
    ) -> None:
        self.index_name = index_name
        self.request_timeout_seconds = request_timeout_seconds
        self._client = client
        self._url = url
        self._username = username
        self._password = password
        self.embedding_provider = embedding_provider or NoOpEmbeddingProvider()

    def _mapping(self) -> dict[str, Any]:
        mapping = {
            "mappings": {
                "properties": dict(DEFAULT_INDEX_MAPPING["mappings"]["properties"])
            }
        }
        if not self.embedding_provider.is_enabled:
            mapping["mappings"]["properties"].pop("embedding", None)
        else:
            mapping["mappings"]["properties"]["embedding"]["dims"] = (
                self.embedding_provider.dimensions
            )
        return mapping

    @property
    def client(self) -> Any:
        if self._client is None:
            try:
                from elasticsearch import Elasticsearch  # type: ignore[import]
            except ImportError as exc:
                raise RuntimeError(
                    "elasticsearch is not installed. Install dependencies before running the indexer."
                ) from exc
            client_kwargs: dict[str, Any] = {
                "request_timeout": self.request_timeout_seconds,
            }
            if self._username:
                client_kwargs["basic_auth"] = (self._username, self._password or "")
            self._client = Elasticsearch(self._url, **client_kwargs)
        return self._client

    def setup_index(self) -> None:
        if self.client.indices.exists(index=self.index_name):
            return
        self.client.indices.create(index=self.index_name, **self._mapping())

    def delete_index(self) -> None:
        if not self.client.indices.exists(index=self.index_name):
            return
        self.client.indices.delete(index=self.index_name)

    def bulk_index(self, documents: list[IndexDocument]) -> tuple[int, int]:
        if not documents:
            return 0, 0

        try:
            from elasticsearch.helpers import bulk  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "elasticsearch is not installed. Install dependencies before running the indexer."
            ) from exc

        actions = [
            {
                "_op_type": "index",
                "_index": self.index_name,
                "_id": document.chunk_id,
                "_source": document.to_es_body(),
            }
            for document in documents
        ]
        success_count, errors = bulk(
            self.client,
            actions,
            raise_on_error=False,
            raise_on_exception=False,
        )
        failed_count = len(errors)
        return success_count, failed_count

    def search(
        self,
        query: str,
        metadata: dict[str, Any] | None = None,
        size: int = 40,
        strategy: str = "hybrid",
    ) -> list[SearchHit]:
        metadata = metadata or {}
        strategy = strategy.lower()
        filters = self._build_filters(metadata)
        bm25_query = self._build_bm25_query(query, filters)
        query_vector = self.embedding_provider.embed_query(query)

        search_kwargs: dict[str, Any] = {
            "index": self.index_name,
            "size": size,
        }
        if strategy == "bm25":
            search_kwargs["query"] = bm25_query
        elif strategy == "vector":
            if query_vector is None:
                search_kwargs["query"] = bm25_query
            else:
                search_kwargs["knn"] = self._build_knn(query_vector, size, filters)
        else:
            search_kwargs["query"] = bm25_query
            if query_vector is not None:
                search_kwargs["knn"] = self._build_knn(query_vector, size, filters)

        response = self.client.search(**search_kwargs)
        hits = response.get("hits", {}).get("hits", [])
        return [
            SearchHit(
                es_id=hit.get("_id"),
                score=hit.get("_score"),
                document=IndexDocument.from_es_source(hit.get("_source", {})),
            )
            for hit in hits
        ]

    def _build_filters(self, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        return [{"terms": {key: value}} for key, value in metadata.items() if value]

    def _build_bm25_query(self, query: str, filters: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "bool": {
                "should": [
                    {"match": {"chunk_text": {"query": query, "boost": 3}}},
                    {"match": {"title": {"query": query, "boost": 2}}},
                    {"match": {"full_reference": {"query": query, "boost": 2}}},
                    {
                        "multi_match": {
                            "query": query,
                            "fields": ["chunk_text", "title^2", "full_reference^2", "department"],
                            "type": "best_fields",
                        }
                    },
                ],
                "minimum_should_match": 1,
                "filter": filters,
            }
        }

    def _build_knn(
        self, query_vector: list[float], size: int, filters: list[dict[str, Any]]
    ) -> dict[str, Any]:
        knn: dict[str, Any] = {
            "field": "embedding",
            "query_vector": query_vector,
            "k": size,
            "num_candidates": max(size * 3, 50),
        }
        if filters:
            knn["filter"] = filters
        return knn

    def delete_documents_for_record(self, circular_db_id: str) -> None:
        self.client.delete_by_query(
            index=self.index_name,
            query={"term": {"circular_db_id": circular_db_id}},
            conflicts="proceed",
            refresh=True,
        )

    def delete_stale_documents_for_record(
        self, circular_db_id: str, active_chunk_ids: list[str]
    ) -> None:
        query: dict[str, Any] = {"term": {"circular_db_id": circular_db_id}}
        if active_chunk_ids:
            query = {
                "bool": {
                    "must": [{"term": {"circular_db_id": circular_db_id}}],
                    "must_not": [{"terms": {"chunk_id": active_chunk_ids}}],
                }
            }
        self.client.delete_by_query(
            index=self.index_name,
            query=query,
            conflicts="proceed",
            refresh=True,
        )
