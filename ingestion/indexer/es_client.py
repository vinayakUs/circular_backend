from __future__ import annotations

import logging
from typing import Any

from config import Config
from ingestion.indexer.dto import IndexDocument, SearchHit
from ingestion.indexer.embedding_provider import EmbeddingProvider, NoOpEmbeddingProvider

try:
    from ranx import Run
    from ranx.fusion import rrf
    RANX_AVAILABLE = True
except ImportError:
    RANX_AVAILABLE = False


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
            "chunk_text_contextual": {"type": "text"},
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
        self.logger = logging.getLogger(__name__)

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
            response = self.client.search(**search_kwargs)
            hits = [
                SearchHit(
                    es_id=hit.get("_id"),
                    score=hit.get("_score"),
                    document=IndexDocument.from_es_source(hit.get("_source", {})),
                )
                for hit in response.get("hits", {}).get("hits", [])
            ]
        elif strategy == "vector":
            if query_vector is None:
                search_kwargs["query"] = bm25_query
                response = self.client.search(**search_kwargs)
                hits = [
                    SearchHit(
                        es_id=hit.get("_id"),
                        score=hit.get("_score"),
                        document=IndexDocument.from_es_source(hit.get("_source", {})),
                    )
                    for hit in response.get("hits", {}).get("hits", [])
                ]
            else:
                search_kwargs["knn"] = self._build_knn(query_vector, size, filters)
                response = self.client.search(**search_kwargs)
                hits = [
                    SearchHit(
                        es_id=hit.get("_id"),
                        score=hit.get("_score"),
                        document=IndexDocument.from_es_source(hit.get("_source", {})),
                    )
                    for hit in response.get("hits", {}).get("hits", [])
                ]
        else:
            if query_vector is not None:
                # Run BM25 and KNN separately for RRF fusion
                rrf_size = Config.ES_RRF_WINDOW_SIZE

                # BM25 query
                bm25_response = self.client.search(
                    index=self.index_name,
                    query=bm25_query,
                    size=rrf_size,
                )

                # KNN query
                knn_response = self.client.search(
                    index=self.index_name,
                    knn=self._build_knn(query_vector, rrf_size, filters),
                    size=rrf_size,
                )

                # Parse BM25 results
                bm25_hits = [
                    SearchHit(
                        es_id=hit.get("_id"),
                        score=hit.get("_score"),
                        document=IndexDocument.from_es_source(hit.get("_source", {})),
                    )
                    for hit in bm25_response.get("hits", {}).get("hits", [])
                ]

                # Parse KNN results
                knn_hits = [
                    SearchHit(
                        es_id=hit.get("_id"),
                        score=hit.get("_score"),
                        document=IndexDocument.from_es_source(hit.get("_source", {})),
                    )
                    for hit in knn_response.get("hits", {}).get("hits", [])
                ]

                # Combine with RRF
                hits = self._combine_with_rrf(bm25_hits, knn_hits, k=Config.ES_RRF_RANK_CONSTANT)
                hits = hits[:size]  # Limit to requested size
            else:
                # Fallback to BM25 only if no query vector
                search_kwargs["query"] = bm25_query
                response = self.client.search(**search_kwargs)
                hits = [
                    SearchHit(
                        es_id=hit.get("_id"),
                        score=hit.get("_score"),
                        document=IndexDocument.from_es_source(hit.get("_source", {})),
                    )
                    for hit in response.get("hits", {}).get("hits", [])
                ]

        return hits

    def _build_filters(self, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        return [{"terms": {key: value}} for key, value in metadata.items() if value]

    def _build_bm25_query(self, query: str, filters: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "bool": {
                "should": [
                    # High boost for circular_id (exact match)
                    {"match": {"circular_id": {"query": query, "boost": 5}}},
                    # High boost for contextual chunks
                    {"match": {"chunk_text_contextual": {"query": query, "boost": 3}}},
                    # multi_match for other fields
                    {
                        "multi_match": {
                            "query": query,
                            "fields": [
                                "chunk_text^2",         # raw text
                                "title^2",             # title
                                "full_reference^2",    # reference
                                "department",          # department
                                "source"               # source
                            ],
                            "type": "best_fields",
                            "tie_breaker": 0.3
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

    def _combine_with_rrf(
        self,
        bm25_hits: list[SearchHit],
        knn_hits: list[SearchHit],
        k: int = 60,
    ) -> list[SearchHit]:
        """Combine BM25 and KNN results using ranx RRF."""
        if not RANX_AVAILABLE:
            self.logger.warning("ranx not available, falling back to BM25 results")
            return bm25_hits

        # Create Run objects for BM25 and KNN results
        # Use a single query ID since we're combining results for one query
        query_id = "q_1"

        # BM25 run: {doc_id: score}
        bm25_dict = {query_id: {hit.es_id: hit.score or 0.0 for hit in bm25_hits}}
        bm25_run = Run(bm25_dict, name="bm25")

        # KNN run: {doc_id: score}
        knn_dict = {query_id: {hit.es_id: hit.score or 0.0 for hit in knn_hits}}
        knn_run = Run(knn_dict, name="knn")

        # Use ranx RRF for fusion
        fused_run = rrf([bm25_run, knn_run], k=k)

        # Get fused results for the query
        fused_results = fused_run[query_id]

        # Sort by score (descending)
        sorted_results = sorted(fused_results.items(), key=lambda x: x[1], reverse=True)

        # Create document lookup
        doc_map = {hit.es_id: hit for hit in bm25_hits + knn_hits}

        # Convert back to SearchHit format
        return [doc_map[doc_id] for doc_id, _ in sorted_results]

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
