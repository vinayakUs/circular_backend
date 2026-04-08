from __future__ import annotations

from typing import Any

from ingestion.indexer.dto import IndexDocument


DEFAULT_INDEX_MAPPING: dict[str, Any] = {
    "mappings": {
        "properties": {
            "chunk_id": {"type": "keyword"},
            "circular_db_id": {"type": "keyword"},
            "circular_id": {"type": "keyword"},
            "source": {"type": "keyword"},
            "title": {"type": "text"},
            "department": {"type": "keyword"},
            "issue_date": {"type": "date"},
            "effective_date": {"type": "date"},
            "full_reference": {"type": "text"},
            "url": {"type": "keyword", "index": False},
            "pdf_url": {"type": "keyword", "index": False},
            "file_path": {"type": "keyword"},
            "content_hash": {"type": "keyword"},
            "chunk_index": {"type": "integer"},
            "chunk_text": {"type": "text"},
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
        client: Any | None = None,
    ) -> None:
        self.index_name = index_name
        self.request_timeout_seconds = request_timeout_seconds
        self._client = client
        self._url = url
        self._username = username
        self._password = password

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
        self.client.indices.create(index=self.index_name, **DEFAULT_INDEX_MAPPING)

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

    def search(self, query: str, size: int = 10) -> list[dict[str, Any]]:
        response = self.client.search(
            index=self.index_name,
            query={"match": {"chunk_text": {"query": query}}},
            size=size,
        )
        hits = response.get("hits", {}).get("hits", [])
        return [
            {
                "_id": hit.get("_id"),
                "_score": hit.get("_score"),
                "_source": hit.get("_source", {}),
            }
            for hit in hits
        ]

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
