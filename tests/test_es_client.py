import unittest
from datetime import datetime
from unittest.mock import patch

from ingestion.indexer.dto import IndexDocument, SearchHit
from ingestion.indexer.embedding_provider import (
    NoOpEmbeddingProvider,
    SentenceTransformerEmbeddingProvider,
    build_embedding_provider,
)
from ingestion.indexer.es_client import ElasticsearchClient
from ingestion.indexer.es_provider import get_es_client
import ingestion.indexer.es_provider as es_provider_module


class GetEsClientTestCase(unittest.TestCase):
    def tearDown(self) -> None:
        es_provider_module._shared_es_client = None

    @patch.object(es_provider_module.Config, "ELASTICSEARCH_URL", "http://es.local:9200")
    @patch.object(
        es_provider_module.Config, "ELASTICSEARCH_INDEX_NAME", "test_circulars_chunks"
    )
    @patch.object(es_provider_module.Config, "ES_REQUEST_TIMEOUT_SECONDS", 45)
    @patch.object(es_provider_module.Config, "ELASTICSEARCH_USERNAME", "elastic")
    @patch.object(es_provider_module.Config, "ELASTICSEARCH_PASSWORD", "secret")
    def test_get_es_client_builds_client_from_config(self) -> None:
        client = get_es_client()

        self.assertIsInstance(client, ElasticsearchClient)
        self.assertEqual(client.index_name, "test_circulars_chunks")
        self.assertEqual(client.request_timeout_seconds, 45)
        self.assertEqual(client._url, "http://es.local:9200")
        self.assertEqual(client._username, "elastic")
        self.assertEqual(client._password, "secret")
        self.assertIsNotNone(client.embedding_provider)

    def test_get_es_client_reuses_cached_instance(self) -> None:
        first_client = get_es_client()
        second_client = get_es_client()

        self.assertIs(first_client, second_client)


class ElasticsearchClientTestCase(unittest.TestCase):
    def test_search_returns_normalized_hits(self) -> None:
        raw_client = unittest.mock.Mock()
        raw_client.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_id": "chunk-1",
                        "_score": 2.5,
                        "_source": {
                            "chunk_id": "chunk-1",
                            "circular_db_id": "db-1",
                            "circular_id": "circular-1",
                            "asset_id": "asset-1",
                            "asset_role": "original_pdf",
                            "source": "SEBI",
                            "title": "Circular 1",
                            "department": "Legal",
                            "issue_date": "2024-01-02",
                            "effective_date": None,
                            "full_reference": "Circular 1 Ref",
                            "url": "https://example.com/circular-1",
                            "pdf_url": "https://example.com/circular-1.pdf",
                            "file_path": "/tmp/circular-1.pdf",
                            "archive_member_path": None,
                            "content_hash": "hash-1",
                            "chunk_index": 0,
                            "chunk_text": "query text",
                            "embedding": None,
                            "indexed_at": "2024-01-02T03:04:05",
                        },
                        "ignored_field": "value",
                    }
                ]
            }
        }
        client = ElasticsearchClient(
            url="http://localhost:9200",
            index_name="circulars_chunks",
            embedding_provider=NoOpEmbeddingProvider(),
            client=raw_client,
        )

        results = client.search("query text", {}, size=40, strategy="bm25")

        raw_client.search.assert_called_once_with(
            index="circulars_chunks",
            size=40,
            query={
                "bool": {
                    "should": [
                        {"match": {"circular_id": {"query": "query text", "boost": 5}}},
                        {"match": {"chunk_text_contextual": {"query": "query text", "boost": 3}}},
                        {
                            "multi_match": {
                                "query": "query text",
                                "fields": [
                                    "chunk_text^2",
                                    "title^2",
                                    "full_reference^2",
                                    "department",
                                    "source",
                                ],
                                "type": "best_fields",
                                "tie_breaker": 0.3,
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                    "filter": [],
                }
            },
        )
        self.assertEqual(
            results,
            [
                SearchHit(
                    es_id="chunk-1",
                    score=2.5,
                    document=IndexDocument(
                        chunk_id="chunk-1",
                        circular_db_id="db-1",
                        circular_id="circular-1",
                        asset_id="asset-1",
                        asset_role="original_pdf",
                        source="SEBI",
                        title="Circular 1",
                        department="Legal",
                        issue_date=datetime(2024, 1, 2).date(),
                        effective_date=None,
                        full_reference="Circular 1 Ref",
                        url="https://example.com/circular-1",
                        pdf_url="https://example.com/circular-1.pdf",
                        file_path="/tmp/circular-1.pdf",
                        archive_member_path=None,
                        content_hash="hash-1",
                        chunk_index=0,
                        chunk_text="query text",
                        chunk_text_contextual=None,
                        embedding=None,
                        indexed_at=datetime(2024, 1, 2, 3, 4, 5),
                    ),
                )
            ],
        )

    def test_search_uses_hybrid_query_when_embeddings_are_enabled(self) -> None:
        raw_client = unittest.mock.Mock()
        raw_client.search.return_value = {"hits": {"hits": []}}
        mock_provider = unittest.mock.MagicMock()
        mock_provider.is_enabled = True
        mock_provider.dimensions = 8
        mock_provider.embed_query.return_value = [0.1] * 8
        client = ElasticsearchClient(
            url="http://localhost:9200",
            index_name="circulars_chunks",
            embedding_provider=mock_provider,
            client=raw_client,
        )

        client.search("margin framework", {"source": ["SEBI"]}, size=10, strategy="hybrid")

        # Hybrid calls search twice: once for BM25, once for KNN
        self.assertEqual(raw_client.search.call_count, 2)
        calls = raw_client.search.call_args_list

        # First call: BM25 query
        _, bm25_kwargs = calls[0]
        self.assertEqual(bm25_kwargs["index"], "circulars_chunks")
        self.assertIn("query", bm25_kwargs)
        self.assertNotIn("knn", bm25_kwargs)

        # Second call: KNN query
        _, knn_kwargs = calls[1]
        self.assertEqual(knn_kwargs["index"], "circulars_chunks")
        self.assertIn("knn", knn_kwargs)
        self.assertNotIn("query", knn_kwargs)
        self.assertEqual(knn_kwargs["knn"]["field"], "embedding")
        self.assertEqual(knn_kwargs["knn"]["k"], 50)  # rrf_size from Config
        self.assertEqual(knn_kwargs["knn"]["num_candidates"], 1000)
        self.assertEqual(knn_kwargs["knn"]["filter"], [{"terms": {"source": ["SEBI"]}}])


class EmbeddingProviderFactoryTestCase(unittest.TestCase):
    def test_build_embedding_provider_returns_sentence_transformers_provider(self) -> None:
        provider = build_embedding_provider(
            "sentence-transformers",
            enabled=True,
            model_name="BAAI/bge-base-en-v1.5",
            query_instruction="Represent this sentence for searching relevant passages: ",
        )

        self.assertIsInstance(provider, SentenceTransformerEmbeddingProvider)
        self.assertEqual(provider.model_name, "BAAI/bge-base-en-v1.5")


if __name__ == "__main__":
    unittest.main()
