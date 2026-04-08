import unittest
from unittest.mock import patch

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
                        "_source": {"title": "Circular 1", "chunk_text": "query text"},
                        "ignored_field": "value",
                    }
                ]
            }
        }
        client = ElasticsearchClient(
            url="http://localhost:9200",
            index_name="circulars_chunks",
            client=raw_client,
        )

        results = client.search("query text")

        raw_client.search.assert_called_once_with(
            index="circulars_chunks",
            query={"match": {"chunk_text": {"query": "query text"}}},
            size=10,
        )
        self.assertEqual(
            results,
            [
                {
                    "_id": "chunk-1",
                    "_score": 2.5,
                    "_source": {
                        "title": "Circular 1",
                        "chunk_text": "query text",
                    },
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
