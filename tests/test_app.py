import unittest
from datetime import date, datetime
from unittest.mock import patch

from elastic_transport import ConnectionTimeout

from app import create_app
from ingestion.indexer.dto import IndexDocument, SearchHit


class AppTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.app = create_app()
        self.client = self.app.test_client()

    def test_health_check(self) -> None:
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {"message": "Flask project initialized successfully."},
        )

    @patch("app.CircularRepository")
    @patch("app.get_db_client")
    def test_circular_counts_endpoint_returns_nse_and_sebi_totals(
        self, get_db_client, repository_cls
    ) -> None:
        db_client = get_db_client.return_value
        db_client.get_pool.return_value = object()
        repository = repository_cls.return_value
        repository.get_source_counts.return_value = {"NSE": 11, "SEBI": 7}

        response = self.client.get("/api/circulars/counts")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {"nse": 11, "sebi": 7, "total": 18},
        )
        repository.get_source_counts.assert_called_once_with(("NSE", "SEBI"))

    @patch("app.get_es_client")
    def test_search_endpoint_returns_es_results(self, get_es_client) -> None:
        es_client = get_es_client.return_value
        es_client.search.return_value = [
            SearchHit(
                es_id="chunk-1",
                score=1.23,
                document=IndexDocument(
                    chunk_id="chunk-1",
                    circular_db_id="db-1",
                    circular_id="circular-1",
                    source="SEBI",
                    title="SEBI circular",
                    department="Markets",
                    issue_date=date(2024, 1, 2),
                    effective_date=None,
                    full_reference="SEBI/HO/MRD/2024/1",
                    url="https://example.com/circular-1",
                    pdf_url="https://example.com/circular-1.pdf",
                    file_path="/tmp/circular-1.pdf",
                    content_hash="hash-1",
                    chunk_index=0,
                    chunk_text="margin framework update",
                    indexed_at=datetime(2024, 1, 2, 3, 4, 5),
                ),
            )
        ]

        response = self.client.get("/api/circulars/search?q=margin")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {
                "query": "margin",
                "results": [
                    {
                        "id": "chunk-1",
                        "score": 1.23,
                        "document": {
                            "chunk_id": "chunk-1",
                            "circular_db_id": "db-1",
                            "circular_id": "circular-1",
                            "source": "SEBI",
                            "title": "SEBI circular",
                            "department": "Markets",
                            "issue_date": "2024-01-02",
                            "effective_date": None,
                            "full_reference": "SEBI/HO/MRD/2024/1",
                            "url": "https://example.com/circular-1",
                            "pdf_url": "https://example.com/circular-1.pdf",
                            "file_path": "/tmp/circular-1.pdf",
                            "content_hash": "hash-1",
                            "chunk_index": 0,
                            "chunk_text": "margin framework update",
                            "indexed_at": "2024-01-02T03:04:05",
                        },
                    }
                ],
            },
        )
        es_client.search.assert_called_once_with("margin")

    @patch("app.get_es_client")
    def test_search_endpoint_handles_es_timeout(self, get_es_client) -> None:
        es_client = get_es_client.return_value
        es_client.search.side_effect = ConnectionTimeout("timed out")

        response = self.client.get("/api/circulars/search?q=margin")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.get_json(),
            {
                "error": "Search service is temporarily unavailable.",
                "query": "margin",
                "results": [],
            },
        )
        es_client.search.assert_called_once_with("margin")

    def test_search_endpoint_requires_q(self) -> None:
        response = self.client.get("/api/circulars/search")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.get_json(),
            {"error": "Query parameter 'q' is required."},
        )

    def test_search_endpoint_rejects_blank_q(self) -> None:
        response = self.client.get("/api/circulars/search?q=   ")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.get_json(),
            {"error": "Query parameter 'q' is required."},
        )


if __name__ == "__main__":
    unittest.main()
