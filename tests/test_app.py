import unittest
from unittest.mock import patch

from app import create_app


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
            {
                "_id": "chunk-1",
                "_score": 1.23,
                "_source": {
                    "title": "SEBI circular",
                    "chunk_text": "margin framework update",
                },
            }
        ]

        response = self.client.get("/api/circulars/search?q=margin")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {
                "query": "margin",
                "results": [
                    {
                        "_id": "chunk-1",
                        "_score": 1.23,
                        "_source": {
                            "title": "SEBI circular",
                            "chunk_text": "margin framework update",
                        },
                    }
                ],
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
