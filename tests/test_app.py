import unittest
from datetime import date, datetime
from unittest.mock import patch
from uuid import UUID

from elastic_transport import ConnectionTimeout

from app import create_app
from ingestion.indexer.dto import IndexDocument, SearchHit
from ingestion.repository import CircularAssetRecord, CircularRecord


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
                    asset_id="asset-1",
                    asset_role="original_pdf",
                    source="SEBI",
                    title="SEBI circular",
                    department="Markets",
                    issue_date=date(2024, 1, 2),
                    effective_date=None,
                    full_reference="SEBI/HO/MRD/2024/1",
                    url="https://example.com/circular-1",
                    pdf_url="https://example.com/circular-1.pdf",
                    file_path="/tmp/circular-1.pdf",
                    archive_member_path=None,
                    content_hash="hash-1",
                    chunk_index=0,
                    chunk_text="margin framework update",
                    embedding=None,
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
                "strategy": "hybrid",
                "results": [
                    {
                        "id": "chunk-1",
                        "score": 1.23,
                        "preview": "<div class='preview'>...framework...</div>",
                        "document": {
                            "chunk_id": "chunk-1",
                            "circular_db_id": "db-1",
                            "circular_id": "circular-1",
                            "asset_id": "asset-1",
                            "asset_role": "original_pdf",
                            "source": "SEBI",
                            "title": "SEBI circular",
                            "department": "Markets",
                            "issue_date": "2024-01-02",
                            "effective_date": None,
                            "full_reference": "SEBI/HO/MRD/2024/1",
                            "url": "https://example.com/circular-1",
                            "pdf_url": "https://example.com/circular-1.pdf",
                            "file_path": "/tmp/circular-1.pdf",
                            "archive_member_path": None,
                            "content_hash": "hash-1",
                            "chunk_index": 0,
                            "chunk_text": "margin framework update",
                            "indexed_at": "2024-01-02T03:04:05",
                        },
                    }
                ],
            },
        )
        es_client.search.assert_called_once_with("margin", {"source": []}, strategy="hybrid")

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
        es_client.search.assert_called_once_with("margin", {"source": []}, strategy="hybrid")

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

    def test_search_endpoint_rejects_unknown_strategy(self) -> None:
        response = self.client.get("/api/circulars/search?q=margin&strategy=magic")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.get_json(),
            {"error": "Unsupported search strategy."},
        )

    @patch("app.CircularRepository")
    @patch("app.get_db_client")
    def test_circular_details_endpoint_returns_record_and_assets(
        self, get_db_client, repository_cls
    ) -> None:
        db_client = get_db_client.return_value
        db_client.get_pool.return_value = object()
        repository = repository_cls.return_value
        record = CircularRecord(
            id=UUID("11111111-1111-1111-1111-111111111111"),
            source="SEBI",
            circular_id="CIRCULAR-1",
            source_item_key="source-key-1",
            full_reference="SEBI/HO/MRD/2024/1",
            department="Markets",
            title="SEBI circular",
            issue_date=date(2024, 1, 2),
            effective_date=date(2024, 2, 1),
            url="https://example.com/circular-1",
            pdf_url="https://example.com/circular-1.pdf",
            status="FETCHED",
            file_path="/tmp/circular-1.pdf",
            content_hash="hash-1",
            error_message=None,
            detected_at=datetime(2024, 1, 2, 3, 4, 5),
            created_at=datetime(2024, 1, 2, 3, 4, 6),
            updated_at=datetime(2024, 1, 2, 3, 4, 7),
            es_indexed_at=datetime(2024, 1, 2, 3, 4, 8),
            es_chunk_count=12,
            es_index_name="circulars_chunks",
        )
        asset = CircularAssetRecord(
            id=UUID("22222222-2222-2222-2222-222222222222"),
            circular_id=record.id,
            asset_role="original_pdf",
            file_path="/tmp/circular-1.pdf",
            content_hash="hash-1",
            mime_type="application/pdf",
            archive_member_path=None,
            file_size_bytes=1024,
            created_at=datetime(2024, 1, 2, 3, 4, 9),
            updated_at=datetime(2024, 1, 2, 3, 4, 10),
        )
        repository.get_record_by_id.return_value = record
        repository.list_assets.return_value = [asset]

        response = self.client.get(
            "/api/circulars/record/11111111-1111-1111-1111-111111111111"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {
                "circular": {
                    "id": "11111111-1111-1111-1111-111111111111",
                    "source": "SEBI",
                    "circular_id": "CIRCULAR-1",
                    "source_item_key": "source-key-1",
                    "full_reference": "SEBI/HO/MRD/2024/1",
                    "department": "Markets",
                    "title": "SEBI circular",
                    "issue_date": "2024-01-02",
                    "effective_date": "2024-02-01",
                    "url": "https://example.com/circular-1",
                    "pdf_url": "https://example.com/circular-1.pdf",
                    "status": "FETCHED",
                    "file_path": "/tmp/circular-1.pdf",
                    "content_hash": "hash-1",
                    "error_message": None,
                    "detected_at": "2024-01-02T03:04:05",
                    "created_at": "2024-01-02T03:04:06",
                    "updated_at": "2024-01-02T03:04:07",
                    "es_indexed_at": "2024-01-02T03:04:08",
                    "es_chunk_count": 12,
                    "es_index_name": "circulars_chunks",
                },
                "assets": [
                    {
                        "id": "22222222-2222-2222-2222-222222222222",
                        "circular_id": "11111111-1111-1111-1111-111111111111",
                        "asset_role": "original_pdf",
                        "file_path": "/tmp/circular-1.pdf",
                        "content_hash": "hash-1",
                        "mime_type": "application/pdf",
                        "archive_member_path": None,
                        "file_size_bytes": 1024,
                        "created_at": "2024-01-02T03:04:09",
                        "updated_at": "2024-01-02T03:04:10",
                    }
                ],
            },
        )
        repository.get_record_by_id.assert_called_once_with(
            UUID("11111111-1111-1111-1111-111111111111")
        )
        repository.list_assets.assert_called_once_with(record.id)

    @patch("app.CircularRepository")
    @patch("app.get_db_client")
    def test_circular_details_endpoint_returns_404_when_missing(
        self, get_db_client, repository_cls
    ) -> None:
        db_client = get_db_client.return_value
        db_client.get_pool.return_value = object()
        repository = repository_cls.return_value
        repository.get_record_by_id.return_value = None

        response = self.client.get(
            "/api/circulars/record/33333333-3333-3333-3333-333333333333"
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(
            response.get_json(),
            {
                "error": "Circular not found.",
                "record_id": "33333333-3333-3333-3333-333333333333",
            },
        )
        repository.get_record_by_id.assert_called_once_with(
            UUID("33333333-3333-3333-3333-333333333333")
        )


if __name__ == "__main__":
    unittest.main()
