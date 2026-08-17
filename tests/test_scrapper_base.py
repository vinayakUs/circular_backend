from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import requests

from ingestion.scrapper.base import IScraper
from ingestion.scrapper.sources.nse import NSEScraper


class _StubScraper(IScraper):
    """Minimal concrete scraper used only to exercise _fetch_with_retry."""

    source_name = "STUB"

    def __init__(self) -> None:
        super().__init__()

    def detect_new(self, from_date, to_date):  # pragma: no cover - not exercised
        raise NotImplementedError

    def get_pdf_download_url(self, circular_id: str) -> str:  # pragma: no cover
        return ""

    def parse_circular_id(self, raw_id: str) -> str:  # pragma: no cover
        return raw_id


def _mock_response(status_code: int, body: str = "") -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.text = body
    response.content = body.encode("utf-8")
    response.headers = {"Content-Type": "text/plain"}
    http_error = requests.exceptions.HTTPError(f"{status_code} Server Error")
    http_error.response = response
    response.raise_for_status.side_effect = http_error
    return response


class FetchWithRetryStatusCheckTestCase(unittest.TestCase):
    """_fetch_with_retry must surface non-2xx as HTTPError so callers
    see a meaningful error type instead of a downstream JSON parse failure."""

    def setUp(self) -> None:
        self.scraper = _StubScraper()
        self.scraper.backoff_seconds = 0  # speed up retries in tests

    @patch("ingestion.scrapper.base.requests.request")
    def test_raises_on_http_500(self, mock_request) -> None:
        mock_request.return_value = _mock_response(500)

        with self.assertRaises(requests.exceptions.HTTPError):
            self.scraper._fetch_with_retry(
                method="GET",
                url="https://example.com/api",
                max_retries=2,
                backoff_seconds=0,
                timeout=5,
            )

    @patch("ingestion.scrapper.base.requests.request")
    def test_raises_on_http_404(self, mock_request) -> None:
        mock_request.return_value = _mock_response(404)

        with self.assertRaises(requests.exceptions.HTTPError):
            self.scraper._fetch_with_retry(
                method="GET",
                url="https://example.com/missing",
                max_retries=2,
                backoff_seconds=0,
                timeout=5,
            )

    @patch("ingestion.scrapper.base.requests.request")
    def test_raises_on_http_401(self, mock_request) -> None:
        mock_request.return_value = _mock_response(401)

        with self.assertRaises(requests.exceptions.HTTPError):
            self.scraper._fetch_with_retry(
                method="GET",
                url="https://example.com/protected",
                max_retries=2,
                backoff_seconds=0,
                timeout=5,
            )

    @patch("ingestion.scrapper.base.requests.request")
    def test_returns_text_on_200(self, mock_request) -> None:
        response = MagicMock()
        response.status_code = 200
        response.text = '{"ok": true}'
        response.raise_for_status.return_value = None
        mock_request.return_value = response

        body = self.scraper._fetch_with_retry(
            method="GET",
            url="https://example.com/api",
            max_retries=2,
            backoff_seconds=0,
            timeout=5,
        )

        self.assertEqual(body, '{"ok": true}')

    @patch("ingestion.scrapper.base.requests.request")
    def test_retries_5xx_then_succeeds(self, mock_request) -> None:
        ok = MagicMock()
        ok.status_code = 200
        ok.text = '{"ok": true}'
        ok.raise_for_status.return_value = None
        mock_request.side_effect = [
            _mock_response(503),
            _mock_response(503),
            ok,
        ]

        body = self.scraper._fetch_with_retry(
            method="GET",
            url="https://example.com/api",
            max_retries=3,
            backoff_seconds=0,
            timeout=5,
        )

        self.assertEqual(body, '{"ok": true}')
        self.assertEqual(mock_request.call_count, 3)

    @patch("ingestion.scrapper.base.requests.request")
    def test_exhausts_retries_on_persistent_5xx(self, mock_request) -> None:
        mock_request.return_value = _mock_response(502)

        with self.assertRaises(requests.exceptions.HTTPError):
            self.scraper._fetch_with_retry(
                method="GET",
                url="https://example.com/api",
                max_retries=3,
                backoff_seconds=0,
                timeout=5,
            )

        self.assertEqual(mock_request.call_count, 3)


class NSEScraperStatusCheckIntegrationTestCase(unittest.TestCase):
    """NSEScraper must surface HTTPError from a failing crawler API."""

    def setUp(self) -> None:
        self.scraper = NSEScraper()
        self.scraper.base_url = "http://localhost:8001"
        self.scraper.backoff_seconds = 0
        self.scraper.max_retries = 2

    @patch.object(NSEScraper, "_fetch_with_retry")
    def test_propagates_http_error_from_crawler_api(self, fetch_with_retry) -> None:
        fetch_with_retry.side_effect = requests.exceptions.HTTPError("503 Service Unavailable")

        with self.assertRaises(requests.exceptions.HTTPError):
            self.scraper._fetch_circulars(date(2026, 6, 1), date(2026, 6, 30))


from datetime import date  # noqa: E402  (placed after classes to mirror module structure)


if __name__ == "__main__":
    unittest.main()