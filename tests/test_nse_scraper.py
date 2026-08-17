from datetime import date
import json
import unittest
from unittest.mock import patch

import requests

from ingestion.scrapper.sources.nse import NSEScraper


class NSEScraperTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = NSEScraper()
        self.scraper.base_url = "http://localhost:8001"

    def test_parse_response_maps_new_api_records_to_circular(self) -> None:
        payload = {
            "source": "nse",
            "count": 2,
            "records": [
                {
                    "source": "nse",
                    "circular_id": "CMPT74957",
                    "department": "Securities Lending & Borrowing Scheme",
                    "title": "Change in name and symbol of Gujarat Gas Limited",
                    "issue_date": "2026-06-30",
                    "download_url": "https://nsearchives.nseindia.com/content/circulars/CMPT74957.pdf",
                },
                {
                    "source": "nse",
                    "circular_id": "INVG74958",
                    "department": "Surveillance & Investigation",
                    "title": "SEBI Final order in the matter of Mauria Udyog Ltd.",
                    "issue_date": "2026-06-30",
                    "download_url": "https://nsearchives.nseindia.com/content/circulars/INVG74958.zip",
                },
            ],
        }

        circulars = self.scraper.parse_response(payload)

        self.assertEqual(len(circulars), 2)

        pdf_circular = circulars[0]
        self.assertEqual(pdf_circular.source, "NSE")
        self.assertEqual(pdf_circular.circular_id, "CMPT74957")
        self.assertEqual(
            pdf_circular.department,
            "Securities Lending & Borrowing Scheme",
        )
        self.assertEqual(pdf_circular.title, "Change in name and symbol of Gujarat Gas Limited")
        self.assertEqual(pdf_circular.issue_date, date(2026, 6, 30))
        self.assertEqual(pdf_circular.applicable_to_nse, True)
        self.assertEqual(pdf_circular.full_reference, "CMPT74957")
        self.assertEqual(
            pdf_circular.url,
            "https://nsearchives.nseindia.com/content/circulars/CMPT74957.pdf",
        )
        self.assertEqual(pdf_circular.pdf_url, pdf_circular.url)
        self.assertEqual(
            pdf_circular.source_item_key,
            "CMPT74957:Securities_Lending_&_Borrowing_Scheme",
        )

        zip_circular = circulars[1]
        self.assertEqual(zip_circular.circular_id, "INVG74958")
        self.assertEqual(
            zip_circular.department,
            "Surveillance & Investigation",
        )
        self.assertEqual(
            zip_circular.pdf_url,
            "https://nsearchives.nseindia.com/content/circulars/INVG74958.zip",
        )
        self.assertEqual(
            zip_circular.source_item_key,
            "INVG74958:Surveillance_&_Investigation",
        )

    def test_parse_response_skips_unsupported_extensions(self) -> None:
        payload = {
            "records": [
                {
                    "circular_id": "CMPT74957",
                    "department": "SLBS",
                    "title": "PDF one",
                    "issue_date": "2026-06-30",
                    "download_url": "https://nsearchives.nseindia.com/content/circulars/CMPT74957.pdf",
                },
                {
                    "circular_id": "HTML1",
                    "department": "SLBS",
                    "title": "Not a real circular",
                    "issue_date": "2026-06-30",
                    "download_url": "https://nsearchives.nseindia.com/notice/HTML1.html",
                },
            ]
        }

        circulars = self.scraper.parse_response(payload)

        self.assertEqual(len(circulars), 1)
        self.assertEqual(circulars[0].circular_id, "CMPT74957")

    def test_parse_response_skips_missing_circular_id(self) -> None:
        payload = {
            "records": [
                {
                    "circular_id": "",
                    "department": "SLBS",
                    "title": "Missing id",
                    "issue_date": "2026-06-30",
                    "download_url": "https://nsearchives.nseindia.com/content/circulars/X1.pdf",
                },
                {
                    "circular_id": "CMPT74957",
                    "department": "SLBS",
                    "title": "Real one",
                    "issue_date": "2026-06-30",
                    "download_url": "https://nsearchives.nseindia.com/content/circulars/CMPT74957.pdf",
                },
            ]
        }

        circulars = self.scraper.parse_response(payload)

        self.assertEqual(len(circulars), 1)
        self.assertEqual(circulars[0].circular_id, "CMPT74957")

    def test_parse_response_handles_empty_records(self) -> None:
        self.assertEqual(self.scraper.parse_response({}), [])
        self.assertEqual(self.scraper.parse_response({"records": []}), [])

    @patch.object(NSEScraper, "_fetch_with_retry")
    def test_fetch_circulars_posts_dd_mm_yyyy_payload(self, fetch_with_retry) -> None:
        fetch_with_retry.return_value = json.dumps({"records": []})

        self.scraper._fetch_circulars(date(2026, 6, 1), date(2026, 6, 30))

        fetch_with_retry.assert_called_once()
        call_kwargs = fetch_with_retry.call_args.kwargs
        self.assertEqual(call_kwargs["method"], "POST")
        self.assertEqual(
            call_kwargs["url"],
            "http://localhost:8001/api/enforcement_crawler/nse/circulars",
        )
        self.assertEqual(call_kwargs["headers"]["Content-Type"], "application/json")

        body = json.loads(call_kwargs["data"])
        self.assertEqual(body, {"from_date": "01-06-2026", "to_date": "30-06-2026"})

    @patch.object(NSEScraper, "_fetch_with_retry")
    def test_fetch_circulars_parses_json_response(self, fetch_with_retry) -> None:
        fetch_with_retry.return_value = json.dumps(
            {
                "records": [
                    {
                        "circular_id": "CMPT74957",
                        "department": "SLBS",
                        "title": "X",
                        "issue_date": "2026-06-30",
                        "download_url": "https://nsearchives.nseindia.com/content/circulars/CMPT74957.pdf",
                    }
                ]
            }
        )

        payload = self.scraper._fetch_circulars(date(2026, 6, 1), date(2026, 6, 30))

        self.assertIn("records", payload)
        self.assertEqual(payload["records"][0]["circular_id"], "CMPT74957")


class NSEScraperErrorPropagationTestCase(unittest.TestCase):
    """Verify failures from the crawler API bubble up cleanly so the
    orchestrator/runner can log and exit non-zero."""

    def setUp(self) -> None:
        self.scraper = NSEScraper()
        self.scraper.base_url = "http://localhost:8001"
        # Keep the test fast — zero backoff so retries don't sleep.
        self.scraper.backoff_seconds = 0
        self.scraper.max_retries = 3

    @patch.object(NSEScraper, "_fetch_with_retry")
    def test_propagates_connection_error(self, fetch_with_retry) -> None:
        fetch_with_retry.side_effect = requests.exceptions.ConnectionError("refused")

        with self.assertRaises(requests.exceptions.ConnectionError):
            self.scraper._fetch_circulars(date(2026, 6, 1), date(2026, 6, 30))

    @patch.object(NSEScraper, "_fetch_with_retry")
    def test_propagates_timeout(self, fetch_with_retry) -> None:
        fetch_with_retry.side_effect = requests.exceptions.Timeout("read timed out")

        with self.assertRaises(requests.exceptions.Timeout):
            self.scraper._fetch_circulars(date(2026, 6, 1), date(2026, 6, 30))

    @patch.object(NSEScraper, "_fetch_with_retry")
    def test_propagates_invalid_json(self, fetch_with_retry) -> None:
        fetch_with_retry.return_value = "<html><body>500 Internal Server Error</body></html>"

        with self.assertRaises(json.JSONDecodeError):
            self.scraper._fetch_circulars(date(2026, 6, 1), date(2026, 6, 30))

    @patch.object(NSEScraper, "_fetch_with_retry")
    def test_propagates_empty_body(self, fetch_with_retry) -> None:
        fetch_with_retry.return_value = ""

        with self.assertRaises(json.JSONDecodeError):
            self.scraper._fetch_circulars(date(2026, 6, 1), date(2026, 6, 30))

    def test_detect_new_surfaces_api_failure(self) -> None:
        """End-to-end: when the underlying API raises, detect_new propagates
        the original exception type — no swallowing."""
        scraper = NSEScraper()
        scraper.base_url = "http://localhost:8001"
        scraper.backoff_seconds = 0
        scraper.max_retries = 2

        with patch.object(
            scraper,
            "_fetch_with_retry",
            side_effect=requests.exceptions.ConnectionError("refused"),
        ):
            with self.assertRaises(requests.exceptions.ConnectionError):
                scraper.detect_new(date(2026, 6, 1), date(2026, 6, 30))


if __name__ == "__main__":
    unittest.main()