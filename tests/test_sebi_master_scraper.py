from datetime import date
import json
import unittest
from unittest.mock import patch

import requests

from ingestion.scrapper.sources.sebi_master import SEBIMasterCircularScraper


SAMPLE_PAYLOAD = {
    "source": "sebi",
    "count": 2,
    "records": [
        {
            "date": "May 15, 2026",
            "title": "Master Circular on Surveillance of Securities Market",
            "circular_id": "101473",
            "html_url": "https://www.sebi.gov.in/legal/master-circulars/may-2026/master-circular-on-surveillance-of-securities-market_101473.html",
            "download_url": "https://www.sebi.gov.in/sebi_data/attachdocs/may-2026/1778851789798.pdf",
            "type": "Master Circular",
            "department": "Integrated Surveillance Department",
        },
        {
            "date": "Apr 10, 2026",
            "title": "Master Circular on Issue and Listing of Non-Convertible Securities",
            "circular_id": "101250",
            "html_url": "https://www.sebi.gov.in/legal/master-circulars/apr-2026/master-circular-on-issue-and-listing-of-non-convertible-securities_101250.html",
            "download_url": "https://www.sebi.gov.in/sebi_data/attachdocs/apr-2026/1776123456789.pdf",
            "type": "Master Circular",
            "department": "Department of Debt and Hybrid Securities",
        },
    ],
}


class SEBIMasterScraperMappingTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = SEBIMasterCircularScraper()
        self.scraper.base_url = "http://localhost:8001"

    def test_source_name_and_endpoint(self) -> None:
        self.assertEqual(self.scraper.source_name, "SEBI_MASTER")
        self.assertEqual(
            self.scraper.ENDPOINT,
            "/api/enforcement_crawler/sebi/master-circular",
        )

    def test_parse_response_maps_records_to_circular(self) -> None:
        circulars = self.scraper.parse_response(SAMPLE_PAYLOAD)

        self.assertEqual(len(circulars), 2)

        first = circulars[0]
        self.assertEqual(first.source, "SEBI_MASTER")
        self.assertEqual(first.circular_id, "101473")
        self.assertEqual(first.full_reference, "101473")
        self.assertEqual(first.department, "Integrated Surveillance Department")
        self.assertEqual(
            first.title,
            "Master Circular on Surveillance of Securities Market",
        )
        self.assertEqual(first.issue_date, date(2026, 5, 15))
        self.assertFalse(first.applicable_to_nse)
        self.assertEqual(
            first.url,
            "https://www.sebi.gov.in/legal/master-circulars/may-2026/master-circular-on-surveillance-of-securities-market_101473.html",
        )
        self.assertEqual(
            first.pdf_url,
            "https://www.sebi.gov.in/sebi_data/attachdocs/may-2026/1778851789798.pdf",
        )
        self.assertEqual(first.source_item_key, first.url)

        second = circulars[1]
        self.assertEqual(second.circular_id, "101250")
        self.assertEqual(
            second.department,
            "Department of Debt and Hybrid Securities",
        )
        self.assertEqual(second.issue_date, date(2026, 4, 10))

    def test_parse_response_handles_empty_records(self) -> None:
        self.assertEqual(self.scraper.parse_response({}), [])
        self.assertEqual(self.scraper.parse_response({"records": []}), [])

    def test_parse_response_skips_unsupported_extension(self) -> None:
        payload = {
            "records": [
                {
                    "date": "May 15, 2026",
                    "title": "Real",
                    "circular_id": "101473",
                    "html_url": "https://www.sebi.gov.in/legal/master-circulars/may-2026/x_101473.html",
                    "download_url": "https://www.sebi.gov.in/sebi_data/attachdocs/may-2026/x.pdf",
                    "type": "Master Circular",
                    "department": "ISD",
                },
                {
                    "date": "May 16, 2026",
                    "title": "HTML only",
                    "circular_id": "101474",
                    "html_url": "https://www.sebi.gov.in/legal/master-circulars/may-2026/x_101474.html",
                    "download_url": "https://www.sebi.gov.in/notice/x.html",
                    "type": "Master Circular",
                    "department": "ISD",
                },
            ]
        }

        circulars = self.scraper.parse_response(payload)

        self.assertEqual(len(circulars), 1)
        self.assertEqual(circulars[0].circular_id, "101473")

    def test_parse_response_skips_missing_circular_id_or_html_url(self) -> None:
        payload = {
            "records": [
                {
                    "date": "May 15, 2026",
                    "title": "Missing id",
                    "circular_id": "",
                    "html_url": "https://www.sebi.gov.in/legal/master-circulars/may-2026/x.html",
                    "download_url": "https://www.sebi.gov.in/sebi_data/attachdocs/may-2026/x.pdf",
                    "type": "Master Circular",
                    "department": "ISD",
                },
                {
                    "date": "May 15, 2026",
                    "title": "Missing html url",
                    "circular_id": "101473",
                    "html_url": "",
                    "download_url": "https://www.sebi.gov.in/sebi_data/attachdocs/may-2026/x.pdf",
                    "type": "Master Circular",
                    "department": "ISD",
                },
                {
                    "date": "May 15, 2026",
                    "title": "Real",
                    "circular_id": "101473",
                    "html_url": "https://www.sebi.gov.in/legal/master-circulars/may-2026/x_101473.html",
                    "download_url": "https://www.sebi.gov.in/sebi_data/attachdocs/may-2026/x.pdf",
                    "type": "Master Circular",
                    "department": "ISD",
                },
            ]
        }

        circulars = self.scraper.parse_response(payload)

        self.assertEqual(len(circulars), 1)
        self.assertEqual(circulars[0].circular_id, "101473")

    def test_parse_circular_id_collapses_whitespace(self) -> None:
        self.assertEqual(self.scraper.parse_circular_id("  101473  "), "101473")
        self.assertEqual(self.scraper.parse_circular_id("101 473"), "101 473")


class SEBIMasterScraperFetchTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = SEBIMasterCircularScraper()
        self.scraper.base_url = "http://localhost:8001"

    @patch.object(SEBIMasterCircularScraper, "_fetch_with_retry")
    def test_fetch_circulars_posts_dd_mm_yyyy_payload(self, fetch_with_retry) -> None:
        fetch_with_retry.return_value = json.dumps({"records": []})

        self.scraper._fetch_circulars(date(2026, 5, 1), date(2026, 5, 30))

        call_kwargs = fetch_with_retry.call_args.kwargs
        self.assertEqual(call_kwargs["method"], "POST")
        self.assertEqual(
            call_kwargs["url"],
            "http://localhost:8001/api/enforcement_crawler/sebi/master-circular",
        )
        self.assertEqual(call_kwargs["headers"]["Content-Type"], "application/json")

        body = json.loads(call_kwargs["data"])
        self.assertEqual(body, {"from_date": "01-05-2026", "to_date": "30-05-2026"})

    @patch.object(SEBIMasterCircularScraper, "_fetch_with_retry")
    def test_fetch_circulars_parses_json_response(self, fetch_with_retry) -> None:
        fetch_with_retry.return_value = json.dumps(SAMPLE_PAYLOAD)

        payload = self.scraper._fetch_circulars(date(2026, 5, 1), date(2026, 5, 30))

        self.assertEqual(payload["count"], 2)
        self.assertEqual(payload["records"][0]["circular_id"], "101473")


class SEBIMasterScraperErrorPropagationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = SEBIMasterCircularScraper()
        self.scraper.base_url = "http://localhost:8001"
        self.scraper.backoff_seconds = 0
        self.scraper.max_retries = 2

    @patch.object(SEBIMasterCircularScraper, "_fetch_with_retry")
    def test_propagates_connection_error(self, fetch_with_retry) -> None:
        fetch_with_retry.side_effect = requests.exceptions.ConnectionError("refused")

        with self.assertRaises(requests.exceptions.ConnectionError):
            self.scraper._fetch_circulars(date(2026, 5, 1), date(2026, 5, 30))

    @patch.object(SEBIMasterCircularScraper, "_fetch_with_retry")
    def test_propagates_http_error(self, fetch_with_retry) -> None:
        fetch_with_retry.side_effect = requests.exceptions.HTTPError("503 Service Unavailable")

        with self.assertRaises(requests.exceptions.HTTPError):
            self.scraper._fetch_circulars(date(2026, 5, 1), date(2026, 5, 30))

    @patch.object(SEBIMasterCircularScraper, "_fetch_with_retry")
    def test_propagates_invalid_json(self, fetch_with_retry) -> None:
        fetch_with_retry.return_value = "<html>500</html>"

        with self.assertRaises(json.JSONDecodeError):
            self.scraper._fetch_circulars(date(2026, 5, 1), date(2026, 5, 30))

    def test_detect_new_surfaces_api_failure(self) -> None:
        scraper = SEBIMasterCircularScraper()
        scraper.base_url = "http://localhost:8001"
        scraper.backoff_seconds = 0
        scraper.max_retries = 2

        with patch.object(
            scraper,
            "_fetch_with_retry",
            side_effect=requests.exceptions.ConnectionError("refused"),
        ):
            with self.assertRaises(requests.exceptions.ConnectionError):
                scraper.detect_new(date(2026, 5, 1), date(2026, 5, 30))


if __name__ == "__main__":
    unittest.main()