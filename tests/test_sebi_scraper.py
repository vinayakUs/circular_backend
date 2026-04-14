from datetime import date
import unittest
from unittest.mock import MagicMock, patch

from ingestion.scrapper.sources.sebi import SEBIScraper


LISTING_PAGE_ONE = """
<input type='hidden' name='totalpage' value='7' />
<input type='hidden' name='nextValue' value='1'/>
<div class='pagination_outer'>
<ul>
<li><a class='active'>1</a></li>
<li><a href="javascript: searchFormNewsList('n', '1');">2</a></li>
<li><a href="javascript: searchFormNewsList('n','-1')" title='Next'>Next</a></li>
</ul>
</div>
<table>
<tr role='row' class='odd'><td>Mar 25, 2026</td><td><a href='https://www.sebi.gov.in/legal/circulars/mar-2026/clarification_100565.html' target="_blank" title="Clarification regarding eligibility">Clarification regarding eligibility</a></td></tr>
<tr role='row' class='odd'><td>Mar 24, 2026</td><td><a href='https://www.sebi.gov.in/legal/circulars/mar-2026/other_100564.html' target="_blank" title="Other circular">Other circular</a></td></tr>
</table>
"""

LISTING_PAGE_TWO = """
<input type='hidden' name='totalpage' value='7' />
<input type='hidden' name='nextValue' value='2'/>
<table>
<tr role='row' class='odd'><td>Jan 16, 2026</td><td><a href='https://www.sebi.gov.in/legal/circulars/jan-2026/clarification_99106.html' target="_blank" title="January circular">January circular</a></td></tr>
<tr role='row' class='odd'><td>Jan 15, 2026</td><td><a href='https://www.sebi.gov.in/legal/circulars/jan-2026/out-of-range_99105.html' target="_blank" title="Another January circular">Another January circular</a></td></tr>
</table>
"""

DETAIL_PAGE = """
<div class='id_area'>
<span>Circular No.:  </span>
<span>HO/38/12/12(1)2026-MIRSD-SEC-FATF/I/7934/2026</span>
</div>
<div class='cover'>
<iframe src='../../../web/?file=https://www.sebi.gov.in/sebi_data/attachdocs/mar-2026/1774433908492.pdf' width='100%'></iframe>
</div>
"""


class SEBIScraperTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = SEBIScraper()
        self.scraper.detail_timeout_seconds = 5
        self.scraper.detail_max_retries = 3
        self.scraper.detail_retry_backoff_seconds = 0

    def test_build_listing_payload_uses_search_then_next_mode(self) -> None:
        first_payload = self.scraper._build_listing_payload(
            date(2025, 1, 1), date(2026, 4, 1), 1
        )
        second_payload = self.scraper._build_listing_payload(
            date(2025, 1, 1), date(2026, 4, 1), 2
        )
        third_payload = self.scraper._build_listing_payload(
            date(2025, 1, 1), date(2026, 4, 1), 3
        )

        self.assertEqual(first_payload["next"], "s")
        self.assertEqual(first_payload["nextValue"], "1")
        self.assertEqual(first_payload["doDirect"], "-1")
        self.assertEqual(first_payload["fromDate"], "01-01-2025")
        self.assertEqual(first_payload["toDate"], "01-04-2026")

        self.assertEqual(second_payload["next"], "n")
        self.assertEqual(second_payload["nextValue"], "1")
        self.assertEqual(third_payload["nextValue"], "2")

    def test_parse_listing_rows_extracts_date_title_and_detail_url(self) -> None:
        rows = self.scraper._parse_listing_rows(LISTING_PAGE_ONE)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], date(2026, 3, 25))
        self.assertEqual(rows[0][1], "Clarification regarding eligibility")
        self.assertEqual(
            rows[0][2],
            "https://www.sebi.gov.in/legal/circulars/mar-2026/clarification_100565.html",
        )

    def test_parse_detail_page_extracts_circular_and_pdf_url(self) -> None:
        circular = self.scraper._parse_detail_page(
            "https://www.sebi.gov.in/legal/circulars/mar-2026/clarification_100565.html",
            date(2026, 3, 25),
            "Clarification regarding eligibility",
            DETAIL_PAGE,
        )

        self.assertIsNotNone(circular)
        assert circular is not None
        self.assertEqual(
            circular.circular_id,
            "HO/38/12/12(1)2026-MIRSD-SEC-FATF/I/7934/2026",
        )
        self.assertEqual(
            circular.full_reference,
            "HO/38/12/12(1)2026-MIRSD-SEC-FATF/I/7934/2026",
        )
        self.assertEqual(
            circular.pdf_url,
            "https://www.sebi.gov.in/sebi_data/attachdocs/mar-2026/1774433908492.pdf",
        )

    def test_parse_circular_id_preserves_real_reference(self) -> None:
        normalized = self.scraper.parse_circular_id(
            " HO/38/12/12(1)2026-MIRSD-SEC-FATF/I/7934/2026 "
        )

        self.assertEqual(
            normalized,
            "HO/38/12/12(1)2026-MIRSD-SEC-FATF/I/7934/2026",
        )

    def test_has_next_page_checks_next_link(self) -> None:
        self.assertTrue(self.scraper._has_next_page(LISTING_PAGE_ONE))
        self.assertFalse(self.scraper._has_next_page("<table></table>"))

    @patch.object(SEBIScraper, "_fetch_detail_page")
    @patch.object(SEBIScraper, "_fetch_listing_page")
    def test_detect_new_uses_search_then_next_pagination(
        self, fetch_listing_page, fetch_detail_page
    ) -> None:
        fetch_listing_page.side_effect = [LISTING_PAGE_ONE, LISTING_PAGE_TWO]
        fetch_detail_page.return_value = DETAIL_PAGE

        result = self.scraper.detect_new(date(2026, 3, 24), date(2026, 3, 25))

        self.assertFalse(result.has_incomplete_items)
        self.assertEqual(result.failed_circulars, [])
        self.assertEqual(len(result.circulars), 2)
        self.assertEqual(
            [circular.title for circular in result.circulars],
            ["Clarification regarding eligibility", "Other circular"],
        )
        self.assertEqual(fetch_listing_page.call_count, 2)
        first_call = fetch_listing_page.call_args_list[0].args
        second_call = fetch_listing_page.call_args_list[1].args
        self.assertEqual(first_call, (date(2026, 3, 24), date(2026, 3, 25), 1))
        self.assertEqual(second_call, (date(2026, 3, 24), date(2026, 3, 25), 2))
        self.assertEqual(fetch_detail_page.call_count, 2)

    @patch.object(SEBIScraper, "_fetch_detail_page")
    @patch.object(SEBIScraper, "_fetch_listing_page")
    def test_detect_new_skips_detail_fetch_failures(
        self, fetch_listing_page, fetch_detail_page
    ) -> None:
        fetch_listing_page.return_value = LISTING_PAGE_ONE
        fetch_detail_page.side_effect = TimeoutError("timed out")

        result = self.scraper.detect_new(date(2026, 3, 24), date(2026, 3, 25))

        self.assertTrue(result.has_incomplete_items)
        self.assertEqual(result.circulars, [])
        self.assertEqual(len(result.failed_circulars), 2)
        self.assertEqual(
            result.failed_circulars[0].source_item_key,
            "https://www.sebi.gov.in/legal/circulars/mar-2026/clarification_100565.html",
        )
        self.assertEqual(
            result.failed_circulars[0].error_message,
            "SEBI detail fetch failed: timed out",
        )
        self.assertEqual(fetch_detail_page.call_count, 2)

    @patch.object(SEBIScraper, "_fetch_detail_page")
    @patch.object(SEBIScraper, "_fetch_listing_page")
    def test_detect_new_returns_failed_placeholder_for_malformed_detail(
        self, fetch_listing_page, fetch_detail_page
    ) -> None:
        fetch_listing_page.return_value = LISTING_PAGE_ONE
        fetch_detail_page.return_value = "<html><body>No iframe or circular number</body></html>"

        result = self.scraper.detect_new(date(2026, 3, 24), date(2026, 3, 25))

        self.assertTrue(result.has_incomplete_items)
        self.assertEqual(result.circulars, [])
        self.assertEqual(len(result.failed_circulars), 2)
        self.assertEqual(
            result.failed_circulars[0].error_message,
            "SEBI detail parse failed: missing circular number or PDF url",
        )

    @patch("ingestion.scrapper.sources.sebi.time.sleep")
    @patch("ingestion.scrapper.sources.sebi.urlopen")
    def test_fetch_detail_page_retries_then_succeeds(
        self, mock_urlopen, mock_sleep
    ) -> None:
        response = MagicMock()
        response.read.return_value = DETAIL_PAGE.encode("utf-8")
        response.__enter__.return_value = response
        response.__exit__.return_value = None
        mock_urlopen.side_effect = [TimeoutError("timed out"), response]

        html = self.scraper._fetch_detail_page(
            "https://www.sebi.gov.in/legal/circulars/mar-2026/clarification_100565.html"
        )

        self.assertEqual(html, DETAIL_PAGE)
        self.assertEqual(mock_urlopen.call_count, 2)
        mock_sleep.assert_not_called()

    @patch("ingestion.scrapper.sources.sebi.time.sleep")
    @patch("ingestion.scrapper.sources.sebi.urlopen")
    def test_fetch_detail_page_raises_after_max_retries(
        self, mock_urlopen, mock_sleep
    ) -> None:
        mock_urlopen.side_effect = TimeoutError("timed out")

        with self.assertRaises(TimeoutError):
            self.scraper._fetch_detail_page(
                "https://www.sebi.gov.in/legal/circulars/mar-2026/clarification_100565.html"
            )

        self.assertEqual(mock_urlopen.call_count, 3)
        mock_sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
