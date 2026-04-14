from __future__ import annotations

from datetime import date, datetime
from html import unescape
import logging
import re
import time
from urllib.parse import parse_qs, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

from config import Config
from ingestion.scrapper.base import IScraper, ScrapeDetectionResult
from ingestion.scrapper.dto import Circular
from ingestion.scrapper.registry import ScraperRegistry


@ScraperRegistry.register
class SEBIScraper(IScraper):
    source_name = "SEBI"
    LISTING_URL = "https://www.sebi.gov.in/sebiweb/ajax/home/getnewslistinfo.jsp"
    base_url = "https://www.sebi.gov.in"
    default_headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.sebi.gov.in/",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    base_payload = {
        "search": "",
        "fromDate": "",
        "toDate": "",
        "fromYear": "",
        "toYear": "",
        "deptId": "-1",
        "sid": "1",
        "ssid": "7",
        "smid": "0",
        "ssidhidden": "7",
        "intmid": "-1",
        "sText": "Legal",
        "ssText": "Circulars",
        "smText": "",
        "doDirect": "-1",
    }
    FORCE_FAIL_CIRCULAR_ID = "HO/49/14/11(12)2026-CFD-POD1/I/8806/2026"

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.detail_timeout_seconds = Config.SEBI_DETAIL_TIMEOUT_SECONDS
        self.detail_max_retries = max(1, Config.SEBI_DETAIL_MAX_RETRIES)
        self.detail_retry_backoff_seconds = max(
            0.0, Config.SEBI_DETAIL_RETRY_BACKOFF_SECONDS
        )

    def detect_new(self, from_date: date, to_date: date) -> ScrapeDetectionResult:
        self.logger.info(
            "Fetching SEBI circulars from_date=%s to_date=%s",
            from_date,
            to_date,
        )
        listing_rows = self._fetch_all_listing_rows(from_date, to_date)
        self.logger.info("SEBI listing rows fetched count=%s", len(listing_rows))
        in_range_rows = [
            row for row in listing_rows if from_date <= row[0] <= to_date
        ]
        self.logger.info(
            "SEBI rows filtered by date from_date=%s to_date=%s count=%s",
            from_date,
            to_date,
            len(in_range_rows),
        )

        circulars: list[Circular] = []
        failed_circulars: list[Circular] = []
        seen_detail_urls: set[str] = set()
        duplicate_count = 0
        skipped_count = 0

        for issue_date, title, detail_url in in_range_rows:
            if detail_url in seen_detail_urls:
                duplicate_count += 1
                continue

            seen_detail_urls.add(detail_url)
            try:
                detail_html = self._fetch_detail_page(detail_url)
            except Exception as exc:
                skipped_count += 1
                failed_circulars.append(
                    self._build_failed_circular(
                        issue_date,
                        title,
                        detail_url,
                        f"SEBI detail fetch failed: {exc}",
                    )
                )
                self.logger.warning(
                    "Skipping SEBI detail page due to fetch failure detail_url=%s error=%s",
                    detail_url,
                    exc,
                )
                continue

            circular = self._parse_detail_page(detail_url, issue_date, title, detail_html)
            if circular is None:
                skipped_count += 1
                failed_circulars.append(
                    self._build_failed_circular(
                        issue_date,
                        title,
                        detail_url,
                        "SEBI detail parse failed: missing circular number or PDF url",
                    )
                )
                self.logger.warning(
                    "Skipping SEBI detail page due to missing circular number or PDF url detail_url=%s",
                    detail_url,
                )
                continue



            # code for testing error handling, should be removed in production
            # if circular.circular_id == self.FORCE_FAIL_CIRCULAR_ID:
            #     skipped_count += 1
            #     failed_circulars.append(
            #         self._build_failed_circular(
            #             issue_date,
            #             title,
            #             detail_url,
            #             f"SEBI test failure forced for circular_id={circular.circular_id}",
            #         )
            #     )
            #     self.logger.warning(
            #         "Forced SEBI failure for testing circular_id=%s detail_url=%s",
            #         circular.circular_id,
            #         detail_url,
            #     )
            #     continue
            

            circulars.append(circular)

        self.logger.info(
            "Parsed SEBI circulars from_date=%s to_date=%s count=%s duplicates_skipped=%s invalid_details_skipped=%s",
            from_date,
            to_date,
            len(circulars),
            duplicate_count,
            skipped_count,
        )
        return ScrapeDetectionResult(
            circulars=circulars,
            failed_circulars=failed_circulars,
            has_incomplete_items=bool(failed_circulars),
        )

    def get_pdf_download_url(self, circular_id: str) -> str:
        return f"{self.base_url}/legal/circulars/{circular_id}.pdf"

    def parse_circular_id(self, raw_id: str) -> str:
        return re.sub(r"\s+", " ", raw_id.strip())

    def _fetch_all_listing_rows(
        self, from_date: date, to_date: date
    ) -> list[tuple[date, str, str]]:
        page_index = 1
        page_count = 0
        rows: list[tuple[date, str, str]] = []
        seen_signatures: set[str] = set()

        while True:
            html = self._fetch_listing_page(from_date, to_date, page_index)
            signature = self._page_signature(html)
            if signature in seen_signatures:
                self.logger.info(
                    "Stopping SEBI pagination because page signature repeated page_index=%s",
                    page_index,
                )
                break

            seen_signatures.add(signature)
            page_rows = self._parse_listing_rows(html)
            if not page_rows:
                self.logger.info(
                    "Stopping SEBI pagination because page returned zero rows page_index=%s",
                    page_index,
                )
                break

            page_count += 1
            rows.extend(page_rows)
            self.logger.info(
                "Parsed SEBI listing page page_index=%s rows=%s cumulative_rows=%s",
                page_index,
                len(page_rows),
                len(rows),
            )

            if not self._has_next_page(html):
                break

            page_index += 1

        self.logger.info(
            "Completed SEBI listing pagination pages=%s rows=%s", page_count, len(rows)
        )
        return rows

    def _build_listing_payload(
        self, from_date: date, to_date: date, page_index: int
    ) -> dict[str, str]:
        payload = dict(self.base_payload)
        payload["fromDate"] = from_date.strftime("%d-%m-%Y")
        payload["toDate"] = to_date.strftime("%d-%m-%Y")

        if page_index == 1:
            payload["next"] = "s"
            payload["nextValue"] = "1"
        else:
            payload["next"] = "n"
            payload["nextValue"] = str(page_index - 1)

        return payload

    def _fetch_listing_page(self, from_date: date, to_date: date, page_index: int) -> str:
        payload = urlencode(self._build_listing_payload(from_date, to_date, page_index)).encode()
        request = Request(self.LISTING_URL, data=payload, headers=self.default_headers)
        with urlopen(request, timeout=30) as response:
            return response.read().decode("utf-8", "ignore")

    def _parse_listing_rows(self, html: str) -> list[tuple[date, str, str]]:
        pattern = re.compile(
            r"<tr[^>]*>\s*<td>([^<]+)</td><td><a href='([^']+)'[^>]*title=\"([^\"]+)\"",
            re.IGNORECASE,
        )
        rows: list[tuple[date, str, str]] = []
        for raw_date, detail_url, raw_title in pattern.findall(html):
            rows.append(
                (
                    datetime.strptime(raw_date.strip(), "%b %d, %Y").date(),
                    unescape(raw_title.strip()),
                    urljoin(self.base_url, detail_url.strip()),
                )
            )
        return rows

    def _has_next_page(self, html: str) -> bool:
        return "title='Next'" in html or 'title="Next"' in html

    def _fetch_detail_page(self, detail_url: str) -> str:
        request = Request(detail_url, headers={"User-Agent": "Mozilla/5.0"})
        last_error: Exception | None = None

        for attempt in range(1, self.detail_max_retries + 1):
            self.logger.info(
                "Fetching SEBI detail page detail_url=%s attempt=%s/%s timeout_seconds=%s",
                detail_url,
                attempt,
                self.detail_max_retries,
                self.detail_timeout_seconds,
            )
            try:
                with urlopen(request, timeout=self.detail_timeout_seconds) as response:
                    return response.read().decode("utf-8", "ignore")
            except Exception as exc:
                last_error = exc
                if attempt >= self.detail_max_retries:
                    break

                sleep_seconds = self.detail_retry_backoff_seconds * attempt
                self.logger.warning(
                    "Retrying SEBI detail page after fetch failure detail_url=%s attempt=%s/%s backoff_seconds=%.2f error=%s",
                    detail_url,
                    attempt,
                    self.detail_max_retries,
                    sleep_seconds,
                    exc,
                )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

        assert last_error is not None
        raise last_error

    def _parse_detail_page(
        self, detail_url: str, issue_date: date, title: str, html: str
    ) -> Circular | None:
        circular_number_match = re.search(
            r"<span>\s*Circular No\.:\s*</span>\s*<span>([^<]+)</span>",
            html,
            re.IGNORECASE,
        )
        if not circular_number_match:
            return None

        raw_circular_number = unescape(circular_number_match.group(1)).strip()
        pdf_url = self._extract_pdf_url(detail_url, html)
        if not pdf_url:
            return None

        circular_id = self.parse_circular_id(raw_circular_number)
        return Circular(
            source=self.source_name,
            circular_id=circular_id,
            full_reference=circular_id,
            department="",
            title=title,
            issue_date=issue_date,
            effective_date=None,
            url=detail_url,
            pdf_url=pdf_url,
            source_item_key=detail_url,
        )

    def _build_failed_circular(
        self, issue_date: date, title: str, detail_url: str, error_message: str
    ) -> Circular:
        fallback_id = self._build_failed_circular_id(detail_url)
        return Circular(
            source=self.source_name,
            circular_id=fallback_id,
            full_reference=fallback_id,
            department="",
            title=title,
            issue_date=issue_date,
            effective_date=None,
            url=detail_url,
            pdf_url="",
            source_item_key=detail_url,
            error_message=error_message,
        )

    def _build_failed_circular_id(self, detail_url: str) -> str:
        parsed = urlparse(detail_url)
        slug = parsed.path.rstrip("/").rsplit("/", 1)[-1] or "detail"
        return f"SEBI_PENDING::{slug[:32].upper()}"

    def _extract_pdf_url(self, detail_url: str, html: str) -> str | None:
        iframe_match = re.search(r"<iframe[^>]+src='([^']+)'", html, re.IGNORECASE)
        if not iframe_match:
            iframe_match = re.search(r'<iframe[^>]+src="([^"]+)"', html, re.IGNORECASE)
        if not iframe_match:
            return None

        iframe_src = urljoin(detail_url, iframe_match.group(1))
        parsed = urlparse(iframe_src)
        return parse_qs(parsed.query).get("file", [None])[0]

    def _page_signature(self, html: str) -> str:
        rows = self._parse_listing_rows(html)
        if not rows:
            return ""
        first_row = rows[0]
        last_row = rows[-1]
        return f"{first_row[0]}|{first_row[2]}|{last_row[0]}|{last_row[2]}|{len(rows)}"
