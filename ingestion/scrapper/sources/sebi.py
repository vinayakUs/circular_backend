from __future__ import annotations

from datetime import date, datetime
from html import unescape
import logging
import os
import re
import time
from urllib.parse import parse_qs, urlencode, urljoin, urlparse
import requests

from ingestion.scrapper._proxy import get_requests_proxies

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

    DEPARTMENT_MAPPING: dict[int, str] = {
        75: "AIF & FP Investors Dept",
        1: "Corporation Finance Department",
        3: "Dept of Economic and Policy Analysis",
        64: "Dept of Debt and Hybrid Securities",
        6: "Enforcement Department",
        35: "Information Technology Department",
        9: "Investment Management Department",
        14: "Market Intermediaries Reg and Supervision",
        15: "Market Regulation Department",
        19: "Office of Investor Assistance and Education",
    }

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        print(f"REQUESTS_CA_BUNDLE: {os.getenv('REQUESTS_CA_BUNDLE')}")
        self.detail_timeout_seconds = Config.SEBI_DETAIL_TIMEOUT_SECONDS
        self.listing_timeout_seconds = int(
            os.getenv("SEBI_LISTING_TIMEOUT_SECONDS", "300")
        )
        self.listing_max_retries = max(1, int(os.getenv("SEBI_LISTING_MAX_RETRIES", "3")))
        self.listing_retry_backoff_seconds = max(
            0.0, float(os.getenv("SEBI_LISTING_RETRY_BACKOFF_SECONDS", "2"))
        )
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
        all_rows: list[tuple[date, str, str, str]] = []
        for dept_id, dept_name in self.DEPARTMENT_MAPPING.items():
            self.logger.info("Fetching SEBI listing for dept_id=%s dept_name=%s", dept_id, dept_name)
            dept_rows = self._fetch_all_listing_rows(from_date, to_date, dept_id, dept_name)
            all_rows.extend(dept_rows)
            self.logger.info("Fetched %s rows for dept_id=%s dept_name=%s", len(dept_rows), dept_id, dept_name)

        self.logger.info("SEBI listing rows fetched count=%s", len(all_rows))
        in_range_rows = [
            row for row in all_rows if from_date <= row[0] <= to_date
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

        for issue_date, title, detail_url, dept_name in in_range_rows:
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
            
            print("===================")
            print(detail_url)
            circular = self._parse_detail_page(detail_url, issue_date, title, detail_html, dept_name)
            print(circular)

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
        self, from_date: date, to_date: date, dept_id: int = -1, dept_name: str = "All Department"
    ) -> list[tuple[date, str, str, str]]:
        page_index = 1
        page_count = 0
        rows: list[tuple[date, str, str, str]] = []
        seen_signatures: set[str] = set()

        while True:
            html = self._fetch_listing_page(from_date, to_date, page_index, dept_id)
            signature = self._page_signature(html)
            if signature in seen_signatures:
                self.logger.info(
                    "Stopping SEBI pagination because page signature repeated page_index=%s dept_id=%s dept_name=%s",
                    page_index,
                    dept_id,
                    dept_name,
                )
                break

            seen_signatures.add(signature)
            page_rows = self._parse_listing_rows(html)
            if not page_rows:
                self.logger.info(
                    "Stopping SEBI pagination because page returned zero rows page_index=%s dept_id=%s dept_name=%s",
                    page_index,
                    dept_id,
                    dept_name,
                )
                break

            page_count += 1
            rows.extend([(*row, dept_name) for row in page_rows])
            self.logger.info(
                "Parsed SEBI listing page page_index=%s rows=%s cumulative_rows=%s dept_id=%s dept_name=%s",
                page_index,
                len(page_rows),
                len(rows),
                dept_id,
                dept_name,
            )

            if not self._has_next_page(html):
                break

            page_index += 1

        self.logger.info(
            "Completed SEBI listing pagination pages=%s rows=%s dept_id=%s dept_name=%s",
            page_count,
            len(rows),
            dept_id,
            dept_name,
        )
        return rows

    def _build_listing_payload(
        self, from_date: date, to_date: date, page_index: int, dept_id: int = -1
    ) -> dict[str, str]:
        payload = dict(self.base_payload)
        payload["fromDate"] = from_date.strftime("%d-%m-%Y")
        payload["toDate"] = to_date.strftime("%d-%m-%Y")
        payload["deptId"] = str(dept_id)

        if page_index == 1:
            payload["next"] = "s"
            payload["nextValue"] = "1"
        else:
            payload["next"] = "n"
            payload["nextValue"] = str(page_index - 1)

        return payload

    def _fetch_listing_page(self, from_date: date, to_date: date, page_index: int, dept_id: int = -1) -> str:
        payload = urlencode(self._build_listing_payload(from_date, to_date, page_index, dept_id))
        last_error: Exception | None = None

        for attempt in range(1, self.listing_max_retries + 1):
            self.logger.info(
                "Fetching SEBI listing page from_date=%s to_date=%s page_index=%s attempt=%s/%s timeout_seconds=%s",
                from_date,
                to_date,
                page_index,
                attempt,
                self.listing_max_retries,
                self.listing_timeout_seconds,
            )
            try:
                response = requests.post(
                    self.LISTING_URL,
                    data=payload,
                    headers=self.default_headers,
                    timeout=self.listing_timeout_seconds,
                    allow_redirects=True,
                    proxies=get_requests_proxies(self.LISTING_URL),
                    verify=False,
                )
                return response.text
            except Exception as exc:
                last_error = exc
                if attempt >= self.listing_max_retries:
                    break

                sleep_seconds = self.listing_retry_backoff_seconds * attempt
                self.logger.warning(
                    "Retrying SEBI listing page after fetch failure from_date=%s to_date=%s page_index=%s attempt=%s/%s backoff_seconds=%.2f error=%s",
                    from_date,
                    to_date,
                    page_index,
                    attempt,
                    self.listing_max_retries,
                    sleep_seconds,
                    exc,
                )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

        assert last_error is not None
        raise last_error

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
                response = requests.get(
                    detail_url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=self.detail_timeout_seconds,
                    allow_redirects=True,
                    proxies=get_requests_proxies(detail_url),
                    verify=False,
                )
                return response.text
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
        self, detail_url: str, issue_date: date, title: str, html: str, dept_name: str = ""
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
        print("=============pdf url==============",pdf_url)
        if not pdf_url:
            return None

        circular_id = self.parse_circular_id(raw_circular_number)
        return Circular(
            source=self.source_name,
            circular_id=circular_id,
            full_reference=circular_id,
            department=dept_name,
            title=title,
            issue_date=issue_date,
            applicable_to_nse=False,
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
            applicable_to_nse=False,
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

        raw_src = iframe_match.group(1)
        # Remove any ../ sequences used for path traversal
        while raw_src.startswith("../"):
            raw_src = raw_src[3:]
        iframe_src = urljoin(detail_url, raw_src)
        print("=============iframe src==============",iframe_src)

        parsed = urlparse(iframe_src)
        file_param = parse_qs(parsed.query).get("file", [None])[0]
        if file_param:
            return urljoin(self.base_url, file_param)
        if iframe_src.lower().endswith(".pdf"):
            return iframe_src
        return None

    def _page_signature(self, html: str) -> str:
        rows = self._parse_listing_rows(html)
        if not rows:
            return ""
        first_row = rows[0]
        last_row = rows[-1]
        return f"{first_row[0]}|{first_row[2]}|{last_row[0]}|{last_row[2]}|{len(rows)}"
