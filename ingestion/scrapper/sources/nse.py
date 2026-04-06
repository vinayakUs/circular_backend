from __future__ import annotations

from datetime import date, datetime
import json
import logging
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingestion.scrapper.base import IScraper
from ingestion.scrapper.dto import Circular
from ingestion.scrapper.registry import ScraperRegistry


@ScraperRegistry.register
class NSEScraper(IScraper):
    source_name = "NSE"
    API_URL = "https://www.nseindia.com/api/circulars"
    base_url = "https://www.nseindia.com"
    default_headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nseindia.com/",
    }

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def detect_new(self, from_date: date, to_date: date) -> list[Circular]:
        self.logger.info(
            "Fetching NSE circulars from_date=%s to_date=%s",
            from_date,
            to_date,
        )
        payload = self._fetch_circulars(from_date, to_date)
        circulars = self.parse_response(payload)
        self.logger.info(
            "Parsed NSE circulars from_date=%s to_date=%s count=%s",
            from_date,
            to_date,
            len(circulars),
        )
        return circulars

    def get_pdf_download_url(self, circular_id: str) -> str:
        normalized_id = self.parse_circular_id(circular_id)
        return f"https://nsearchives.nseindia.com/content/circulars/{normalized_id}.pdf"

    def parse_circular_id(self, raw_id: str) -> str:
        return raw_id.replace("/", "").strip().upper()

    def parse_response(self, payload: dict) -> list[Circular]:
        circulars: list[Circular] = []
        seen_ids: set[str] = set()
        duplicate_count = 0
        skipped_non_pdf_count = 0
        for item in payload.get("data", []):
            file_ext = str(item.get("fileExt", "")).lower()
            if file_ext != "pdf":
                skipped_non_pdf_count += 1
                continue

            file_dept = str(item.get("fileDept", "")).strip().upper()
            circ_number = str(item.get("circNumber", "")).strip()
            circular_id = self.parse_circular_id(f"{file_dept}{circ_number}")
            if circular_id in seen_ids:
                duplicate_count += 1
                continue

            seen_ids.add(circular_id)
            issue_date = self._parse_issue_date(item.get("cirDate", ""))
            full_reference = str(item.get("circDisplayNo", "")).strip() or circular_id

            circulars.append(
                Circular(
                    source=self.source_name,
                    circular_id=circular_id,
                    full_reference=full_reference,
                    department=file_dept,
                    title=str(item.get("sub", "")).strip(),
                    issue_date=issue_date,
                    effective_date=None,
                    url=self._build_listing_url(issue_date, issue_date),
                    pdf_url=str(item.get("circFilelink", "")).strip()
                    or self.get_pdf_download_url(circular_id),
                )
            )

        self.logger.info(
            "NSE payload processed total_items=%s pdf_circulars=%s skipped_non_pdf=%s duplicates_skipped=%s",
            len(payload.get("data", [])),
            len(circulars),
            skipped_non_pdf_count,
            duplicate_count,
        )
        return circulars

    def _fetch_circulars(self, from_date: date, to_date: date) -> dict:
        request = Request(
            self._build_listing_url(from_date, to_date),
            headers=self.default_headers,
        )
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))

    def _build_listing_url(self, from_date: date, to_date: date) -> str:
        query = urlencode(
            {
                "fromDate": from_date.strftime("%d-%m-%Y"),
                "toDate": to_date.strftime("%d-%m-%Y"),
            }
        )
        return f"{self.API_URL}?&{query}"

    def _parse_issue_date(self, raw_value: str) -> date:
        return datetime.strptime(str(raw_value), "%Y%m%d").date()
