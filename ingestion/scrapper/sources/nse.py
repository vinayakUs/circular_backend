from __future__ import annotations

from datetime import date, datetime
import json
import os
from pathlib import Path
import re
from urllib.parse import urlparse

from config import Config
from ingestion.scrapper.base import DEFAULT_USER_AGENT, IScraper, ScrapeDetectionResult
from ingestion.scrapper.dto import Circular
from ingestion.scrapper.registry import ScraperRegistry


@ScraperRegistry.register
class NSEScraper(IScraper):
    source_name = "NSE"
    ENDPOINT = "/api/enforcement_crawler/nse/circulars"
    SUPPORTED_EXTENSIONS = (".pdf", ".zip")
    default_headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    def __init__(self) -> None:
        super().__init__()
        self.base_url = Config.ENFORCEMENT_CRAWLER_BASE_URL
        if Config.ENFORCEMENT_CRAWLER_AUTH_TOKEN:
            self.default_headers["Authorization"] = f"Bearer {Config.ENFORCEMENT_CRAWLER_AUTH_TOKEN}"
        self.max_retries = max(1, int(os.getenv("NSE_MAX_RETRIES", "3")))
        self.backoff_seconds = max(0.0, float(os.getenv("NSE_RETRY_BACKOFF_SECONDS", "2.0")))
        self.timeout_seconds = float(os.getenv("NSE_TIMEOUT_SECONDS", "30"))

    def detect_new(self, from_date: date, to_date: date) -> ScrapeDetectionResult:
        self.logger.info(
            "Fetching NSE circulars via crawler API from_date=%s to_date=%s",
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
        return ScrapeDetectionResult(circulars=circulars)

    def get_pdf_download_url(self, circular_id: str) -> str:
        normalized_id = self.parse_circular_id(circular_id)
        return f"https://nsearchives.nseindia.com/content/circulars/{normalized_id}.pdf"

    def parse_circular_id(self, raw_id: str) -> str:
        return raw_id.replace("/", "").strip().upper()

    def parse_response(self, payload: dict) -> list[Circular]:
        circulars: list[Circular] = []
        skipped_unsupported_count = 0
        skipped_missing_field_count = 0

        records = payload.get("records") or []
        for item in records:
            download_url = str(item.get("download_url", "")).strip()
            if not self._is_supported_download(download_url):
                skipped_unsupported_count += 1
                continue

            raw_circular_id = str(item.get("circular_id", "")).strip()
            if not raw_circular_id:
                skipped_missing_field_count += 1
                continue

            circular_id = self.parse_circular_id(raw_circular_id)
            department = str(item.get("department", "")).strip()
            title = str(item.get("title", "")).strip()
            issue_date = self._parse_issue_date(item.get("issue_date", ""))

            # Include department in the key so the same circular_id from a
            # different department is recorded as a distinct row. Department
            # labels contain spaces (e.g. "Securities Lending & Borrowing Scheme")
            # which are normalized to underscores so the key is DB-safe.
            normalized_department = self._normalize_department(department)
            source_item_key = (
                f"{circular_id}:{normalized_department}"
                if normalized_department
                else circular_id
            )

            circulars.append(
                Circular(
                    source=self.source_name,
                    circular_id=circular_id,
                    full_reference=circular_id,
                    department=department,
                    title=title,
                    issue_date=issue_date,
                    applicable_to_nse=True,
                    url=download_url,
                    pdf_url=download_url,
                    source_item_key=source_item_key,
                )
            )

        self.logger.info(
            "NSE payload processed total_records=%s supported_circulars=%s skipped_unsupported=%s skipped_missing_circular_id=%s",
            len(records),
            len(circulars),
            skipped_unsupported_count,
            skipped_missing_field_count,
        )
        return circulars

    def _fetch_circulars(self, from_date: date, to_date: date) -> dict:
        url = f"{self.base_url}{self.ENDPOINT}"
        body = json.dumps(
            {
                "from_date": from_date.strftime("%d-%m-%Y"),
                "to_date": to_date.strftime("%d-%m-%Y"),
            }
        )
        text = self._fetch_with_retry(
            method="POST",
            url=url,
            max_retries=self.max_retries,
            backoff_seconds=self.backoff_seconds,
            timeout=self.timeout_seconds,
            extra_log={"from_date": str(from_date), "to_date": str(to_date)},
            data=body,
            headers=self.default_headers,
        )
        return json.loads(text)

    def _is_supported_download(self, download_url: str) -> bool:
        suffix = Path(urlparse(download_url).path).suffix.lower()
        return suffix in self.SUPPORTED_EXTENSIONS

    def _parse_issue_date(self, raw_value: str) -> date:
        return datetime.strptime(str(raw_value), "%Y-%m-%d").date()

    @staticmethod
    def _normalize_department(department: str) -> str:
        if not department:
            return ""
        # Strip whitespace, replace whitespace runs with underscores, drop trailing punctuation.
        normalized = department.strip()
        normalized = re.sub(r"\s+", "_", normalized)
        normalized = normalized.strip("_")
        return normalized