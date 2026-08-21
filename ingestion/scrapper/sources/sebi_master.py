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
class SEBIMasterCircularScraper(IScraper):
    source_name = "SEBI_MASTER"
    ENDPOINT = "/api/enforcement_crawler/sebi/master-circular"
    # Signal to the orchestrator: after downloading the PDF, extract the real
    # circular reference from page 1 and overwrite the API short ID.
    enrich_reference_from_pdf = True
    SUPPORTED_EXTENSIONS = (".pdf", ".zip")
    default_headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    def __init__(self) -> None:
        super().__init__()
        self.base_url = Config.ENFORCEMENT_CRAWLER_BASE_URL
        self.max_retries = max(1, int(os.getenv("SEBI_MASTER_MAX_RETRIES", "3")))
        self.backoff_seconds = max(
            0.0, float(os.getenv("SEBI_MASTER_RETRY_BACKOFF_SECONDS", "2.0"))
        )
        self.timeout_seconds = float(os.getenv("SEBI_MASTER_TIMEOUT_SECONDS", "30"))

    def detect_new(self, from_date: date, to_date: date) -> ScrapeDetectionResult:
        self.logger.info(
            "Fetching SEBI master-circulars via crawler API from_date=%s to_date=%s",
            from_date,
            to_date,
        )
        payload = self._fetch_circulars(from_date, to_date)
        circulars = self.parse_response(payload)
        self.logger.info(
            "Parsed SEBI master-circulars from_date=%s to_date=%s count=%s",
            from_date,
            to_date,
            len(circulars),
        )
        return ScrapeDetectionResult(circulars=circulars)

    def get_pdf_download_url(self, circular_id: str) -> str:
        return f"{self.base_url}/legal/master-circulars/{circular_id}.pdf"

    def parse_circular_id(self, raw_id: str) -> str:
        return re.sub(r"\s+", " ", raw_id.strip())

    def parse_response(self, payload: dict) -> list[Circular]:
        circulars: list[Circular] = []
        skipped_unsupported_count = 0
        skipped_missing_field_count = 0

        records = payload.get("records") or []
        for item in records:
            raw_circular_id = str(item.get("circular_id", "")).strip()
            html_url = str(item.get("html_url", "")).strip()
            download_url = str(item.get("download_url", "")).strip()

            if not raw_circular_id or not html_url or not self._is_supported_download(download_url):
                skipped_unsupported_count += 1
                if not raw_circular_id or not html_url:
                    skipped_missing_field_count += 1
                continue

            circular_id = self.parse_circular_id(raw_circular_id)
            department = str(item.get("department", "")).strip()
            title = str(item.get("title", "")).strip()
            issue_date = self._parse_issue_date(item.get("date", ""))

            # html_url is the canonical detail page and is unique per record.
            source_item_key = html_url

            circulars.append(
                Circular(
                    source=self.source_name,
                    circular_id=circular_id,
                    full_reference=circular_id,
                    department=department,
                    title=title,
                    issue_date=issue_date,
                    applicable_to_nse=False,
                    url=html_url,
                    pdf_url=download_url,
                    source_item_key=source_item_key,
                )
            )

        self.logger.info(
            "SEBI master payload processed total_records=%s supported_circulars=%s skipped_unsupported=%s skipped_missing_field=%s",
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
        return datetime.strptime(str(raw_value), "%b %d, %Y").date()