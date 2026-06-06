from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
import logging
import time

import requests

from ingestion.scrapper._proxy import get_requests_proxies
from ingestion.scrapper.dto import Circular

DEFAULT_USER_AGENT = "Mozilla/5.0"


@dataclass(slots=True)
class ScrapeDetectionResult:
    circulars: list[Circular] = field(default_factory=list)
    failed_circulars: list[Circular] = field(default_factory=list)
    has_incomplete_items: bool = False


class IScraper(ABC):
    """Contract implemented by all source-specific scrapers."""

    source_name: str

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    def _fetch_with_retry(
        self,
        method: str,
        url: str,
        *,
        max_retries: int,
        backoff_seconds: float,
        timeout: float,
        extra_log: dict | None = None,
        **kwargs,
    ) -> str:
        last_error: Exception | None = None
        for attempt in range(1, max_retries + 1):
            log_msg = f"Fetching {method} {url} attempt={attempt}/{max_retries} timeout={timeout:.1f}s"
            if extra_log:
                log_msg += f" {extra_log}"
            self.logger.info(log_msg)
            try:
                response = requests.request(
                    method, url, timeout=timeout,
                    allow_redirects=True,
                    proxies=get_requests_proxies(url),
                    verify=False,
                    **kwargs,
                )
                return response.text
            except Exception as exc:
                last_error = exc
                if attempt >= max_retries:
                    break
                sleep_seconds = backoff_seconds * attempt
                self.logger.warning(
                    "Retrying after error attempt=%s/%s backoff=%.2fs error=%s",
                    attempt, max_retries, sleep_seconds, exc,
                )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
        raise last_error

    @abstractmethod
    def detect_new(self, from_date: date, to_date: date) -> ScrapeDetectionResult:
        """Return newly discovered circulars for the date range."""

    @abstractmethod
    def get_pdf_download_url(self, circular_id: str) -> str:
        """Return the download URL for the circular PDF."""

    @abstractmethod
    def parse_circular_id(self, raw_id: str) -> str:
        """Normalize a source-specific identifier."""
