from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date

from ingestion.scrapper.dto import Circular


@dataclass(slots=True)
class ScrapeDetectionResult:
    circulars: list[Circular] = field(default_factory=list)
    failed_circulars: list[Circular] = field(default_factory=list)
    has_incomplete_items: bool = False


class IScraper(ABC):
    """Contract implemented by all source-specific scrapers."""

    source_name: str

    @abstractmethod
    def detect_new(self, from_date: date, to_date: date) -> ScrapeDetectionResult:
        """Return newly discovered circulars for the date range."""

    @abstractmethod
    def get_pdf_download_url(self, circular_id: str) -> str:
        """Return the download URL for the circular PDF."""

    @abstractmethod
    def parse_circular_id(self, raw_id: str) -> str:
        """Normalize a source-specific identifier."""
