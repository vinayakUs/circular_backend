from __future__ import annotations

from datetime import date, timedelta
import hashlib
import logging
from pathlib import Path
import re
import time
from typing import Any
from urllib.request import Request, urlopen

from config import Config
from ingestion.repository import CircularRepository
from ingestion.scrapper.base import IScraper, ScrapeDetectionResult
from ingestion.scrapper.dto import Circular
from ingestion.scrapper.registry import ScraperRegistry
import ingestion.scrapper.sources.nse
import ingestion.scrapper.sources.sebi


class ScraperOrchestrator:
    """Coordinates detection, persistence, and file downloads."""

    def __init__(
        self,
        db_pool: Any = None,
        redis_client: Any = None,
        storage_path: str = "data/regulatory_raw",
        default_lookback_days: int = 7,
        circular_repository: CircularRepository | None = None,
        enabled_sources: list[str] | tuple[str, ...] | None = None,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.db_pool = db_pool
        self.redis_client = redis_client
        self.storage_path = Path(storage_path)
        self.default_lookback_days = default_lookback_days
        self.circular_repository = circular_repository or CircularRepository(db_pool)
        self.enabled_sources = tuple(
            source.upper()
            for source in (
                enabled_sources
                if enabled_sources is not None
                else Config.SCRAPER_ENABLED_SOURCES
            )
        )

    def run(self) -> None:
        today = date.today()
        enabled_scrapers = self._get_enabled_scrapers()
        self.logger.info(
            "Starting ingestion run enabled_sources=%s storage_path=%s lookback_days=%s",
            [source.source_name for source in enabled_scrapers],
            self.storage_path,
            self.default_lookback_days,
        )
        started_at = time.perf_counter()
        for source in enabled_scrapers:
            self._scrape_source(source, today)
        self.logger.info(
            "Completed ingestion run enabled_sources=%s duration_seconds=%.2f",
            [source.source_name for source in enabled_scrapers],
            time.perf_counter() - started_at,
        )

    def _scrape_source(self, source: IScraper, today: date) -> None:
        last_run_date = self.circular_repository.get_checkpoint(source.source_name)
        if last_run_date is None:
            last_run_date = today - timedelta(days=self.default_lookback_days)

        self.logger.info(
            "Starting source scrape source=%s from_date=%s to_date=%s",
            source.source_name,
            last_run_date,
            today,
        )
        started_at = time.perf_counter()
        detection_result = self._detect_new_circulars(source, last_run_date, today)
        circulars = detection_result.circulars
        self.logger.info(
            "Detected circulars source=%s count=%s failed_placeholders=%s",
            source.source_name,
            len(circulars),
            len(detection_result.failed_circulars),
        )

        fetched_count = 0
        skipped_count = 0
        failed_count = 0
        earliest_failed_issue_date: date | None = None
        for circular in circulars:
            record_id, _created = self.circular_repository.upsert_circular(circular)
            record = self.circular_repository.get_record(
                circular.source, circular.circular_id
            )
            if (
                record is not None
                and record.status == "FETCHED"
                and record.pdf_url == circular.pdf_url
            ):
                skipped_count += 1
                self.logger.info(
                    "Skipping already fetched circular source=%s circular_id=%s record_id=%s",
                    circular.source,
                    circular.circular_id,
                    record_id,
                )
                continue

            try:
                self.circular_repository.update_status(record_id, "DISCOVERED")
                file_path, content_hash = self._download_pdf(circular.pdf_url, circular)
                self.circular_repository.update_file_path(
                    record_id, file_path, content_hash
                )
                self.circular_repository.update_status(record_id, "FETCHED")
                fetched_count += 1
                self.logger.info(
                    "Fetched circular source=%s circular_id=%s record_id=%s file_path=%s",
                    circular.source,
                    circular.circular_id,
                    record_id,
                    file_path,
                )
            except Exception as exc:
                failed_count += 1
                earliest_failed_issue_date = self._min_issue_date(
                    earliest_failed_issue_date, circular.issue_date
                )
                self.circular_repository.update_status(record_id, "FAILED", str(exc))
                self.logger.exception(
                    "Failed circular fetch source=%s circular_id=%s record_id=%s",
                    circular.source,
                    circular.circular_id,
                    record_id,
                )

        for failed_circular in detection_result.failed_circulars:
            record_id, _created = self.circular_repository.upsert_circular(failed_circular)
            failed_count += 1
            earliest_failed_issue_date = self._min_issue_date(
                earliest_failed_issue_date, failed_circular.issue_date
            )
            self.circular_repository.update_status(
                record_id,
                "FAILED",
                failed_circular.error_message or "Source detail parsing failed",
            )

        self._update_checkpoint(
            source.source_name,
            last_run_date,
            today,
            detection_result.has_incomplete_items or earliest_failed_issue_date is not None,
            earliest_failed_issue_date,
        )
        self.logger.info(
            "Completed source scrape source=%s detected=%s failed_placeholders=%s fetched=%s skipped=%s failed=%s duration_seconds=%.2f",
            source.source_name,
            len(circulars),
            len(detection_result.failed_circulars),
            fetched_count,
            skipped_count,
            failed_count,
            time.perf_counter() - started_at,
        )

    def _detect_new_circulars(
        self, source: IScraper, from_date: date, to_date: date
    ) -> ScrapeDetectionResult:
        return source.detect_new(from_date, to_date)

    def _update_checkpoint(
        self,
        source_name: str,
        last_run_date: date,
        today: date,
        has_failures: bool,
        earliest_failed_issue_date: date | None,
    ) -> None:
        if not has_failures:
            self.circular_repository.set_checkpoint(source_name, today)
            return

        if earliest_failed_issue_date is None:
            self.logger.warning(
                "Checkpoint left unchanged because run had failures without issue dates source=%s last_run_date=%s",
                source_name,
                last_run_date,
            )
            return

        safe_checkpoint = earliest_failed_issue_date - timedelta(days=1)
        if safe_checkpoint > last_run_date:
            self.circular_repository.set_checkpoint(source_name, safe_checkpoint)
            self.logger.warning(
                "Advanced checkpoint conservatively after failures source=%s last_run_date=%s safe_checkpoint=%s earliest_failed_issue_date=%s",
                source_name,
                last_run_date,
                safe_checkpoint,
                earliest_failed_issue_date,
            )
            return

        self.logger.warning(
            "Checkpoint left unchanged after failures source=%s last_run_date=%s earliest_failed_issue_date=%s",
            source_name,
            last_run_date,
            earliest_failed_issue_date,
        )

    def _min_issue_date(
        self, current: date | None, candidate: date
    ) -> date:
        if current is None or candidate < current:
            return candidate
        return current

    def _get_enabled_scrapers(self) -> list[IScraper]:
        if not self.enabled_sources:
            return ScraperRegistry.list_all()

        return [ScraperRegistry.get(source_name) for source_name in self.enabled_sources]

    def _download_pdf(self, pdf_url: str, circular: Circular) -> tuple[str, str]:
        target_dir = (
            self.storage_path
            / circular.source.upper()
            / f"{circular.issue_date:%Y}"
            / f"{circular.issue_date:%m}"
        )
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"{self._build_safe_filename(circular.circular_id)}.pdf"

        self.logger.debug(
            "Downloading file source=%s circular_id=%s pdf_url=%s target_path=%s",
            circular.source,
            circular.circular_id,
            pdf_url,
            target_path,
        )
        content = self._fetch_pdf_bytes(pdf_url, circular)
        target_path.write_bytes(content)
        content_hash = hashlib.sha256(content).hexdigest()
        return str(target_path), content_hash

    def _build_safe_filename(self, value: str) -> str:
        safe_value = re.sub(r'[<>:"/\\\\|?*]+', "_", value).strip()
        safe_value = re.sub(r"\s+", "_", safe_value)
        safe_value = re.sub(r"_+", "_", safe_value).strip("._")
        return safe_value or "document"

    def _fetch_pdf_bytes(self, pdf_url: str, circular: Circular) -> bytes:
        if pdf_url:
            request = Request(
                pdf_url,
                headers={"User-Agent": "Mozilla/5.0", "Referer": circular.url or pdf_url},
            )
            with urlopen(request, timeout=30) as response:
                return response.read()

        placeholder = (
            f"PDF download pending for {circular.circular_id}\n"
            f"Source URL: {circular.url}\n"
        )
        return placeholder.encode("utf-8")
