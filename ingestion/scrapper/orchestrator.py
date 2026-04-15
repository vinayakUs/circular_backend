from __future__ import annotations

from datetime import date, timedelta
import hashlib
import logging
from pathlib import Path
import re
import shutil
import time
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import zipfile

from config import Config
from ingestion.repository import CircularAsset, CircularRepository
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
        if circular_repository is None and db_pool is None:
            raise ValueError(
                "ScraperOrchestrator requires circular_repository or db_pool"
            )
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
                file_path, content_hash, assets = self._download_assets(
                    circular.pdf_url, circular
                )
                self.circular_repository.replace_assets(record_id, assets)
                self.circular_repository.update_file_path(record_id, file_path, content_hash)
                self.circular_repository.update_status(record_id, "FETCHED")
                fetched_count += 1
                self.logger.info(
                    "Fetched circular source=%s circular_id=%s record_id=%s file_path=%s asset_count=%s",
                    circular.source,
                    circular.circular_id,
                    record_id,
                    file_path,
                    len(assets),
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

    def _download_assets(
        self, pdf_url: str, circular: Circular
    ) -> tuple[str, str, list[CircularAsset]]:
        circular_dir = (
            self.storage_path
            / circular.source.upper()
            / f"{circular.issue_date:%Y}"
            / f"{circular.issue_date:%m}"
            / self._build_safe_filename(circular.circular_id)
        )
        if circular_dir.exists():
            shutil.rmtree(circular_dir)

        original_dir = circular_dir / "original"
        extracted_dir = circular_dir / "extracted"
        original_dir.mkdir(parents=True, exist_ok=True)

        self.logger.debug(
            "Downloading file source=%s circular_id=%s pdf_url=%s target_dir=%s",
            circular.source,
            circular.circular_id,
            pdf_url,
            circular_dir,
        )
        content = self._fetch_pdf_bytes(pdf_url, circular)
        content_hash = hashlib.sha256(content).hexdigest()
        download_type = self._detect_download_type(pdf_url, content)

        if download_type == "zip":
            original_path = original_dir / "source.zip"
            original_path.write_bytes(content)
            extracted_assets = self._extract_zip_assets(
                archive_path=original_path,
                extracted_dir=extracted_dir,
                content_hash=content_hash,
                circular=circular
            )
            assets = [
                CircularAsset(
                    asset_role="original_zip",
                    file_path=str(original_path),
                    content_hash=content_hash,
                    mime_type="application/zip",
                    file_size_bytes=original_path.stat().st_size,
                ),
                *extracted_assets,
            ]
            return str(original_path), content_hash, assets

        original_path = original_dir / "source.pdf"
        original_path.write_bytes(content)
        return (
            str(original_path),
            content_hash,
            [
                CircularAsset(
                    asset_role="original_pdf",
                    file_path=str(original_path),
                    content_hash=content_hash,
                    mime_type="application/pdf",
                    file_size_bytes=original_path.stat().st_size,
                )
            ],
        )

    def _build_safe_filename(self, value: str) -> str:
        safe_value = re.sub(r'[<>:"/\\\\|?*]+', "_", value).strip()
        safe_value = re.sub(r"\s+", "_", safe_value)
        safe_value = re.sub(r"_+", "_", safe_value).strip("._")
        return safe_value or "document"

    def _detect_download_type(self, pdf_url: str, content: bytes) -> str:
        if content.startswith(b"PK\x03\x04"):
            return "zip"
        if content.startswith(b"%PDF"):
            return "pdf"

        suffix = Path(urlparse(pdf_url).path).suffix.lower()
        if suffix == ".zip":
            return "zip"
        return "pdf"

    def _extract_zip_assets(
        self,
        archive_path: Path,
        extracted_dir: Path,
        content_hash: str,
        circular: Circular,
    ) -> list[CircularAsset]:
        extracted_dir.mkdir(parents=True, exist_ok=True)
        assets: list[CircularAsset] = []

        with zipfile.ZipFile(archive_path) as archive:
            pdf_members = [
                member
                for member in archive.infolist()
                if not member.is_dir() and member.filename.lower().endswith(".pdf")
            ]
            if not pdf_members:
                raise ValueError(f"ZIP archive contains no PDFs: {archive_path}")

            selected_members = self._select_zip_pdf_members(pdf_members, circular)

            for index, member in enumerate(selected_members):
                member_name = Path(member.filename).name or f"document_{index + 1}.pdf"
                safe_name = self._build_safe_filename(Path(member_name).stem) + ".pdf"
                target_path = extracted_dir / f"{index:03d}_{safe_name}"

                with archive.open(member) as source, target_path.open("wb") as target:
                    shutil.copyfileobj(source, target)

                assets.append(
                    CircularAsset(
                        asset_role="extracted_pdf",
                        file_path=str(target_path),
                        content_hash=content_hash,
                        mime_type="application/pdf",
                        archive_member_path=member.filename,
                        file_size_bytes=target_path.stat().st_size,
                    )
                )

        return assets


    def _select_zip_pdf_members(
        self,
        pdf_members: list[zipfile.ZipInfo],
        circular: Circular,
    ) -> list[zipfile.ZipInfo]:
        if circular.source.upper() != "NSE":
            return pdf_members

        normalized_circular_id = self._normalize_zip_match_value(circular.circular_id)
        matching_members = [
            member
            for member in pdf_members
            if normalized_circular_id
            and normalized_circular_id
            in self._normalize_zip_match_value(Path(member.filename).stem)
        ]

        if matching_members:
            self.logger.info(
                "Selected NSE ZIP members by circular_id source=%s circular_id=%s matched=%s total_pdf_members=%s",
                circular.source,
                circular.circular_id,
                len(matching_members),
                len(pdf_members),
            )
            return matching_members

        self.logger.warning(
            "No NSE ZIP member matched circular_id; falling back to all PDFs source=%s circular_id=%s total_pdf_members=%s",
            circular.source,
            circular.circular_id,
            len(pdf_members),
        )
        return pdf_members


    def _normalize_zip_match_value(self, value: str) -> str:
        return re.sub(r"[^A-Z0-9]+", "", value.upper())

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
