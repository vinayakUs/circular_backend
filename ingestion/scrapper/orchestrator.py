from __future__ import annotations

import dataclasses
from datetime import date, timedelta
import hashlib
import importlib
import logging
from pathlib import Path
import re
import time
from typing import Any
from urllib.parse import urlparse
import zipfile

import requests

from config import Config
from ingestion.logging_utils import emit_block, format_bytes, truncate
from ingestion.repository import CircularAsset, CircularRepository
from ingestion.repository.asset_repository import AssetRepository
from ingestion.repository.checkpoint_repository import CheckpointRepository
from ingestion.scrapper._proxy import get_requests_proxies
from ingestion.scrapper.base import DEFAULT_USER_AGENT, IScraper, ScrapeDetectionResult
from ingestion.scrapper.dto import Circular
from ingestion.scrapper.pdf_reference_extractor import extract_master_circular_reference
from ingestion.scrapper.registry import ScraperRegistry
from ingestion.indexer.es_client import ElasticsearchClient
from ingestion.indexer.es_provider import get_es_client
from storage.s3_client import S3StorageClient
from utils.s3_utils import build_safe_filename

# Force-import the source modules so their @ScraperRegistry.register runs.
importlib.import_module("ingestion.scrapper.sources.nse")
importlib.import_module("ingestion.scrapper.sources.sebi")
importlib.import_module("ingestion.scrapper.sources.sebi_master")


class ScraperOrchestrator:
    """Coordinates detection, persistence, and file downloads."""

    def __init__(
        self,
        db_pool: Any = None,
        default_lookback_days: int = 7,
        circular_repository: CircularRepository | None = None,
        asset_repository: AssetRepository | None = None,
        checkpoint_repository: CheckpointRepository | None = None,
        enabled_sources: list[str] | tuple[str, ...] | None = None,
        from_date: date | None = None,
        to_date: date | None = None,
        s3_client: S3StorageClient | None = None,
        es_client: ElasticsearchClient | None = None,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        if circular_repository is None and db_pool is None:
            raise ValueError(
                "ScraperOrchestrator requires circular_repository or db_pool"
            )
        self.db_pool = db_pool
        self.default_lookback_days = default_lookback_days
        self.circular_repository = circular_repository or CircularRepository(db_pool)
        self.asset_repository = asset_repository or AssetRepository(db_pool)
        self.checkpoint_repository = checkpoint_repository or CheckpointRepository(db_pool)
        self.enabled_sources = tuple(
            source.upper()
            for source in (
                enabled_sources
                if enabled_sources is not None
                else Config.SCRAPER_ENABLED_SOURCES
            )
        )
        self.from_date = from_date
        self.to_date = to_date or date.today()
        if s3_client is None:
            raise RuntimeError(
                "S3 client unavailable; orchestrator cannot store assets. "
                "Configure AWS_S3_BUCKET or pass s3_client explicitly."
            )
        self.s3_client = s3_client
        self._es_client = es_client

    def run(self) -> None:
        today = date.today()
        enabled_scrapers = self._get_enabled_scrapers()
        self.logger.info(
            "run.start sources=%s from=%s to=%s",
            [source.source_name for source in enabled_scrapers],
            self.from_date or "checkpoint",
            self.to_date,
        )
        started_at = time.perf_counter()
        successful_sources: list[str] = []
        failed_sources: list[tuple[str, str]] = []
        for source in enabled_scrapers:
            source_name = source.source_name
            try:
                self._scrape_source(source, today)
                successful_sources.append(source_name)
            except Exception as exc:
                # Isolate per-source failures so one scraper being down doesn't
                # block the rest of the run. Runner exits non-zero only if
                # EVERY enabled source failed.
                failed_sources.append((source_name, str(exc)))
                self.logger.exception(
                    "Source scrape failed source=%s error=%s — continuing with remaining sources",
                    source_name,
                    exc,
                )

        if failed_sources:
            self.logger.warning(
                "run completed with source failures failed=%s ok=%s",
                failed_sources,
                successful_sources,
            )
        self.logger.info(
            "run.done ok=%s failed=%s · %.1fs",
            successful_sources,
            [name for name, _ in failed_sources],
            time.perf_counter() - started_at,
        )
        # Raise only when ALL sources failed, so the runner can still exit 0
        # on partial success and surface a hard failure when nothing worked.
        if enabled_scrapers and not successful_sources:
            raise RuntimeError(
                f"All enabled sources failed: {[name for name, _ in failed_sources]}"
            )

    def _scrape_source(self, source: IScraper, today: date) -> None:
        if self.from_date is not None:
            last_run_date = self.from_date
        else:
            last_run_date = self.checkpoint_repository.get_checkpoint(source.source_name)
            if last_run_date is None:
                last_run_date = today - timedelta(days=self.default_lookback_days)

        started_at = time.perf_counter()
        detection_result = self._detect_new_circulars(source, last_run_date, self.to_date)
        circulars = detection_result.circulars
        self.logger.info(
            "%s: processing %d circulars (%d detect failures) window=%s..%s",
            source.source_name,
            len(circulars),
            len(detection_result.failed_circulars),
            last_run_date,
            self.to_date,
        )

        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        failed_count = 0
        failure_summaries: list[str] = []
        earliest_failed_issue_date: date | None = None
        progress_width = len(str(len(circulars)))

        for index, circular in enumerate(circulars, start=1):
            record_started = time.perf_counter()
            progress = f"[{index:>{progress_width}}/{len(circulars)}]"
            base_header = f"── {progress} {circular.source} {circular.circular_id}"

            record = self.circular_repository.get_record(circular.source, circular.source_item_key)

            if record is not None and record.status == "FETCHED":
                skipped_count += 1
                emit_block(
                    self.logger,
                    logging.INFO,
                    f"{base_header} · SKIPPED · already FETCHED",
                    [
                        ("title", truncate(circular.title or "(no title)")),
                        ("db", f"existing row id={record.id} · status=FETCHED"),
                    ],
                )
                continue

            db_note = (
                f"existing row id={record.id} · was status={record.status} "
                f"(not FETCHED) → reprocessing"
                if record is not None
                else None
            )

            is_fresh = True
            stale_note: str | None = None

            if getattr(source, "supersede_on_same_title", False) and circular.title:
                existing = self.circular_repository.list_active_same_title(
                    source=circular.source, title=circular.title
                )
                if any(
                    circular.issue_date
                    and r.issue_date
                    and r.issue_date >= circular.issue_date
                    for r in existing
                ):
                    circular.is_active = False
                    is_fresh = False
                    stale_note = "stale same-title vs newer record → stored inactive"

            superseded_count = 0
            if is_fresh:
                superseded_count = self._supersede_older_same_title(source, circular)

            record_id, created, personas = self.circular_repository.upsert_circular(circular)
            if created:
                inserted_count += 1
            else:
                updated_count += 1

            phase = "fetch"
            try:
                self.circular_repository.update_status(record_id, "DISCOVERED")
                file_path, content_hash, assets = self._download_assets(
                    circular.pdf_url, circular, source
                )
                phase = "assets"
                stored_assets, previous_assets = self.asset_repository.replace_assets(
                    record_id, assets
                )

                # Optional PDF reference enrichment (e.g. SEBI_MASTER extracts the
                # real circular reference from page 1 and overwrites the API ID).
                if getattr(source, "enrich_reference_from_pdf", False):
                    phase = "enrich"
                    circular = self._enrich_from_pdf(
                        source, circular, record_id, pdf_url_for_retry=circular.pdf_url
                    )

                self.circular_repository.update_status(record_id, "FETCHED")
                duration = time.perf_counter() - record_started
                emit_block(
                    self.logger,
                    logging.INFO,
                    f"{base_header} · {'INSERTED' if created else 'UPDATED'} · {duration:.2f}s",
                    self._record_success_fields(
                        circular=circular,
                        record_id=record_id,
                        created=created,
                        db_note=db_note,
                        personas=personas,
                        assets=assets,
                        previous_assets=previous_assets,
                        superseded_count=superseded_count,
                        stale_note=stale_note,
                    ),
                )
            except Exception as exc:
                failed_count += 1
                duration = time.perf_counter() - record_started
                earliest_failed_issue_date = self._min_issue_date(
                    earliest_failed_issue_date, circular.issue_date
                )
                self.circular_repository.update_status(record_id, "FAILED", str(exc))
                self.logger.debug(
                    "Failed circular fetch source=%s circular_id=%s record_id=%s",
                    circular.source,
                    circular.circular_id,
                    record_id,
                    exc_info=True,
                )
                failure_summaries.append(f"{circular.circular_id}: {exc}")
                emit_block(
                    self.logger,
                    logging.ERROR,
                    f"{base_header} · FAILED · {duration:.2f}s",
                    [
                        ("title", truncate(circular.title or "(no title)")),
                        ("department", circular.department or ""),
                        ("issue_date", str(circular.issue_date or "")),
                        ("personas", ", ".join(personas) if personas else "none"),
                        ("db", db_note or f"new row id={record_id}"),
                        (phase, "failed"),
                        ("error", truncate(str(exc), 200)),
                        ("status", "FAILED"),
                    ],
                )

        for failed_circular in detection_result.failed_circulars:
            record_id, _created, _personas = self.circular_repository.upsert_circular(failed_circular)
            failed_count += 1
            earliest_failed_issue_date = self._min_issue_date(
                earliest_failed_issue_date, failed_circular.issue_date
            )
            error_message = failed_circular.error_message or "Source detail parsing failed"
            self.circular_repository.update_status(record_id, "FAILED", error_message)
            failure_summaries.append(f"{failed_circular.circular_id}: {error_message}")
            emit_block(
                self.logger,
                logging.ERROR,
                f"── {failed_circular.source} {failed_circular.circular_id} · FAILED (detect)",
                [
                    ("title", truncate(failed_circular.title or "(no title)")),
                    ("error", truncate(error_message, 200)),
                    ("status", "FAILED"),
                ],
            )

        effective_checkpoint = self._update_checkpoint(
            source.source_name,
            last_run_date,
            self.to_date,
            detection_result.has_incomplete_items or earliest_failed_issue_date is not None,
            earliest_failed_issue_date,
        )
        duration = time.perf_counter() - started_at
        lines = [
            f"{source.source_name} done: detected={len(circulars)} inserted={inserted_count} "
            f"updated={updated_count} skipped={skipped_count} failed={failed_count} "
            f"· {duration:.1f}s · checkpoint={effective_checkpoint}"
        ]
        if failure_summaries:
            lines.append("  failures:")
            lines.extend(f"    - {item}" for item in failure_summaries)
        self.logger.info("\n".join(lines))

    def _record_success_fields(
        self,
        circular: Circular,
        record_id: Any,
        created: bool,
        db_note: str | None,
        personas: list[str],
        assets: list[CircularAsset],
        previous_assets: int,
        superseded_count: int,
        stale_note: str | None,
    ) -> list[tuple[str, str]]:
        if created:
            db_desc = f"new row id={record_id}"
        else:
            db_desc = db_note or f"existing row id={record_id}"

        original = next(
            (a for a in assets if a.asset_role in ("original_pdf", "original_zip")),
            assets[0] if assets else None,
        )
        extracted_count = sum(1 for a in assets if a.asset_role == "extracted_pdf")
        if original is not None and original.asset_role == "original_zip":
            fetch_desc = f"zip + {extracted_count} pdfs"
        else:
            fetch_desc = "pdf"
        size = format_bytes(original.file_size_bytes if original else None)

        object_word = "object" if len(assets) == 1 else "objects"
        if previous_assets:
            prev_word = "asset" if previous_assets == 1 else "assets"
            s3_desc = (
                f"uploaded {len(assets)} {object_word} · "
                f"replaced {previous_assets} previous {prev_word}"
            )
        else:
            s3_desc = f"uploaded {len(assets)} {object_word} · new"

        fields: list[tuple[str, str]] = [
            ("title", truncate(circular.title or "(no title)")),
            ("department", circular.department or ""),
            ("issue_date", str(circular.issue_date or "")),
            ("personas", ", ".join(personas) if personas else "none"),
            ("db", db_desc),
            ("fetch", f"ok · {fetch_desc} · {size}"),
            ("s3", s3_desc),
        ]
        if superseded_count:
            fields.append(("superseded", f"{superseded_count} older same-title record(s)"))
        if stale_note:
            fields.append(("note", stale_note))
        fields.append(("status", "FETCHED"))
        return fields

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
    ) -> date:
        """Advance (or hold) the source checkpoint; returns the effective date."""
        if not has_failures:
            self.checkpoint_repository.set_checkpoint(source_name, today)
            return today

        if earliest_failed_issue_date is None:
            self.logger.warning(
                "Checkpoint left unchanged because run had failures without issue dates source=%s last_run_date=%s",
                source_name,
                last_run_date,
            )
            return last_run_date

        safe_checkpoint = earliest_failed_issue_date - timedelta(days=1)
        if safe_checkpoint > last_run_date:
            self.checkpoint_repository.set_checkpoint(source_name, safe_checkpoint)
            self.logger.warning(
                "Advanced checkpoint conservatively after failures source=%s last_run_date=%s safe_checkpoint=%s earliest_failed_issue_date=%s",
                source_name,
                last_run_date,
                safe_checkpoint,
                earliest_failed_issue_date,
            )
            return safe_checkpoint

        self.logger.warning(
            "Checkpoint left unchanged after failures source=%s last_run_date=%s earliest_failed_issue_date=%s",
            source_name,
            last_run_date,
            earliest_failed_issue_date,
        )
        return last_run_date

    def _min_issue_date(
        self, current: date | None, candidate: date
    ) -> date:
        if current is None or candidate < current:
            return candidate
        return current



    def _supersede_older_same_title(
        self,
        source: IScraper,
        circular: Circular,
    ) -> int:
        """If the source opts into same-title supersession, find active same-title
        records older than `circular.issue_date` and flip them to is_active=FALSE.
        Returns count flipped.

        No-op when the scraper doesn't opt in, when title is empty, when no
        older same-title active records exist, or when the candidate has no
        issue_date.
        """
        if not getattr(source, "supersede_on_same_title", False):
            return 0

        if not circular.title or not circular.issue_date:
            return 0

        existing = self.circular_repository.list_active_same_title(
            source=circular.source,
            title=circular.title,
        )

        older_ids = [
            r.id
            for r in existing
            if r.issue_date and r.issue_date < circular.issue_date
        ]

        if not older_ids:
            return 0

        # Delete ES chunks BEFORE flipping is_active in DB. If ES delete fails,
        # the exception propagates → the caller marks this ingest FAILED and
        # the next run retries, leaving the DB and ES in their current state.
        # Doing ES first also keeps the indexer filter (is_active=TRUE) from
        # re-indexing a record whose chunks we couldn't clean up.
        if self._es_client is None:
            raise RuntimeError(
                "ES client unavailable; cannot complete supersession cleanup. "
                "Next orchestrator run will retry."
            )
        for record_id in older_ids:
            self._es_client.delete_documents_for_record(str(record_id))

        count = self.circular_repository.mark_inactive(older_ids)

        self.logger.debug(
            "Superseded older same-title circulars source=%s new_circular_id=%s "
            "new_issue_date=%s superseded_count=%s",
            source.source_name,
            circular.circular_id,
            circular.issue_date,
            count,
        )

        return count


    def _enrich_from_pdf(
        self,
        source: IScraper,
        circular: Circular,
        record_id: Any,
        pdf_url_for_retry: str,
    ) -> Circular:
        """Download the PDF once more, extract the real circular reference
        from page 1, and overwrite circular_id/full_reference. Raises on
        failure so the caller marks the record FAILED."""
        try:
            pdf_bytes = self._fetch_pdf_bytes(pdf_url_for_retry, circular)
        except Exception as exc:
            raise RuntimeError(
                f"PDF re-fetch for reference extraction failed: {exc}"
            ) from exc

        extracted = extract_master_circular_reference(pdf_bytes)
        if not extracted:
            raise ValueError(
                f"Could not extract master circular reference from page 1 "
                f"of {circular.pdf_url}"
            )

        if extracted == circular.circular_id:
            self.logger.debug(
                "Extracted reference matches existing circular_id source=%s record_id=%s circular_id=%s",
                source.source_name,
                record_id,
                extracted,
            )
            return circular

        enriched = dataclasses.replace(
            circular,
            circular_id=extracted,
            full_reference=extracted,
        )
        self.circular_repository.upsert_circular(enriched)
        self.logger.info(
            "Enriched circular with extracted reference source=%s record_id=%s from=%s to=%s",
            source.source_name,
            record_id,
            circular.circular_id,
            extracted,
        )
        return enriched

    def _get_enabled_scrapers(self) -> list[IScraper]:
        if not self.enabled_sources:
            return ScraperRegistry.list_all()

        return [ScraperRegistry.get(source_name) for source_name in self.enabled_sources]

    def _download_assets(
        self, pdf_url: str, circular: Circular, source: IScraper
    ) -> tuple[str, str, list[CircularAsset]]:
        prefix = (
            f"{circular.source.upper()}/"
            f"{circular.issue_date:%Y}/{circular.issue_date:%m}/"
            f"{build_safe_filename(circular.circular_id)}_"
            f"{hashlib.md5(circular.source_item_key.encode()).hexdigest()[:12]}"
        )

        self.s3_client.delete_prefix(prefix)

        self.logger.debug(
            "Downloading file source=%s circular_id=%s pdf_url=%s prefix=%s",
            circular.source,
            circular.circular_id,
            pdf_url,
            prefix,
        )
        content = self._fetch_pdf_bytes(pdf_url, circular)
        content_hash = hashlib.sha256(content).hexdigest()
        download_type = self._detect_download_type(pdf_url, content)

        if download_type == "zip":
            original_key = f"{prefix}/original/source.zip"
            self.s3_client.upload_bytes(original_key, content)
            extracted_assets = self._extract_zip_assets(
                archive_content=content,
                prefix=prefix,
                content_hash=content_hash,
                circular=circular,
            )
            assets = [
                CircularAsset(
                    asset_role="original_zip",
                    file_path=f"s3://{Config.AWS_S3_BUCKET}/{original_key}",
                    content_hash=content_hash,
                    mime_type="application/zip",
                    file_size_bytes=len(content),
                ),
                *extracted_assets,
            ]
            return f"s3://{Config.AWS_S3_BUCKET}/{original_key}", content_hash, assets


        if download_type== "pdf" and getattr(source, "merge_chapter_pdfs" ,False):
            from ingestion.scrapper.sebi_master_merge import merge_sebi_master_chapters
            content = merge_sebi_master_chapters(
                main_pdf_bytes=content,
                main_pdf_url=pdf_url,
                chapter_fetcher=lambda url: self._fetch_pdf_bytes(url, circular),
            )
            content_hash = hashlib.sha256(content).hexdigest()


        original_key = f"{prefix}/original/source.pdf"
        self.s3_client.upload_bytes(original_key, content)
        return (
            f"s3://{Config.AWS_S3_BUCKET}/{original_key}",
            content_hash,
            [
                CircularAsset(
                    asset_role="original_pdf",
                    file_path=f"s3://{Config.AWS_S3_BUCKET}/{original_key}",
                    content_hash=content_hash,
                    mime_type="application/pdf",
                    file_size_bytes=len(content),
                )
            ],
        )


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
        archive_content: bytes,
        prefix: str,
        content_hash: str,
        circular: Circular,
    ) -> list[CircularAsset]:
        assets: list[CircularAsset] = []
        import io

        with zipfile.ZipFile(io.BytesIO(archive_content)) as archive:
            pdf_members = [
                member
                for member in archive.infolist()
                if not member.is_dir() and member.filename.lower().endswith(".pdf")
            ]
            if not pdf_members:
                raise ValueError(f"ZIP archive contains no PDFs")

            selected_members = self._select_zip_pdf_members(pdf_members, circular)

            for index, member in enumerate(selected_members):
                member_name = Path(member.filename).name or f"document_{index + 1}.pdf"
                safe_name = build_safe_filename(Path(member_name).stem) + ".pdf"
                target_key = f"{prefix}/extracted/{index:03d}_{safe_name}"

                with archive.open(member) as source:
                    member_content = source.read()
                self.s3_client.upload_bytes(target_key, member_content)

                assets.append(
                    CircularAsset(
                        asset_role="extracted_pdf",
                        file_path=f"s3://{Config.AWS_S3_BUCKET}/{target_key}",
                        content_hash=content_hash,
                        mime_type="application/pdf",
                        archive_member_path=member.filename,
                        file_size_bytes=len(member_content),
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
            self.logger.debug(
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
        if not pdf_url:
            placeholder = (
                f"PDF download pending for {circular.circular_id}\n"
                f"Source URL: {circular.url}\n"
            )
            return placeholder.encode("utf-8")

        # Retry on mid-stream disconnects (ChunkedEncodingError / IncompleteRead
        # are common with SEBI's CDN connection drops part-way through a
        # large PDF). Same backoff pattern as IScraper._fetch_with_retry.
        max_retries = 3
        backoff_seconds = 1.0
        last_error: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(
                    pdf_url,
                    headers={"User-Agent": DEFAULT_USER_AGENT, "Referer": circular.url or pdf_url},
                    timeout=30,
                    allow_redirects=True,
                    proxies=get_requests_proxies(pdf_url),
                    verify=False,
                )
                response.raise_for_status()
                content = response.content

                # Validate it's actually PDF or ZIP, not an HTML error page
                if not content.startswith(b"%PDF") and not content.startswith(b"PK\x03\x04"):
                    raise ValueError(
                        f"NSE returned invalid content for {pdf_url}: "
                        f"Content-Type={response.headers.get('Content-Type')!r}, "
                        f"first 50 bytes={content[:50]!r}"
                    )
                return content
            except (
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as exc:
                last_error = exc
                if attempt >= max_retries:
                    break
                sleep_seconds = backoff_seconds * attempt
                self.logger.warning(
                    "PDF download interrupted, retrying attempt=%d/%d backoff=%.2fs url=%s error=%s",
                    attempt, max_retries, sleep_seconds, pdf_url, exc,
                )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
        raise RuntimeError(
            f"PDF download failed after {max_retries} attempts: {last_error}"
        )
