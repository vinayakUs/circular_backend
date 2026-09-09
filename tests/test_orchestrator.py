from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
import zipfile

from ingestion.scrapper.base import IScraper, ScrapeDetectionResult
from ingestion.scrapper.dto import Circular
from ingestion.scrapper.orchestrator import ScraperOrchestrator
from tests.fakes import FakeCircularRepository


class StubScraper(IScraper):
    source_name = "TEST"
    # Default off; tests opt in per-instance by passing supersede_on_same_title=True.
    supersede_on_same_title = False

    def __init__(
        self,
        circulars: list[Circular],
        failed_circulars: list[Circular] | None = None,
        has_incomplete_items: bool = False,
        supersede_on_same_title: bool = False,
    ) -> None:
        self.circulars = circulars
        self.failed_circulars = failed_circulars or []
        self.has_incomplete_items = has_incomplete_items
        self.detect_calls: list[tuple[date, date]] = []
        # Per-instance opt-in (class default is False; tests flip it).
        self.supersede_on_same_title = supersede_on_same_title

    def detect_new(self, from_date: date, to_date: date) -> ScrapeDetectionResult:
        self.detect_calls.append((from_date, to_date))
        return ScrapeDetectionResult(
            circulars=list(self.circulars),
            failed_circulars=list(self.failed_circulars),
            has_incomplete_items=self.has_incomplete_items,
        )

    def get_pdf_download_url(self, circular_id: str) -> str:
        return f"https://example.com/{circular_id}.pdf"

    def parse_circular_id(self, raw_id: str) -> str:
        return raw_id


class _NoopESClient:
    """No-op ES client for tests that don't exercise ES deletion.

    Provides just enough surface for `delete_documents_for_record` so the
    orchestrator's supersession path completes without raising.
    """

    def delete_documents_for_record(self, circular_db_id: str) -> None:
        return None


class _NoopS3Client:
    """No-op S3 client for tests that don't exercise S3 uploads."""

    def delete_prefix(self, prefix: str) -> None:
        return None

    def upload_bytes(self, key: str, content: bytes) -> None:
        return None


class OrchestratorTestCase(unittest.TestCase):
    def build_circular(self) -> Circular:
        return Circular(
            source="TEST",
            circular_id="ABC123",
            full_reference="TEST/ABC123",
            department="OPS",
            title="Test Circular",
            issue_date=date(2026, 4, 6),
            url="https://example.com/circular",
            pdf_url="https://example.com/circular.pdf",
            source_item_key="TEST::ABC123",
        )

    def test_orchestrator_uses_repository_checkpoint(self) -> None:
        repository = FakeCircularRepository()
        repository.set_checkpoint("TEST", date(2026, 4, 4))
        scraper = StubScraper([self.build_circular()])

        with TemporaryDirectory() as temp_dir:
            orchestrator = ScraperOrchestrator(
                storage_path=temp_dir,
                circular_repository=repository,
            )
            orchestrator._fetch_pdf_bytes = lambda pdf_url, circular: b"pdf-bytes"  # type: ignore[method-assign]

            orchestrator._scrape_source(scraper, date(2026, 4, 6))

        self.assertEqual(scraper.detect_calls, [(date(2026, 4, 4), date(2026, 4, 6))])
        self.assertEqual(repository.get_checkpoint("TEST"), date(2026, 4, 6))

    def test_orchestrator_skips_duplicate_fetched_circulars(self) -> None:
        repository = FakeCircularRepository()
        circular = self.build_circular()
        record_id, _created = repository.upsert_circular(circular)
        repository.update_file_path(record_id, "existing.pdf", "hash")
        repository.replace_assets(
            record_id,
            [],
        )
        repository.update_status(record_id, "FETCHED")

        scraper = StubScraper([circular])

        with TemporaryDirectory() as temp_dir:
            orchestrator = ScraperOrchestrator(
                storage_path=temp_dir,
                circular_repository=repository,
            )

            def fail_if_called(pdf_url: str, current: Circular) -> bytes:
                raise AssertionError("download should have been skipped")

            orchestrator._fetch_pdf_bytes = fail_if_called  # type: ignore[method-assign]
            orchestrator._scrape_source(scraper, date(2026, 4, 6))

        record = repository.get_record("TEST", "ABC123")
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "FETCHED")
        self.assertEqual(record.file_path, "existing.pdf")

    def test_orchestrator_saves_file_and_status_for_new_circular(self) -> None:
        repository = FakeCircularRepository()
        scraper = StubScraper([self.build_circular()])

        with TemporaryDirectory() as temp_dir:
            orchestrator = ScraperOrchestrator(
                storage_path=temp_dir,
                circular_repository=repository,
            )
            orchestrator._fetch_pdf_bytes = lambda pdf_url, circular: b"pdf-bytes"  # type: ignore[method-assign]

            orchestrator._scrape_source(scraper, date(2026, 4, 6))

            record = repository.get_record("TEST", "ABC123")
            self.assertIsNotNone(record)
            self.assertEqual(record.status, "FETCHED")
            self.assertTrue(record.file_path)
            self.assertTrue(Path(record.file_path).exists())
            self.assertEqual(len(record.content_hash), 64)
            assets = repository.list_assets(record.id)
            self.assertEqual(len(assets), 1)
            self.assertEqual(assets[0].asset_role, "original_pdf")

    def test_orchestrator_sanitizes_filename_but_not_record_id(self) -> None:
        repository = FakeCircularRepository()
        circular = Circular(
            source="SEBI",
            circular_id="HO/(68)2026-IMD-POD-2/I/5780/2026",
            full_reference="HO/(68)2026-IMD-POD-2/I/5780/2026",
            department="",
            title="SEBI Circular",
            issue_date=date(2026, 4, 6),
            url="https://example.com/detail",
            pdf_url="https://example.com/file.pdf",
        )
        scraper = StubScraper([circular])

        with TemporaryDirectory() as temp_dir:
            orchestrator = ScraperOrchestrator(
                storage_path=temp_dir,
                circular_repository=repository,
            )
            orchestrator._fetch_pdf_bytes = lambda pdf_url, current: b"pdf-bytes"  # type: ignore[method-assign]

            orchestrator._scrape_source(scraper, date(2026, 4, 6))

            record = repository.get_record("SEBI", "HO/(68)2026-IMD-POD-2/I/5780/2026")
            self.assertIsNotNone(record)
            assert record is not None
            self.assertIn("HO_(68)2026-IMD-POD-2_I_5780_2026/original/source.pdf", record.file_path)

    def test_orchestrator_persists_failed_placeholders_and_advances_checkpoint_safely(
        self,
    ) -> None:
        repository = FakeCircularRepository()
        repository.set_checkpoint("SEBI", date(2026, 4, 10))
        successful = Circular(
            source="SEBI",
            circular_id="HO/RECOVERED/2026",
            full_reference="HO/RECOVERED/2026",
            department="",
            title="Recovered circular",
            issue_date=date(2026, 4, 13),
            url="https://www.sebi.gov.in/legal/circulars/apr-2026/recovered.html",
            pdf_url="https://www.sebi.gov.in/sebi_data/attachdocs/apr-2026/recovered.pdf",
            source_item_key="https://www.sebi.gov.in/legal/circulars/apr-2026/recovered.html",
        )
        failed = Circular(
            source="SEBI",
            circular_id="SEBI_PENDING::MISSED.HTML",
            full_reference="SEBI_PENDING::MISSED.HTML",
            department="",
            title="Missed circular",
            issue_date=date(2026, 4, 12),
            url="https://www.sebi.gov.in/legal/circulars/apr-2026/missed.html",
            pdf_url="",
            source_item_key="https://www.sebi.gov.in/legal/circulars/apr-2026/missed.html",
            error_message="SEBI detail fetch failed: timed out",
        )
        scraper = StubScraper(
            [successful], failed_circulars=[failed], has_incomplete_items=True
        )
        scraper.source_name = "SEBI"

        with TemporaryDirectory() as temp_dir:
            orchestrator = ScraperOrchestrator(
                storage_path=temp_dir,
                circular_repository=repository,
            )
            orchestrator._fetch_pdf_bytes = lambda pdf_url, circular: b"pdf-bytes"  # type: ignore[method-assign]

            orchestrator._scrape_source(scraper, date(2026, 4, 14))

        failed_record = repository.get_record("SEBI", "SEBI_PENDING::MISSED.HTML")
        self.assertIsNotNone(failed_record)
        assert failed_record is not None
        self.assertEqual(failed_record.status, "FAILED")
        self.assertEqual(failed_record.error_message, "SEBI detail fetch failed: timed out")

        successful_record = repository.get_record("SEBI", "HO/RECOVERED/2026")
        self.assertIsNotNone(successful_record)
        assert successful_record is not None
        self.assertEqual(successful_record.status, "FETCHED")
        self.assertEqual(repository.get_checkpoint("SEBI"), date(2026, 4, 11))

    def test_orchestrator_extracts_zip_into_multiple_assets(self) -> None:
        repository = FakeCircularRepository()
        circular = self.build_circular()
        circular.pdf_url = "https://example.com/archive.zip"
        scraper = StubScraper([circular])

        with TemporaryDirectory() as temp_dir:
            orchestrator = ScraperOrchestrator(
                storage_path=temp_dir,
                circular_repository=repository,
            )

            def build_zip(_pdf_url: str, _circular: Circular) -> bytes:
                zip_path = Path(temp_dir) / "payload.zip"
                with zipfile.ZipFile(zip_path, "w") as archive:
                    archive.writestr("one.pdf", b"%PDF-1.4 one")
                    archive.writestr("nested/two.pdf", b"%PDF-1.4 two")
                    archive.writestr("note.txt", b"ignore")
                return zip_path.read_bytes()

            orchestrator._fetch_pdf_bytes = build_zip  # type: ignore[method-assign]
            orchestrator._scrape_source(scraper, date(2026, 4, 6))

            record = repository.get_record("TEST", "ABC123")
            self.assertIsNotNone(record)
            assert record is not None
            self.assertTrue(record.file_path.endswith("/original/source.zip"))
            assets = repository.list_assets(record.id)
            self.assertEqual([asset.asset_role for asset in assets].count("original_zip"), 1)
            self.assertEqual([asset.asset_role for asset in assets].count("extracted_pdf"), 2)
            self.assertTrue(
                all(
                    asset.archive_member_path is not None
                    for asset in assets
                    if asset.asset_role == "extracted_pdf"
                )
            )

    def test_run_only_executes_enabled_sources(self) -> None:
        repository = FakeCircularRepository()
        enabled_scraper = StubScraper([self.build_circular()])
        disabled_scraper = StubScraper([self.build_circular()])
        enabled_scraper.source_name = "NSE"
        disabled_scraper.source_name = "SEBI"

        with TemporaryDirectory() as temp_dir:
            orchestrator = ScraperOrchestrator(
                storage_path=temp_dir,
                circular_repository=repository,
                enabled_sources=("NSE",),
            )
            orchestrator._fetch_pdf_bytes = lambda pdf_url, circular: b"pdf-bytes"  # type: ignore[method-assign]
            orchestrator._get_enabled_scrapers = lambda: [enabled_scraper]  # type: ignore[method-assign]

            orchestrator.run()

        self.assertEqual(len(enabled_scraper.detect_calls), 1)
        self.assertEqual(len(disabled_scraper.detect_calls), 0)

    # ─────────────────────────────────────────────────────────────────────
    # Phase 3: same-title supersession gate
    # ─────────────────────────────────────────────────────────────────────

    def _seed_same_title(
        self,
        repository: FakeCircularRepository,
        circular_id: str,
        issue_date: date,
        is_active: bool = True,
    ) -> None:
        """Pre-seed a record with the same title as the test candidate so the
        supersession gate has something to compare against."""
        seeded = Circular(
            source="TEST",
            circular_id=circular_id,
            full_reference=f"TEST/{circular_id}",
            department="OPS",
            title="Test Circular",
            issue_date=issue_date,
            url=f"https://example.com/{circular_id}",
            pdf_url=f"https://example.com/{circular_id}.pdf",
            source_item_key=f"TEST::{circular_id}",
            is_active=is_active,
        )
        repository.upsert_circular(seeded)

    def _run_orchestrator_with_no_io(
        self,
        scraper: StubScraper,
        repository: FakeCircularRepository,
    ) -> None:
        """Run `_scrape_source` with network/PDF I/O stubbed so the test
        doesn't touch S3 or HTTP."""
        # The orchestrator requires both asset_repository and
        # checkpoint_repository; pass the same FakeCircularRepository for all
        # three roles — it implements the needed subset (update_status,
        # replace_assets, get_checkpoint, set_checkpoint).
        orchestrator = ScraperOrchestrator(
            circular_repository=repository,
            asset_repository=repository,
            checkpoint_repository=repository,
            s3_client=_NoopS3Client(),
            es_client=_NoopESClient(),
        )
        orchestrator._fetch_pdf_bytes = lambda pdf_url, circular: b"pdf-bytes"  # type: ignore[method-assign]
        orchestrator._download_assets = lambda pdf_url, circular, source: ("local/path.pdf", "hash", [])  # type: ignore[method-assign]
        orchestrator._scrape_source(scraper, date(2026, 4, 6))

    def test_supersession_fresh_candidate_flips_older_inactive(self) -> None:
        """A fresh (newer issue_date) candidate ingests as active and the
        older active same-title record is flipped to is_active=False."""
        repository = FakeCircularRepository()
        self._seed_same_title(repository, "OLDER-001", date(2024, 1, 1))

        fresh = self.build_circular()  # issue_date=2026-04-06
        fresh.source_item_key = "TEST::FRESH-001"
        scraper = StubScraper([fresh], supersede_on_same_title=True)

        self._run_orchestrator_with_no_io(scraper, repository)

        records = {r.circular_id: r for r in repository.list_records()}
        self.assertIn("OLDER-001", records)
        self.assertIn("FRESH-001", {r.source_item_key.replace("TEST::", "") for r in records.values()})
        # Older record is now inactive
        self.assertFalse(records["OLDER-001"].is_active)
        # New record is active
        fresh_record = next(
            r for r in records.values() if r.source_item_key == "TEST::FRESH-001"
        )
        self.assertTrue(fresh_record.is_active)
        self.assertEqual(fresh_record.status, "FETCHED")

    def test_supersession_stale_candidate_ingested_inactive(self) -> None:
        """A stale (older issue_date) candidate lands in DB as inactive and
        does NOT disturb the existing newer active same-title record."""
        repository = FakeCircularRepository()
        self._seed_same_title(repository, "NEWER-001", date(2026, 8, 31))

        stale = self.build_circular()  # issue_date=2026-04-06 (older than 2026-08-31)
        stale.source_item_key = "TEST::STALE-001"
        scraper = StubScraper([stale], supersede_on_same_title=True)

        self._run_orchestrator_with_no_io(scraper, repository)

        records = {r.circular_id: r for r in repository.list_records()}
        # The newer record is untouched (still active)
        self.assertTrue(records["NEWER-001"].is_active)
        # The stale candidate was inserted but is_active=False
        stale_record = next(
            r for r in records.values() if r.source_item_key == "TEST::STALE-001"
        )
        self.assertFalse(stale_record.is_active)
        # list_active_same_title should now return only NEWER-001
        active_same = repository.list_active_same_title(
            source="TEST", title="Test Circular"
        )
        active_ids = {r.id for r in active_same}
        self.assertIn(records["NEWER-001"].id, active_ids)
        self.assertNotIn(stale_record.id, active_ids)

    def test_supersession_no_opt_in_is_noop(self) -> None:
        """When the scraper does not opt in, no calls to
        list_active_same_title / mark_inactive happen — older records
        remain active."""
        repository = FakeCircularRepository()
        self._seed_same_title(repository, "OLDER-002", date(2024, 1, 1))

        fresh = self.build_circular()
        fresh.source_item_key = "TEST::FRESH-002"
        # supersede_on_same_title=False (the class default)
        scraper = StubScraper([fresh])

        self._run_orchestrator_with_no_io(scraper, repository)

        records = {r.circular_id: r for r in repository.list_records()}
        # Older record was NOT flipped
        self.assertTrue(records["OLDER-002"].is_active)
        # New record is active (default behavior unchanged)
        fresh_record = next(
            r for r in records.values() if r.source_item_key == "TEST::FRESH-002"
        )
        self.assertTrue(fresh_record.is_active)

    def test_supersession_empty_title_is_noop(self) -> None:
        """When the candidate's title is empty, the gate short-circuits and
        no supersession logic runs."""
        repository = FakeCircularRepository()
        self._seed_same_title(repository, "OLDER-003", date(2024, 1, 1))

        candidate = self.build_circular()
        candidate.source_item_key = "TEST::EMPTY-TITLE"
        candidate.title = ""  # empty
        scraper = StubScraper([candidate], supersede_on_same_title=True)

        self._run_orchestrator_with_no_io(scraper, repository)

        # Older record untouched (gate short-circuited before querying)
        records = {r.circular_id: r for r in repository.list_records()}
        self.assertTrue(records["OLDER-003"].is_active)

    # ─────────────────────────────────────────────────────────────────────
    # ES cleanup on supersession
    # ─────────────────────────────────────────────────────────────────────

    def test_supersession_es_delete_failure_propagates_and_skips_db_update(self) -> None:
        """If delete_documents_for_record raises, the exception propagates and
        mark_inactive is never called — DB and ES stay in their pre-supersession
        state, and the next orchestrator run retries."""
        repository = FakeCircularRepository()
        self._seed_same_title(repository, "OLDER-ES", date(2024, 1, 1))

        class FailingESClient:
            def __init__(self):
                self.calls: list[str] = []

            def delete_documents_for_record(self, circular_db_id: str) -> None:
                self.calls.append(circular_db_id)
                raise ConnectionError(f"ES unreachable for {circular_db_id}")

        es_client = FailingESClient()
        mark_inactive_calls: list[list] = []
        original_mark_inactive = repository.mark_inactive
        repository.mark_inactive = lambda ids: mark_inactive_calls.append(ids) or original_mark_inactive(ids)

        fresh = self.build_circular()
        fresh.source_item_key = "TEST::FRESH-ES"
        scraper = StubScraper([fresh], supersede_on_same_title=True)

        orchestrator = ScraperOrchestrator(
            circular_repository=repository,
            asset_repository=repository,
            checkpoint_repository=repository,
            s3_client=_NoopS3Client(),
            es_client=es_client,  # type: ignore[arg-type]
        )
        orchestrator._fetch_pdf_bytes = lambda pdf_url, circular: b"pdf-bytes"  # type: ignore[method-assign]
        orchestrator._download_assets = lambda pdf_url, circular, source: ("local/path.pdf", "hash", [])  # type: ignore[method-assign]

        with self.assertRaises(ConnectionError) as ctx:
            orchestrator._scrape_source(scraper, date(2026, 4, 6))
        self.assertIn("ES unreachable", str(ctx.exception))

        # ES was attempted for the older record
        self.assertEqual(len(es_client.calls), 1)
        # mark_inactive was NEVER called — DB is untouched
        self.assertEqual(mark_inactive_calls, [])
        # Older record still active in DB
        records = {r.circular_id: r for r in repository.list_records()}
        self.assertTrue(records["OLDER-ES"].is_active)


if __name__ == "__main__":
    unittest.main()
