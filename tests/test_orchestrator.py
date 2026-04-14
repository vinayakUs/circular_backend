from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ingestion.scrapper.base import IScraper, ScrapeDetectionResult
from ingestion.scrapper.dto import Circular
from ingestion.scrapper.orchestrator import ScraperOrchestrator
from tests.fakes import FakeCircularRepository


class StubScraper(IScraper):
    source_name = "TEST"

    def __init__(
        self,
        circulars: list[Circular],
        failed_circulars: list[Circular] | None = None,
        has_incomplete_items: bool = False,
    ) -> None:
        self.circulars = circulars
        self.failed_circulars = failed_circulars or []
        self.has_incomplete_items = has_incomplete_items
        self.detect_calls: list[tuple[date, date]] = []

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
            self.assertIn("HO_(68)2026-IMD-POD-2_I_5780_2026.pdf", record.file_path)

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


if __name__ == "__main__":
    unittest.main()
