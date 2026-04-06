from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from ingestion.repository import CircularRepository
from ingestion.scrapper.base import IScraper
from ingestion.scrapper.dto import Circular
from ingestion.scrapper.orchestrator import ScraperOrchestrator


class StubScraper(IScraper):
    source_name = "TEST"

    def __init__(self, circulars: list[Circular]) -> None:
        self.circulars = circulars
        self.detect_calls: list[tuple[date, date]] = []

    def detect_new(self, from_date: date, to_date: date) -> list[Circular]:
        self.detect_calls.append((from_date, to_date))
        return list(self.circulars)

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
        )

    def test_orchestrator_uses_repository_checkpoint(self) -> None:
        repository = CircularRepository()
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
        repository = CircularRepository()
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
        repository = CircularRepository()
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
        repository = CircularRepository()
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

    def test_run_only_executes_enabled_sources(self) -> None:
        repository = CircularRepository()
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
