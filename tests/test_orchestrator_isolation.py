from datetime import date
import unittest
from unittest.mock import patch

from ingestion.scrapper.base import IScraper, ScrapeDetectionResult
from ingestion.scrapper.dto import Circular
from ingestion.scrapper.orchestrator import ScraperOrchestrator
from tests.fakes import FakeCircularRepository


class _FailingScraper(IScraper):
    """IScraper whose detect_new raises — used to simulate API down."""

    source_name = "FAIL"

    def __init__(self, exc: Exception | None = None) -> None:
        self._exc = exc or ConnectionError("refused")
        self.detect_calls = 0

    def detect_new(self, from_date: date, to_date: date) -> ScrapeDetectionResult:
        self.detect_calls += 1
        raise self._exc

    def get_pdf_download_url(self, circular_id: str) -> str:
        return ""

    def parse_circular_id(self, raw_id: str) -> str:
        return raw_id


class _OkScraper(IScraper):
    """IScraper that returns empty results — used to verify the loop continues."""

    source_name = "OK"

    def __init__(self) -> None:
        self.detect_calls = 0

    def detect_new(self, from_date: date, to_date: date) -> ScrapeDetectionResult:
        self.detect_calls += 1
        return ScrapeDetectionResult(circulars=[])

    def get_pdf_download_url(self, circular_id: str) -> str:
        return ""

    def parse_circular_id(self, raw_id: str) -> str:
        return raw_id


class OrchestratorFailureIsolationTestCase(unittest.TestCase):
    """When one source's API is down, the orchestrator must continue with
    the remaining sources and only raise if ALL of them failed."""

    def _build_orchestrator(
        self,
        enabled_sources: tuple[str, ...] | list[str],
        repository: FakeCircularRepository,
    ) -> ScraperOrchestrator:
        return ScraperOrchestrator(
            circular_repository=repository,
            checkpoint_repository=repository,
            asset_repository=repository,
            s3_client=None,
            default_lookback_days=7,
            from_date=date(2026, 5, 1),
            to_date=date(2026, 5, 30),
            enabled_sources=enabled_sources,
        )

    def test_one_failing_source_does_not_block_remaining_sources(self) -> None:
        repository = FakeCircularRepository()
        failing = _FailingScraper()
        ok = _OkScraper()

        orchestrator = self._build_orchestrator(("FAIL", "OK"), repository)

        # Patch ScraperRegistry.get so enabled_sources lookup works without
        # the orchestrator's eager import side effects.
        def fake_get(name: str) -> IScraper:
            registry = {"FAIL": failing, "OK": ok}
            return registry[name]

        with patch(
            "ingestion.scrapper.registry.ScraperRegistry.get", side_effect=fake_get
        ):
            # Partial success should NOT raise — the OK source still ran.
            orchestrator.run()

        self.assertEqual(failing.detect_calls, 1)
        self.assertEqual(ok.detect_calls, 1)

    def test_all_sources_failing_raises_runtime_error(self) -> None:
        repository = FakeCircularRepository()
        fail1 = _FailingScraper()
        fail2 = _FailingScraper()

        orchestrator = self._build_orchestrator(("FAIL", "OTHER"), repository)

        def fake_get(name: str) -> IScraper:
            registry = {"FAIL": fail1, "OTHER": fail2}
            return registry[name]

        with patch(
            "ingestion.scrapper.registry.ScraperRegistry.get", side_effect=fake_get
        ):
            with self.assertRaises(RuntimeError) as ctx:
                orchestrator.run()

        self.assertIn("All enabled sources failed", str(ctx.exception))
        self.assertEqual(fail1.detect_calls, 1)
        self.assertEqual(fail2.detect_calls, 1)

    def test_successful_run_does_not_raise(self) -> None:
        repository = FakeCircularRepository()
        ok = _OkScraper()

        orchestrator = self._build_orchestrator(("OK",), repository)

        def fake_get(name: str) -> IScraper:
            return ok

        with patch(
            "ingestion.scrapper.registry.ScraperRegistry.get", side_effect=fake_get
        ):
            # Should not raise.
            orchestrator.run()

        self.assertEqual(ok.detect_calls, 1)


if __name__ == "__main__":
    unittest.main()