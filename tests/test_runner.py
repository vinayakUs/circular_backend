import unittest
from unittest.mock import patch

from ingestion.scrapper import runner


class RunnerTestCase(unittest.TestCase):
    def test_parse_sources_returns_uppercase_tuple(self) -> None:
        self.assertEqual(runner._parse_sources("nse,bse"), ("NSE", "BSE"))
        self.assertIsNone(runner._parse_sources(None))

    @patch("ingestion.scrapper.runner.get_db_client")
    @patch("ingestion.scrapper.runner.ScraperOrchestrator")
    @patch("sys.argv", ["run-scrapper", "--sources", "NSE"])
    def test_main_builds_orchestrator_with_sources(
        self, orchestrator_cls, get_db_client
    ) -> None:
        orchestrator = orchestrator_cls.return_value
        db_client = get_db_client.return_value
        db_client.get_pool.return_value = object()

        result = runner.main()

        orchestrator_cls.assert_called_once()
        _, kwargs = orchestrator_cls.call_args
        self.assertEqual(kwargs["enabled_sources"], ("NSE",))
        self.assertIs(kwargs["db_pool"], db_client.get_pool.return_value)
        orchestrator.run.assert_called_once_with()
        db_client.close.assert_called_once_with()
        self.assertEqual(result, 0)


if __name__ == "__main__":
    unittest.main()
