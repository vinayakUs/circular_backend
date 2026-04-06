from datetime import date
import unittest

from ingestion.repository import CircularRepository
from ingestion.scrapper.dto import Circular


class CircularRepositoryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = CircularRepository()

    def test_upsert_circular_deduplicates_by_source_and_id(self) -> None:
        circular = Circular(
            source="NSE",
            circular_id="FAOP73629",
            full_reference="NSE/FAOP/73629",
            department="FAOP",
            title="Business Continuity",
            issue_date=date(2026, 4, 6),
            url="https://example.com/circular",
            pdf_url="https://example.com/circular.pdf",
        )

        first_id, created_first = self.repository.upsert_circular(circular)
        second_id, created_second = self.repository.upsert_circular(circular)

        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first_id, second_id)
        self.assertEqual(len(self.repository.list_records()), 1)

    def test_checkpoint_round_trip_is_per_source(self) -> None:
        checkpoint_date = date(2026, 4, 6)

        self.repository.set_checkpoint("NSE", checkpoint_date)

        self.assertEqual(self.repository.get_checkpoint("NSE"), checkpoint_date)
        self.assertIsNone(self.repository.get_checkpoint("SEBI"))


if __name__ == "__main__":
    unittest.main()
