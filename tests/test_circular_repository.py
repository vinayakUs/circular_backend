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

    def test_upsert_circular_merges_failed_placeholder_by_source_item_key(self) -> None:
        failed = Circular(
            source="SEBI",
            circular_id="SEBI_PENDING::EXAMPLE_12345.HTML",
            full_reference="SEBI_PENDING::EXAMPLE_12345.HTML",
            department="",
            title="Clarification regarding compliance requirements",
            issue_date=date(2026, 4, 12),
            url="https://www.sebi.gov.in/legal/circulars/apr-2026/example_12345.html",
            pdf_url="",
            source_item_key="https://www.sebi.gov.in/legal/circulars/apr-2026/example_12345.html",
        )
        recovered = Circular(
            source="SEBI",
            circular_id="HO/CFD/SEC/123/2026",
            full_reference="HO/CFD/SEC/123/2026",
            department="",
            title="Clarification regarding compliance requirements",
            issue_date=date(2026, 4, 12),
            url="https://www.sebi.gov.in/legal/circulars/apr-2026/example_12345.html",
            pdf_url="https://www.sebi.gov.in/sebi_data/attachdocs/apr-2026/abcdef.pdf",
            source_item_key="https://www.sebi.gov.in/legal/circulars/apr-2026/example_12345.html",
        )

        failed_id, created_failed = self.repository.upsert_circular(failed)
        recovered_id, created_recovered = self.repository.upsert_circular(recovered)

        self.assertTrue(created_failed)
        self.assertFalse(created_recovered)
        self.assertEqual(failed_id, recovered_id)
        self.assertEqual(len(self.repository.list_records()), 1)
        record = self.repository.get_record("SEBI", "HO/CFD/SEC/123/2026")
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(
            record.source_item_key,
            "https://www.sebi.gov.in/legal/circulars/apr-2026/example_12345.html",
        )


if __name__ == "__main__":
    unittest.main()
