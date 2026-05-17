from __future__ import annotations

from datetime import date, datetime, timezone
import unittest
from uuid import uuid4

from db import get_db_client
from ingestion.repository.circular_repository import CircularRepository
from ingestion.repository._uuid_utils import _raw_to_uuid, _uuid_to_raw


class CircularRepositoryPositiveTestCase(unittest.TestCase):
    """Positive tests for CircularRepository using a real database."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.pool = get_db_client().get_pool()
        cls.repo = CircularRepository(cls.pool)
        cls._ensure_test_circular()

    @classmethod
    def _ensure_test_circular(cls) -> None:
        with cls.pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM circulars WHERE source = :1 AND circular_id = :2",
                ("TEST", "CIRC_TEST_001"),
            )
            row = cursor.fetchone()
            if not row:
                out_id = cursor.var(bytes)
                cursor.execute(
                    """
                    INSERT INTO circulars (source, circular_id, source_item_key, full_reference,
                                           department, title, issue_date, status, detected_at)
                    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
                    RETURNING id INTO :10
                    """,
                    ("TEST", "CIRC_TEST_001", "TEST_KEY_001", "REF/TEST/001",
                     "Legal", "Test Circular for Repo", date(2024, 1, 15),
                     "DISCOVERED", datetime.now(timezone.utc), out_id),
                )
                conn.commit()

    def setUp(self) -> None:
        self._get_or_create_circular_id()

    def _get_or_create_circular_id(self):
        with self.pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM circulars WHERE source = :1 AND circular_id = :2",
                ("TEST", "CIRC_TEST_001"),
            )
            row = cursor.fetchone()
            if row:
                self.test_record_id = _raw_to_uuid(row[0])

    def test_upsert_circular_inserts_new_record(self) -> None:
        unique_id = str(uuid4())[:8].upper()
        record_id, inserted = self.repo.upsert_circular(
            source="TEST",
            circular_id=f"CIRC_TEST_INSERT_{unique_id}",
            source_item_key=f"INSERT_KEY_{unique_id}",
            full_reference=f"REF/TEST/INSERT/{unique_id}",
            department="Finance",
            title="Insert Test Circular",
            issue_date=date(2024, 2, 1),
            detected_at=datetime.now(timezone.utc),
        )
        self.assertIsNotNone(record_id)
        self.assertTrue(inserted)
        # Verify it can be retrieved
        record = self.repo.get_record("TEST", f"CIRC_TEST_INSERT_{unique_id}")
        self.assertIsNotNone(record)
        self.assertEqual(record.full_reference, f"REF/TEST/INSERT/{unique_id}")
        self.assertEqual(record.department, "Finance")

    def test_upsert_circular_updates_existing_record(self) -> None:
        unique_id = str(uuid4())[:8].upper()
        # First insert
        record_id, _ = self.repo.upsert_circular(
            source="TEST",
            circular_id=f"CIRC_TEST_UPDATE_{unique_id}",
            source_item_key=f"UPDATE_KEY_{unique_id}",
            full_reference=f"REF/TEST/UPDATE/{unique_id}",
            department="Old Dept",
            title="Update Test Circular",
            issue_date=date(2024, 3, 1),
            detected_at=datetime.now(timezone.utc),
        )
        # Update it
        record_id2, inserted = self.repo.upsert_circular(
            source="TEST",
            circular_id=f"CIRC_TEST_UPDATE_{unique_id}",
            source_item_key=f"UPDATE_KEY_{unique_id}",
            full_reference=f"REF/TEST/UPDATE/{unique_id}/V2",
            department="New Dept",
            title="Updated Circular",
            issue_date=date(2024, 3, 15),
            detected_at=datetime.now(timezone.utc),
        )
        self.assertEqual(record_id, record_id2)
        self.assertFalse(inserted)
        # Verify update
        record = self.repo.get_record("TEST", f"CIRC_TEST_UPDATE_{unique_id}")
        self.assertEqual(record.full_reference, f"REF/TEST/UPDATE/{unique_id}/V2")
        self.assertEqual(record.department, "New Dept")

    def test_get_record_returns_correct_data(self) -> None:
        record = self.repo.get_record("TEST", "CIRC_TEST_001")
        self.assertIsNotNone(record)
        self.assertEqual(record.source, "TEST")
        self.assertEqual(record.circular_id, "CIRC_TEST_001")
        self.assertEqual(record.full_reference, "REF/TEST/001")
        self.assertEqual(record.department, "Legal")

    def test_get_record_by_id(self) -> None:
        record = self.repo.get_record_by_id(self.test_record_id)
        self.assertIsNotNone(record)
        self.assertEqual(record.source, "TEST")

    def test_get_record_by_circular_id_with_source(self) -> None:
        record = self.repo.get_record_by_circular_id("CIRC_TEST_001", source="TEST")
        self.assertIsNotNone(record)
        self.assertEqual(record.circular_id, "CIRC_TEST_001")

    def test_get_record_by_circular_id_without_source(self) -> None:
        record = self.repo.get_record_by_circular_id("CIRC_TEST_001")
        self.assertIsNotNone(record)
        self.assertEqual(record.circular_id, "CIRC_TEST_001")

    def test_get_record_by_full_reference(self) -> None:
        record = self.repo.get_record_by_full_reference("REF/TEST/001")
        self.assertIsNotNone(record)
        self.assertEqual(record.full_reference, "REF/TEST/001")

    def test_list_records_returns_all_for_source(self) -> None:
        records = self.repo.list_records(source="TEST")
        self.assertIsInstance(records, list)
        self.assertGreater(len(records), 0)
        for r in records:
            self.assertEqual(r.source, "TEST")

    def test_list_records_returns_all_when_no_source(self) -> None:
        records = self.repo.list_records()
        self.assertIsInstance(records, list)

    def test_list_paginated_returns_correct_slice(self) -> None:
        records, total = self.repo.list_paginated(limit=2, offset=0)
        self.assertLessEqual(len(records), 2)
        self.assertGreaterEqual(total, len(records))

    def test_list_paginated_filters_by_source(self) -> None:
        records, total = self.repo.list_paginated(source="TEST", limit=10, offset=0)
        for r in records:
            self.assertEqual(r.source, "TEST")

    def test_list_paginated_filters_by_date_range(self) -> None:
        records, total = self.repo.list_paginated(
            from_date=date(2024, 1, 1),
            to_date=date(2024, 12, 31),
            limit=10, offset=0,
        )
        for r in records:
            self.assertGreaterEqual(r.issue_date.date(), date(2024, 1, 1))
            self.assertLessEqual(r.issue_date.date(), date(2024, 12, 31))

    def test_update_status(self) -> None:
        record_id, _ = self.repo.upsert_circular(
            source="TEST",
            circular_id="CIRC_TEST_STATUS",
            source_item_key="STATUS_KEY",
            full_reference="REF/TEST/STATUS",
            department="Legal",
            title="Status Test",
            issue_date=date(2024, 4, 1),
            detected_at=datetime.now(timezone.utc),
        )
        self.repo.update_status(record_id, "PROCESSED", "All done")
        record = self.repo.get_record_by_id(record_id)
        self.assertEqual(record.status, "PROCESSED")

    def test_update_applicable_to_nse(self) -> None:
        record_id, _ = self.repo.upsert_circular(
            source="TEST",
            circular_id="CIRC_TEST_NSE",
            source_item_key="NSE_KEY",
            full_reference="REF/TEST/NSE",
            department="Legal",
            title="NSE Test",
            issue_date=date(2024, 5, 1),
            detected_at=datetime.now(timezone.utc),
        )
        self.repo.update_applicable_to_nse(record_id, True)
        record = self.repo.get_record_by_id(record_id)
        self.assertTrue(record.applicable_to_nse)

    def test_get_source_counts(self) -> None:
        counts = self.repo.get_source_counts(["TEST", "SEBI", "NSE"])
        self.assertIsInstance(counts, dict)
        self.assertIn("TEST", counts)
        self.assertIn("SEBI", counts)
        self.assertIn("NSE", counts)
        self.assertGreaterEqual(counts["TEST"], 1)

    def test_exists_by_source_and_id_returns_true_for_existing(self) -> None:
        exists = self.repo.exists_by_source_and_id("TEST", "CIRC_TEST_001")
        self.assertTrue(exists)

    def test_exists_by_source_and_id_returns_false_for_nonexistent(self) -> None:
        exists = self.repo.exists_by_source_and_id("TEST", "NONEXISTENT_CIRC")
        self.assertFalse(exists)


class CircularRepositoryNegativeTestCase(unittest.TestCase):
    """Negative tests for CircularRepository using a real database."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.pool = get_db_client().get_pool()
        cls.repo = CircularRepository(cls.pool)

    def test_init_raises_when_db_pool_is_none(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            CircularRepository(None)
        self.assertIn("db_pool", str(ctx.exception))

    def test_get_record_returns_none_for_nonexistent(self) -> None:
        record = self.repo.get_record("NONEXISTENT", "NONEXISTENT")
        self.assertIsNone(record)

    def test_get_record_by_id_returns_none_for_nonexistent(self) -> None:
        fake_uuid = uuid4()
        record = self.repo.get_record_by_id(fake_uuid)
        self.assertIsNone(record)

    def test_get_record_by_circular_id_returns_none_for_nonexistent(self) -> None:
        record = self.repo.get_record_by_circular_id("NONEXISTENT_CIRC")
        self.assertIsNone(record)

    def test_get_record_by_full_reference_returns_none_for_nonexistent(self) -> None:
        record = self.repo.get_record_by_full_reference("NONEXISTENT/REF")
        self.assertIsNone(record)

    def test_list_records_returns_empty_for_nonexistent_source(self) -> None:
        records = self.repo.list_records(source="NONEXISTENT_SOURCE")
        self.assertEqual(records, [])

    def test_list_paginated_returns_zero_for_no_matches(self) -> None:
        records, total = self.repo.list_paginated(source="NONEXISTENT_XYZ", limit=10, offset=0)
        self.assertEqual(records, [])
        self.assertEqual(total, 0)

    def test_get_source_counts_returns_zero_for_nonexistent_sources(self) -> None:
        counts = self.repo.get_source_counts(["NONEXISTENT_1", "NONEXISTENT_2"])
        self.assertEqual(counts["NONEXISTENT_1"], 0)
        self.assertEqual(counts["NONEXISTENT_2"], 0)

    def test_get_source_counts_returns_empty_for_empty_input(self) -> None:
        counts = self.repo.get_source_counts([])
        self.assertEqual(counts, {})


if __name__ == "__main__":
    unittest.main()