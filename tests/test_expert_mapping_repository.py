from __future__ import annotations

from datetime import date
import unittest
from uuid import uuid4

from db import get_db_client
from ingestion.repository.expert_mapping_repository import ExpertMappingRepository
from ingestion.repository._uuid_utils import _raw_to_uuid, _uuid_to_raw


TEST_CIRCULAR_ID = "TEST_EXPERT_REPO_001"
TEST_DEPT_NAME = "Legal"


class ExpertMappingRepositoryPositiveTestCase(unittest.TestCase):
    """Positive tests for ExpertMappingRepository using a real database."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.pool = get_db_client().get_pool()
        cls.repo = ExpertMappingRepository(cls.pool)
        cls._ensure_test_circular()
        cls._ensure_test_department()

    @classmethod
    def _ensure_test_circular(cls) -> None:
        with cls.pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM circulars WHERE circular_id = :1",
                (TEST_CIRCULAR_ID,),
            )
            row = cursor.fetchone()
            if not row:
                cursor.execute(
                    """
                    INSERT INTO circulars (source, circular_id, full_reference, title, issue_date, status)
                    VALUES (:1, :2, :3, :4, :5, :6)
                    """,
                    ("TEST", TEST_CIRCULAR_ID, "TEST/2024/002", "Expert Test Circular", date(2024, 3, 1), "DISCOVERED"),
                )
                conn.commit()
            cursor.execute("SELECT id FROM circulars WHERE circular_id = :1", (TEST_CIRCULAR_ID,))
            cls.test_circular_id = _raw_to_uuid(cursor.fetchone()[0])

    @classmethod
    def _ensure_test_department(cls) -> None:
        with cls.pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM properties WHERE name = :1 AND type = 'DEPARTMENT'",
                (TEST_DEPT_NAME,),
            )
            row = cursor.fetchone()
            if not row:
                cursor.execute(
                    """
                    INSERT INTO properties (name, type, metadata)
                    VALUES (:1, :2, :3)
                    """,
                    (TEST_DEPT_NAME, "DEPARTMENT", "{}"),
                )
                conn.commit()
            cursor.execute("SELECT id FROM properties WHERE name = :1 AND type = 'DEPARTMENT'", (TEST_DEPT_NAME,))
            cls.test_dept_id = _raw_to_uuid(cursor.fetchone()[0])

    def setUp(self) -> None:
        self._cleanup_mappings(self.test_circular_id)

    def _cleanup_mappings(self, circular_id) -> None:
        with self.pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM experts WHERE circular_id = :1", (_uuid_to_raw(circular_id),))
            conn.commit()

    def tearDown(self) -> None:
        self._cleanup_mappings(self.test_circular_id)

    def test_save_expert_mapping_inserts_and_returns_id(self) -> None:
        result = self.repo.save_expert_mapping(
            self.test_circular_id, self.test_dept_id,
            "Senior Counsel", "Handle regulatory filings", [{"text": "regulatory", "score": 0.9}],
        )
        self.assertIsNotNone(result)
        mappings = self.repo.get_expert_mappings_for_circular(self.test_circular_id)
        self.assertEqual(len(mappings), 1)
        self.assertEqual(mappings[0]["title"], "Senior Counsel")
        self.assertEqual(mappings[0]["text"], "Handle regulatory filings")
        self.assertEqual(len(mappings[0]["highlights"]), 1)

    def test_save_expert_mapping_multiple_experts_for_same_circular(self) -> None:
        self.repo.save_expert_mapping(
            self.test_circular_id, self.test_dept_id,
            "Expert A", "Text A", [],
        )
        self.repo.save_expert_mapping(
            self.test_circular_id, self.test_dept_id,
            "Expert B", "Text B", [],
        )
        mappings = self.repo.get_expert_mappings_for_circular(self.test_circular_id)
        self.assertEqual(len(mappings), 2)

    def test_update_expert_mapping_modifies_existing(self) -> None:
        row_id = self.repo.save_expert_mapping(
            self.test_circular_id, self.test_dept_id,
            "Original Title", "Original Text", [],
        )
        updated = self.repo.update_expert_mapping(
            row_id, self.test_dept_id, "Updated Title", "Updated Text", [{"text": "updated", "score": 0.8}],
        )
        self.assertTrue(updated)
        mappings = self.repo.get_expert_mappings_for_circular(self.test_circular_id)
        self.assertEqual(len(mappings), 1)
        self.assertEqual(mappings[0]["title"], "Updated Title")
        self.assertEqual(mappings[0]["text"], "Updated Text")
        self.assertEqual(len(mappings[0]["highlights"]), 1)

    def test_update_expert_mapping_returns_false_when_not_found(self) -> None:
        fake_uuid = uuid4()
        result = self.repo.update_expert_mapping(
            fake_uuid, self.test_dept_id, "Title", "Text", [],
        )
        self.assertFalse(result)

    def test_delete_expert_mapping_removes_record(self) -> None:
        row_id = self.repo.save_expert_mapping(
            self.test_circular_id, self.test_dept_id,
            "To Delete", "Will be removed", [],
        )
        deleted = self.repo.delete_expert_mapping(row_id)
        self.assertTrue(deleted)
        mappings = self.repo.get_expert_mappings_for_circular(self.test_circular_id)
        self.assertEqual(len(mappings), 0)

    def test_delete_expert_mapping_returns_false_when_not_found(self) -> None:
        fake_uuid = uuid4()
        result = self.repo.delete_expert_mapping(fake_uuid)
        self.assertFalse(result)

    def test_get_expert_mappings_for_circular_returns_ordered_by_created_at(self) -> None:
        self.repo.save_expert_mapping(self.test_circular_id, self.test_dept_id, "First", "First text", [])
        self.repo.save_expert_mapping(self.test_circular_id, self.test_dept_id, "Second", "Second text", [])
        mappings = self.repo.get_expert_mappings_for_circular(self.test_circular_id)
        self.assertEqual(len(mappings), 2)
        self.assertEqual(mappings[0]["title"], "First")
        self.assertEqual(mappings[1]["title"], "Second")

    def test_get_experts_by_department_filters_correctly(self) -> None:
        self.repo.save_expert_mapping(self.test_circular_id, self.test_dept_id, "Filtered Expert", "Filter test", [])
        experts, total = self.repo.get_experts_by_department(
            department_id=self.test_dept_id,
            source=None, from_date=None, to_date=None, full_circular_no=None,
            limit=10, offset=0,
        )
        self.assertGreaterEqual(total, 1)
        found = any(e["expert_name"] == "Filtered Expert" for e in experts)
        self.assertTrue(found)

    def test_get_experts_by_department_filters_by_source(self) -> None:
        self.repo.save_expert_mapping(self.test_circular_id, self.test_dept_id, "Source Test", "Source filter", [])
        experts, total = self.repo.get_experts_by_department(
            department_id=None,
            source="TEST", from_date=None, to_date=None, full_circular_no=None,
            limit=10, offset=0,
        )
        self.assertGreaterEqual(total, 1)

    def test_get_experts_by_department_pagination(self) -> None:
        for i in range(5):
            self.repo.save_expert_mapping(
                self.test_circular_id, self.test_dept_id,
                f"Page Expert {i}", f"Page text {i}", [],
            )
        experts_page1, total = self.repo.get_experts_by_department(
            department_id=None, source=None, from_date=None, to_date=None, full_circular_no=None,
            limit=2, offset=0,
        )
        self.assertLessEqual(len(experts_page1), 2)
        self.assertGreaterEqual(total, 5)

    def test_save_expert_mapping_with_complex_highlights(self) -> None:
        highlights = [
            {"text": "regulation 23A", "score": 0.95, "type": "entity", "start": 10, "end": 23},
            {"text": "compliance", "score": 0.88, "type": "keyword", "start": 45, "end": 55},
            {"text": "SEBI", "score": 0.99, "type": "regulation", "start": 100, "end": 104, "metadata": {"chapter": "3", "section": "7"}},
        ]
        row_id = self.repo.save_expert_mapping(
            self.test_circular_id, self.test_dept_id,
            "Compliance Officer", "Handles SEBI compliance matters", highlights,
        )
        self.assertIsNotNone(row_id)
        mappings = self.repo.get_expert_mappings_for_circular(self.test_circular_id)
        self.assertEqual(len(mappings), 1)
        self.assertEqual(len(mappings[0]["highlights"]), 3)
        self.assertAlmostEqual(float(mappings[0]["highlights"][2]["score"]), 0.99, places=2)
        self.assertEqual(mappings[0]["highlights"][2]["metadata"], {"chapter": "3", "section": "7"})

    def test_save_expert_mapping_with_empty_highlights_list(self) -> None:
        row_id = self.repo.save_expert_mapping(
            self.test_circular_id, self.test_dept_id,
            "No Highlights", "No highlights test", [],
        )
        self.assertIsNotNone(row_id)
        mappings = self.repo.get_expert_mappings_for_circular(self.test_circular_id)
        self.assertEqual(len(mappings), 1)
        self.assertEqual(mappings[0]["highlights"], [])

    def test_update_expert_mapping_with_nested_highlights(self) -> None:
        highlights = [{"text": "original", "score": 0.5, "nested": {"a": 1, "b": [1, 2, 3]}}]
        row_id = self.repo.save_expert_mapping(
            self.test_circular_id, self.test_dept_id,
            "Original", "Original text", highlights,
        )
        updated_highlights = [{"text": "updated", "score": 0.9, "nested": {"a": 2, "b": [4, 5, 6]}}]
        result = self.repo.update_expert_mapping(
            row_id, self.test_dept_id, "Updated", "Updated text", updated_highlights,
        )
        self.assertTrue(result)
        mappings = self.repo.get_expert_mappings_for_circular(self.test_circular_id)
        self.assertEqual(mappings[0]["highlights"][0]["nested"]["a"], 2)
        self.assertEqual(mappings[0]["highlights"][0]["nested"]["b"], [4, 5, 6])

    def test_update_expert_mapping_clears_highlights(self) -> None:
        row_id = self.repo.save_expert_mapping(
            self.test_circular_id, self.test_dept_id,
            "With Data", "Has highlights", [{"text": "data", "score": 0.8}],
        )
        result = self.repo.update_expert_mapping(
            row_id, self.test_dept_id, "No Data", "No highlights", [],
        )
        self.assertTrue(result)
        mappings = self.repo.get_expert_mappings_for_circular(self.test_circular_id)
        self.assertEqual(mappings[0]["highlights"], [])

    def test_save_multiple_experts_same_circular_different_depts(self) -> None:
        with self.pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM properties WHERE name = :1 AND type = 'DEPARTMENT'", ("Legal",))
            dept1_id = _raw_to_uuid(cursor.fetchone()[0])
            # Create another department - use out bind variable for RETURNING
            out_id = cursor.var(bytes)
            cursor.execute(
                "INSERT INTO properties (name, type, metadata) VALUES (:1, :2, :3) RETURNING id INTO :4",
                ["Finance", "DEPARTMENT", "{}", out_id],
            )
            conn.commit()
            dept2_id = _raw_to_uuid(out_id.getvalue()[0])

        self.repo.save_expert_mapping(self.test_circular_id, dept1_id, "Legal Expert", "Legal text", [])
        self.repo.save_expert_mapping(self.test_circular_id, dept2_id, "Finance Expert", "Finance text", [])
        mappings = self.repo.get_expert_mappings_for_circular(self.test_circular_id)
        self.assertEqual(len(mappings), 2)


class ExpertMappingRepositoryNegativeTestCase(unittest.TestCase):
    """Negative tests for ExpertMappingRepository using a real database."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.pool = get_db_client().get_pool()
        cls.repo = ExpertMappingRepository(cls.pool)

    def test_init_raises_when_db_pool_is_none(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            ExpertMappingRepository(None)
        self.assertIn("db_pool", str(ctx.exception))

    def test_get_expert_mappings_for_circular_returns_empty_for_nonexistent(self) -> None:
        fake_uuid = uuid4()
        result = self.repo.get_expert_mappings_for_circular(fake_uuid)
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()