from __future__ import annotations

from datetime import date
import unittest
from uuid import uuid4

from db import get_db_client
from ingestion.repository.asset_repository import AssetRepository, CircularAsset
from ingestion.repository._uuid_utils import _raw_to_uuid, _uuid_to_raw


TEST_CIRCULAR_ID = "TEST_ASSET_REPO_001"


class AssetRepositoryPositiveTestCase(unittest.TestCase):
    """Positive tests for AssetRepository using a real database."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.pool = get_db_client().get_pool()
        cls.repo = AssetRepository(cls.pool)
        cls._ensure_test_circular()

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
                    ("TEST", TEST_CIRCULAR_ID, "TEST/2024/001", "Test Circular", date(2024, 1, 15), "DISCOVERED"),
                )
                conn.commit()
            cursor.execute("SELECT id FROM circulars WHERE circular_id = :1", (TEST_CIRCULAR_ID,))
            cls.test_circular_id = _raw_to_uuid(cursor.fetchone()[0])

    def setUp(self) -> None:
        self._cleanup_assets(self.test_circular_id)

    def _cleanup_assets(self, circular_id) -> None:
        with self.pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM circular_assets WHERE circular_id = :1", (_uuid_to_raw(circular_id),))
            conn.commit()

    def tearDown(self) -> None:
        self._cleanup_assets(self.test_circular_id)

    def test_replace_assets_inserts_and_returns_records(self) -> None:
        assets = [
            CircularAsset(asset_role="original_pdf", file_path="/data/test/doc1.pdf",
                          content_hash="abc123", mime_type="application/pdf", file_size_bytes=1024),
            CircularAsset(asset_role="extracted_pdf", file_path="/data/test/doc1.txt",
                          content_hash="def456", mime_type="text/plain", file_size_bytes=256),
        ]
        result = self.repo.replace_assets(self.test_circular_id, assets)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].asset_role, "original_pdf")
        self.assertEqual(result[0].file_path, "/data/test/doc1.pdf")
        self.assertEqual(result[0].content_hash, "abc123")

    def test_replace_assets_replaces_existing_assets(self) -> None:
        initial = [
            CircularAsset(asset_role="original_pdf", file_path="/data/test/old.pdf",
                          content_hash="oldhash", mime_type="application/pdf"),
        ]
        self.repo.replace_assets(self.test_circular_id, initial)
        updated = [
            CircularAsset(asset_role="original_pdf", file_path="/data/test/new.pdf",
                          content_hash="newhash", mime_type="application/pdf"),
            CircularAsset(asset_role="extracted_pdf", file_path="/data/test/new.txt",
                          content_hash="newtext", mime_type="text/plain"),
        ]
        result = self.repo.replace_assets(self.test_circular_id, updated)
        self.assertEqual(len(result), 2)
        asset_map = {a.asset_role: a for a in result}
        self.assertEqual(asset_map["original_pdf"].file_path, "/data/test/new.pdf")
        self.assertEqual(asset_map["original_pdf"].content_hash, "newhash")
        self.assertEqual(asset_map["extracted_pdf"].file_path, "/data/test/new.txt")

    def test_list_assets_returns_sorted_assets(self) -> None:
        assets = [
            CircularAsset(asset_role="extracted_pdf", file_path="/data/test/c.txt"),
            CircularAsset(asset_role="original_pdf", file_path="/data/test/a.pdf"),
            CircularAsset(asset_role="original_zip", file_path="/data/test/b.zip"),
        ]
        self.repo.replace_assets(self.test_circular_id, assets)
        result = self.repo.list_assets(self.test_circular_id)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0].asset_role, "original_pdf")
        self.assertEqual(result[1].asset_role, "original_zip")
        self.assertEqual(result[2].asset_role, "extracted_pdf")

    def test_list_assets_with_archive_member(self) -> None:
        assets = [
            CircularAsset(asset_role="original_zip", file_path="/data/test/archive.zip",
                          archive_member_path="inner/file.pdf", mime_type="application/pdf"),
        ]
        self.repo.replace_assets(self.test_circular_id, assets)
        result = self.repo.list_assets(self.test_circular_id)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].archive_member_path, "inner/file.pdf")

    def test_get_primary_asset_returns_first_sorted_asset(self) -> None:
        assets = [
            CircularAsset(asset_role="extracted_pdf", file_path="/data/test/c.txt"),
            CircularAsset(asset_role="original_pdf", file_path="/data/test/a.pdf"),
        ]
        self.repo.replace_assets(self.test_circular_id, assets)
        result = self.repo.get_primary_asset(self.test_circular_id)
        self.assertIsNotNone(result)
        self.assertEqual(result.asset_role, "original_pdf")

    def test_replace_assets_handles_null_optional_fields(self) -> None:
        assets = [
            CircularAsset(asset_role="original_pdf", file_path="/data/test/null_test.pdf"),
        ]
        result = self.repo.replace_assets(self.test_circular_id, assets)
        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0].content_hash)
        self.assertIsNone(result[0].mime_type)
        self.assertIsNone(result[0].archive_member_path)
        self.assertIsNone(result[0].file_size_bytes)


class AssetRepositoryNegativeTestCase(unittest.TestCase):
    """Negative tests for AssetRepository using a real database."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.pool = get_db_client().get_pool()
        cls.repo = AssetRepository(cls.pool)
        cls._ensure_test_circular()

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
                    ("TEST", TEST_CIRCULAR_ID, "TEST/2024/001", "Test Circular", date(2024, 1, 15), "DISCOVERED"),
                )
                conn.commit()
            cursor.execute("SELECT id FROM circulars WHERE circular_id = :1", (TEST_CIRCULAR_ID,))
            cls.test_circular_id = _raw_to_uuid(cursor.fetchone()[0])

    def test_init_raises_when_db_pool_is_none(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            AssetRepository(None)
        self.assertIn("db_pool", str(ctx.exception))

    def test_replace_assets_with_empty_list_clears_all_assets(self) -> None:
        self._cleanup_assets(self.test_circular_id)
        self.repo.replace_assets(self.test_circular_id, [])
        result = self.repo.list_assets(self.test_circular_id)
        self.assertEqual(result, [])
        self._cleanup_assets(self.test_circular_id)

    def test_list_assets_returns_empty_for_nonexistent_circular(self) -> None:
        fake_uuid = uuid4()
        result = self.repo.list_assets(fake_uuid)
        self.assertEqual(result, [])

    def test_get_primary_asset_returns_none_for_nonexistent_circular(self) -> None:
        fake_uuid = uuid4()
        result = self.repo.get_primary_asset(fake_uuid)
        self.assertIsNone(result)

    def test_replace_assets_commits_on_success(self) -> None:
        self._cleanup_assets(self.test_circular_id)
        assets = [
            CircularAsset(asset_role="original_pdf", file_path="/data/test/commit_test.pdf"),
        ]
        self.repo.replace_assets(self.test_circular_id, assets)
        result = self.repo.list_assets(self.test_circular_id)
        self.assertEqual(len(result), 1)
        self._cleanup_assets(self.test_circular_id)

    def _cleanup_assets(self, circular_id) -> None:
        with self.pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM circular_assets WHERE circular_id = :1", (_uuid_to_raw(circular_id),))
            conn.commit()


if __name__ == "__main__":
    unittest.main()