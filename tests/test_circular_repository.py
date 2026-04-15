import unittest

from ingestion.repository import CircularRepository


class CircularRepositoryTestCase(unittest.TestCase):
    def test_constructor_requires_db_pool(self) -> None:
        with self.assertRaises(ValueError):
            CircularRepository(None)

    def test_schema_sql_includes_source_item_key_index(self) -> None:
        repository = CircularRepository(object())

        schema_sql = repository.schema_sql()

        self.assertIn("source_item_key TEXT", schema_sql)
        self.assertIn(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_circulars_source_item_key",
            schema_sql,
        )
        self.assertIn("CREATE TABLE IF NOT EXISTS circular_assets", schema_sql)
        self.assertIn("archive_member_path TEXT", schema_sql)


if __name__ == "__main__":
    unittest.main()
