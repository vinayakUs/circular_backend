import unittest
from unittest.mock import patch

from db import DatabaseClient, get_db_client


class DatabaseClientTestCase(unittest.TestCase):
    def tearDown(self) -> None:
        get_db_client().close()

    def test_get_db_client_returns_singleton(self) -> None:
        first = get_db_client()
        second = get_db_client()

        self.assertIs(first, second)
        self.assertIsInstance(first, DatabaseClient)

    @patch("db.client.DatabaseClient._create_pool")
    def test_get_pool_initializes_and_returns_pool(self, create_pool) -> None:
        client = get_db_client()
        fake_pool = type(
            "FakePool",
            (),
            {"open": lambda self: None, "close": lambda self: None},
        )()
        create_pool.return_value = fake_pool

        pool = client.get_pool()

        self.assertIs(pool, fake_pool)
        create_pool.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
