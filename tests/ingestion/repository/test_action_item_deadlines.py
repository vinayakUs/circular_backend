"""Unit tests for ActionItemRepository.get_deadlines SQL construction.

No database — a fake pool + cursor capture the (sql, params) pairs the
repository executes. The load-bearing detail: `limit=None` builds a data
query with NO LIMIT clause at all (the date range alone bounds the result),
runs no COUNT, and reports total = len(rows). A positive limit keeps the
old COUNT + LIMIT/OFFSET pairing.

Run with:  pytest tests/ingestion/repository/test_action_item_deadlines.py
"""
from __future__ import annotations

import unittest
from collections import deque
from contextlib import contextmanager
from datetime import date
from typing import Optional
from uuid import UUID

from ingestion.repository.action_item_repository import ActionItemRepository

ITEM_ID = UUID("11111111-1111-1111-1111-111111111111")
CIRC_ID = UUID("22222222-2222-2222-2222-222222222222")


class FakeTransaction:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeCursor:
    """Records every (sql, params) execute; results queue per call setup."""

    def __init__(self):
        self.calls: list[tuple[str, tuple]] = []
        self.results: deque = deque()

    def execute(self, sql, params=None):
        self.calls.append((sql, tuple(params or ())))

    def fetchone(self):
        return self.results.popleft() if self.results else None

    def fetchall(self):
        out = list(self.results)
        self.results.clear()
        return out


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor

    def cursor(self) -> FakeCursor:
        return self._cursor

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()


class FakePool:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor

    @contextmanager
    def acquire(self):
        yield FakeConnection(self._cursor)


class DeadlinesSQLTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.cursor = FakeCursor()
        self.repo = ActionItemRepository(db_pool=FakePool(self.cursor))

    def queue_rows(self, rows: list, count: Optional[int] = None) -> None:
        """COUNT result first (if any), then the data rows."""
        if count is not None:
            self.cursor.results.append((count,))
        self.cursor.results.extend(rows)

    def executed(self) -> list[tuple[str, tuple]]:
        return self.cursor.calls

    def data_call(self) -> tuple[str, tuple]:
        """The last executed statement — the SELECT for rows."""
        assert self.cursor.calls, "no statement was executed"
        return self.cursor.calls[-1]

    def row(self, deadline: date = date(2026, 11, 12)) -> tuple:
        return (ITEM_ID, deadline, "Do the thing", "NSE", CIRC_ID, "Circular")


class UnboundedQueryTest(DeadlinesSQLTestCase):
    def test_no_limit_omits_the_paging_clause_and_skips_count(self) -> None:
        rows = [self.row(), self.row(date(2026, 11, 20))]
        self.queue_rows(rows)

        data, total = self.repo.get_deadlines(
            persona_slug="compliance-officer",
            from_date=date(2026, 1, 1),
            to_date=date(2026, 12, 31),
            exchange_csv="NSE,SEBI",
            limit=None,
        )

        self.assertEqual(data, rows)
        self.assertEqual(total, len(rows))
        self.assertEqual(len(self.executed()), 1, "COUNT must not run when unbounded")
        sql, params = self.data_call()
        self.assertIn("ORDER BY", sql)
        self.assertNotIn("LIMIT", sql)
        self.assertNotIn("OFFSET", sql)
        # persona_slug, from, to, exchange ×2 — and nothing else.
        self.assertEqual(
            params,
            ("compliance-officer", date(2026, 1, 1), date(2026, 12, 31), "NSE,SEBI", "NSE,SEBI"),
        )

    def test_offset_without_limit_becomes_a_bare_offset(self) -> None:
        self.queue_rows([self.row()])
        self.repo.get_deadlines(from_date=date(2026, 1, 1), limit=None, offset=7)
        sql, params = self.data_call()
        self.assertIn("OFFSET %s", sql)
        self.assertNotIn("LIMIT", sql)
        self.assertEqual(params[-1], 7)

    def test_repository_rejects_a_nonpositive_limit(self) -> None:
        for bad_limit in (0, -5):
            with self.assertRaises(ValueError):
                self.repo.get_deadlines(limit=bad_limit)
            self.assertEqual(self.executed(), [])


class PagedQueryTest(DeadlinesSQLTestCase):
    def test_limit_restores_count_plus_paged_select(self) -> None:
        rows = [self.row()]
        self.queue_rows(rows, count=137)

        data, total = self.repo.get_deadlines(
            persona_slug=None,
            from_date=date(2026, 1, 1),
            to_date=date(2026, 12, 31),
            limit=10,
            offset=20,
        )

        self.assertEqual(data, rows)
        self.assertEqual(total, 137)
        self.assertEqual(len(self.executed()), 2)

        count_sql, count_params = self.executed()[0]
        self.assertIn("COUNT(*)", count_sql)
        # No persona_slug here: from, to, exchange ×2.
        self.assertEqual(count_params, (date(2026, 1, 1), date(2026, 12, 31), None, None))
        self.assertNotIn("LIMIT", count_sql)

        data_sql, data_params = self.data_call()
        self.assertIn("LIMIT  %s OFFSET %s", data_sql)
        self.assertEqual(
            data_params,
            (date(2026, 1, 1), date(2026, 12, 31), None, None, 10, 20),
        )

    def test_exchange_filter_binds_into_both_statements(self) -> None:
        self.queue_rows([], count=0)
        _, total = self.repo.get_deadlines(
            from_date=date(2026, 1, 1), to_date=date(2026, 3, 31),
            exchange_csv="NSE", limit=5,
        )
        self.assertEqual(total, 0)
        for _sql, params in self.executed():
            self.assertIn("NSE", params)
