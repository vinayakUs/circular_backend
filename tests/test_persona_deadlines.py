"""Tests for GET /api/personas/<slug>/deadlines.

Covers:
- Valid requests with filters and pagination
- Error cases (invalid dates, unknown persona, from > to)
- Security (SQL injection attempts)
- Edge cases (empty params, huge values, same-day range)
- Cross-persona data flow
"""
import unittest
from datetime import date, timedelta
from uuid import UUID

from app import create_app
from db.postgres_client import get_postgres_client


# ─────────────────────────────────────────────────────────────────────
# Test data setup
# ─────────────────────────────────────────────────────────────────────

# We rely on:
#   - circulars table populated with sample data
#   - circular_persona_matches table populated for personas
#   - action_items table has rows with known deadlines
#
# These are seeded by the manual backfill script in shell history.
# Tests that need specific dates use date(2020, 1, 1) → date(2030, 12, 31)
# which covers all currently-known action_item deadlines.


class PersonaDeadlinesTestCase(unittest.TestCase):
    """End-to-end tests for /api/personas/<slug>/deadlines."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.app = create_app()
        cls.client = cls.app.test_client()

    # ─── Valid requests ────────────────────────────────────

    def test_no_params(self) -> None:
        """Default window (today-30 to today+335), no filters."""
        resp = self.client.get("/api/personas/compliance-officer/deadlines")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertIn("items", body)
        self.assertIn("total", body)
        self.assertIn("limit", body)
        self.assertIn("offset", body)
        self.assertIsInstance(body["items"], list)
        # Default window: today-30 to today+335 (≈ 1 year)
        # Action_item deadline (2026-05-22) may or may not be in this window
        # depending on "today". Tests below use explicit dates to avoid flakiness.

    def test_priority_lowercase(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines"
            "?from=2020-01-01&to=2030-12-31&priority=critical"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 1)

    def test_priority_mixed_case_normalized(self) -> None:
        """?priority=Critical should match (lowercased before query)."""
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines"
            "?from=2020-01-01&to=2030-12-31&priority=Critical"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 1)

    def test_priority_with_whitespace_in_csv(self) -> None:
        """?priority=Critical,%20High — whitespace stripped before query."""
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines"
            "?from=2020-01-01&to=2030-12-31&priority=Critical,%20High"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 1)

    def test_priority_empty_csv_entries(self) -> None:
        """?priority=,,, (commas but no values) → empty array → 0 matches."""
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines"
            "?from=2020-01-01&to=2030-12-31&priority=,,,"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 0)

    def test_priority_invalid_value(self) -> None:
        """Unknown priority value → 0 matches (no error)."""
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines"
            "?from=2020-01-01&to=2030-12-31&priority=banana"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 0)

    def test_priority_high_no_match(self) -> None:
        """Valid priority that has no matching action_items → 0."""
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines"
            "?from=2020-01-01&to=2030-12-31&priority=high"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 0)

    def test_exchange_normalized_lowercase(self) -> None:
        """?exchange=nse should match (uppercased before query)."""
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines"
            "?from=2020-01-01&to=2030-12-31&exchange=nse"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 1)

    def test_exchange_no_match(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines"
            "?from=2020-01-01&to=2030-12-31&exchange=SEBI"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 0)

    def test_combined_priority_and_exchange(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines"
            "?from=2020-01-01&to=2030-12-31&priority=critical&exchange=NSE"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 1)

    def test_pagination_limit_10(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?from=2020-01-01&to=2030-12-31&limit=10"
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertEqual(body["limit"], 10)

    def test_pagination_offset_past_total(self) -> None:
        """Offset > total → empty items, but total still correct."""
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?from=2020-01-01&to=2030-12-31&offset=999"
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertEqual(body["items"], [])
        self.assertEqual(body["total"], 1)  # total still 1

    def test_limit_zero_defaults_to_100(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?limit=0"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["limit"], 100)

    def test_negative_limit_defaults_to_100(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?limit=-5"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["limit"], 100)

    def test_negative_offset_defaults_to_0(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?offset=-5"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["offset"], 0)

    def test_huge_limit_no_cap(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?limit=100000"
        )
        self.assertEqual(resp.status_code, 200)
        # We removed max cap — caller opts in to size

    def test_empty_string_params_defaulted(self) -> None:
        """?from=&to= → defaults applied (no crash on empty strings)."""
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?from=&to="
        )
        self.assertEqual(resp.status_code, 200)

    def test_only_from_param(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?from=2020-01-01"
        )
        self.assertEqual(resp.status_code, 200)

    def test_only_to_param(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?to=2030-12-31"
        )
        self.assertEqual(resp.status_code, 200)

    def test_from_equals_to_same_day(self) -> None:
        """Both bounds on 2026-05-22 — deadline on that day should match."""
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines"
            "?from=2026-05-22&to=2026-05-22"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 1)

    # ─── Error cases ────────────────────────────────────────

    def test_invalid_date_format(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?from=2026-13-99"
        )
        self.assertEqual(resp.status_code, 400)

    def test_invalid_date_format_bad_month(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?from=foo-bar-baz"
        )
        self.assertEqual(resp.status_code, 400)

    def test_non_date_string(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?from=hello"
        )
        self.assertEqual(resp.status_code, 400)

    def test_unknown_persona_returns_404(self) -> None:
        resp = self.client.get("/api/personas/nonexistent/deadlines")
        self.assertEqual(resp.status_code, 404)

    def test_from_after_to_returns_400(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?from=2027-01-01&to=2025-01-01"
        )
        self.assertEqual(resp.status_code, 400)

    def test_limit_not_integer_returns_400(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?limit=abc"
        )
        self.assertEqual(resp.status_code, 400)

    def test_offset_not_integer_returns_400(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?offset=xyz"
        )
        self.assertEqual(resp.status_code, 400)

    # ─── Security ───────────────────────────────────────────

    def test_sql_injection_in_from_rejected(self) -> None:
        """SQL injection in 'from' → 400 (date format invalid)."""
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines"
            "?from=2026-01-01%27%20OR%20%271%27=%271"
        )
        self.assertEqual(resp.status_code, 400)

    def test_sql_injection_in_priority_handled(self) -> None:
        """SQL injection in 'priority' → 200, returns 0 (no match)."""
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines"
            "?priority=%27%20OR%20%271%27=%271"
        )
        # psycopg2 parameterizes — the malicious string is just a value
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 0)

    def test_sql_injection_in_slug_returns_404(self) -> None:
        """SQL injection in path segment → 404 (no matching persona)."""
        resp = self.client.get(
            "/api/personas/anything%27%20OR%20%271=%271/deadlines"
        )
        self.assertEqual(resp.status_code, 404)

    # ─── Cross-persona data flow ────────────────────────────

    def test_compliance_officer_has_action_item(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?from=2020-01-01&to=2030-12-31"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 1)

    def test_risk_manager_shared_action_item(self) -> None:
        """Same circular matched to 3 personas — risk-manager should see it too."""
        resp = self.client.get(
            "/api/personas/risk-manager/deadlines?from=2020-01-01&to=2030-12-31"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 1)

    def test_fpi_shared_action_item(self) -> None:
        """Same circular matched to 3 personas — fpi should see it too."""
        resp = self.client.get(
            "/api/personas/fpi/deadlines?from=2020-01-01&to=2030-12-31"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 1)

    def test_operations_has_no_deadline(self) -> None:
        """operations persona has an action_item but no deadline — should not appear."""
        resp = self.client.get(
            "/api/personas/operations/deadlines?from=2020-01-01&to=2030-12-31"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 0)

    def test_trading_desk_has_no_action_items(self) -> None:
        """trading-desk matches 6 circulars but none have action_items."""
        resp = self.client.get(
            "/api/personas/trading-desk/deadlines?from=2020-01-01&to=2030-12-31"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["total"], 0)

    # ─── Response shape ─────────────────────────────────────

    def test_response_shape(self) -> None:
        resp = self.client.get(
            "/api/personas/compliance-officer/deadlines?from=2020-01-01&to=2030-12-31&limit=1"
        )
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertEqual(set(body.keys()), {"persona_slug", "items", "total", "limit", "offset"})
        self.assertEqual(body["persona_slug"], "compliance-officer")

        # Each item has the expected fields
        item = body["items"][0]
        self.assertEqual(
            set(item.keys()),
            {"id", "date", "title", "exchange", "priority", "url", "source_circular_id"},
        )
        # UUIDs parse correctly
        UUID(item["id"])
        UUID(item["source_circular_id"])
        # Date format is YYYY-MM-DD
        self.assertRegex(item["date"], r"^\d{4}-\d{2}-\d{2}$")
        # URL points to circulars page
        self.assertTrue(item["url"].startswith("/circulars/"))


if __name__ == "__main__":
    unittest.main()