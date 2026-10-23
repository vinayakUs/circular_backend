"""Controller tests for the persona circulars endpoint.

    GET /api/personas/<slug>/circulars

Same harness as `test_circulars.py`: a bare `Flask()` plus
`register_routes()` so `create_app()` (and its RAG warmup) never runs, with
`patch.object` standing in for the database. No database is touched — SQL
behaviour is exercised separately against a live database.

`get_personas` is patched too, so slug validation is pinned to a fixture
instead of whatever `config/personas.json` happens to contain.

Run with:  pytest tests/app/routes/test_personas.py
"""
from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch
from uuid import UUID

from flask import Flask

from app.routes import personas
from app.services.personas import Persona
from ingestion.repository.circular_repository import CircularRecord
from utils.exchange_source import ExchangeSource

#: The one validation message a bad source must produce, verbatim from
#: ExchangeSource.parse_list.
BAD_SOURCE_ERROR = "source must be 'NSE', 'SEBI', 'SEBI_MASTER'."

RECORD_ID = UUID("3f2b9c1a-7e4d-4a11-9b02-5c8e1d4f7a63")


def make_record(**overrides) -> CircularRecord:
    """A fully-populated CircularRecord; override only what a test cares about."""
    defaults = dict(
        id=RECORD_ID,
        source="NSE",
        circular_id="NSE/HO/MEM/2026/9912",
        source_item_key="key-9912",
        full_reference="NSE/HO/MEM/2026/9912",
        department="Membership",
        title="Introduction of revised client registration framework",
        issue_date=date(2026, 10, 15),
        effective_date=None,
        url="https://nsearchives.nseindia.com/circulars/xyz.pdf",
        pdf_url="https://nsearchives.nseindia.com/circulars/xyz.pdf",
        content_hash="abc123",
        status="FETCHED",
        error_message=None,
        detected_at=datetime(2026, 10, 15, 6, 0, tzinfo=timezone.utc),
        created_at=datetime(2026, 10, 15, 6, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 10, 15, 6, 0, tzinfo=timezone.utc),
        es_indexed_at=None,
        es_chunk_count=None,
        es_index_name=None,
        applicable_to_nse=True,
        is_active=True,
        signatory=[{"name": "A. Sharma", "designation": "GM, Membership"}],
    )
    defaults.update(overrides)
    return CircularRecord(**defaults)


class PersonaCircularsRouteTestCase(unittest.TestCase):
    """Base fixture — the setUp of every group below."""

    SLUG = "compliance-officer"

    def setUp(self) -> None:
        app = Flask(__name__)
        personas.register_routes(app)
        app.config.update(TESTING=True)
        self.client = app.test_client()

        for target in ("get_postgres_client", "CircularRepository", "get_personas"):
            patcher = patch.object(personas, target)
            self.addCleanup(patcher.stop)
            setattr(self, f"_{target}_patcher", patcher)

        db_client = self._get_postgres_client_patcher.start()
        repository_cls = self._CircularRepository_patcher.start()
        get_personas = self._get_personas_patcher.start()

        db_client.return_value.get_pool.return_value = object()
        get_personas.return_value = [
            Persona(
                slug=self.SLUG,
                name="Compliance Officer",
                description="Regulatory compliance, SEBI directives.",
                icon="shield-check",
            )
        ]

        self.repository = repository_cls.return_value
        self.repository.list_for_persona.return_value = ([], 0)

    # ── helpers ─────────────────────────────────────────────────
    def list_url(self, slug: str | None = None) -> str:
        return f"/api/personas/{slug or self.SLUG}/circulars"

    def get_list(self, query: str = "", slug: str | None = None):
        return self.client.get(f"{self.list_url(slug)}?{query}")

    def repo_kwargs(self) -> dict:
        """What the route handed to CircularRepository.list_for_persona."""
        self.repository.list_for_persona.assert_called_once()
        return self.repository.list_for_persona.call_args.kwargs

    def list_sources(self, query: str = ""):
        """Assert 200, then return the parsed sources from the route."""
        resp = self.get_list(f"limit=5&{query}")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        return self.repo_kwargs()["sources"]

    def assert_rejected(self, query: str, message: str, status: int = 400) -> None:
        """Assert a 4xx with `message`, and that no query was issued."""
        resp = self.get_list(query)
        self.assertEqual(resp.status_code, status, resp.get_json())
        self.assertEqual(resp.get_json()["error"], message)
        self.repository.list_for_persona.assert_not_called()

    def assert_source_rejected(self, query: str) -> None:
        self.assert_rejected(f"limit=5&{query}", BAD_SOURCE_ERROR)


# ══════════════════════════════════════════════════════════════════════
# Validation
# ══════════════════════════════════════════════════════════════════════
class PersonaCircularsValidationTest(PersonaCircularsRouteTestCase):
    def test_unknown_persona_is_rejected(self) -> None:
        resp = self.get_list("", slug="nope")
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.get_json()["error"], "Unknown persona: 'nope'")
        self.repository.list_for_persona.assert_not_called()

    def test_unknown_persona_is_checked_before_any_other_param(self) -> None:
        # A bad `source` alongside a bad slug still yields the 404: the slug
        # is validated first, so the caller learns the real problem.
        resp = self.get_list("source=BOGUS", slug="nope")
        self.assertEqual(resp.status_code, 404)
        self.repository.list_for_persona.assert_not_called()

    def test_known_persona_is_accepted(self) -> None:
        self.assertEqual(self.get_list().status_code, 200)

    def test_malformed_from_is_rejected(self) -> None:
        self.assert_rejected(
            "from=15-10-2026", "from must be YYYY-MM-DD, got '15-10-2026'"
        )

    def test_malformed_to_is_rejected(self) -> None:
        self.assert_rejected(
            "to=not-a-date", "to must be YYYY-MM-DD, got 'not-a-date'"
        )

    def test_to_before_from_is_rejected(self) -> None:
        self.assert_rejected("from=2026-10-01&to=2026-09-01", "to must be >= from")

    def test_bad_limit_is_rejected(self) -> None:
        self.assert_rejected("limit=many", "limit and offset must be integers")

    def test_bad_offset_is_rejected(self) -> None:
        self.assert_rejected("offset=2.5", "limit and offset must be integers")

    def test_negative_offset_is_clamped_to_zero(self) -> None:
        resp = self.get_list("offset=-40")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        self.assertEqual(self.repo_kwargs()["offset"], 0)

    def test_limit_is_clamped_to_at_least_one(self) -> None:
        resp = self.get_list("limit=0")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        self.assertEqual(self.repo_kwargs()["limit"], 1)

    def test_limit_is_clamped_to_100(self) -> None:
        resp = self.get_list("limit=5000")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        self.assertEqual(self.repo_kwargs()["limit"], 100)

    def test_default_limit_is_ten(self) -> None:
        self.assertEqual(self.get_list().status_code, 200)
        self.assertEqual(self.repo_kwargs()["limit"], 10)

    def test_persona_slug_is_forwarded_to_the_repository(self) -> None:
        self.assertEqual(self.get_list().status_code, 200)
        self.assertEqual(self.repo_kwargs()["persona_slug"], self.SLUG)


# ══════════════════════════════════════════════════════════════════════
# Source parsing — mirrors GET /api/circulars
# ══════════════════════════════════════════════════════════════════════
class PersonaCircularsSourceTest(PersonaCircularsRouteTestCase):
    def test_absent_source_means_no_filter(self) -> None:
        self.assertIsNone(self.list_sources())

    def test_single_source_becomes_a_one_element_list(self) -> None:
        self.assertEqual(self.list_sources("source=NSE"), [ExchangeSource.NSE])

    def test_repeated_sources_are_all_collected(self) -> None:
        self.assertEqual(
            self.list_sources("source=NSE&source=SEBI"),
            [ExchangeSource.NSE, ExchangeSource.SEBI],
        )

    def test_all_three_sources_together(self) -> None:
        self.assertEqual(
            self.list_sources("source=NSE&source=SEBI&source=SEBI_MASTER"),
            [ExchangeSource.NSE, ExchangeSource.SEBI, ExchangeSource.SEBI_MASTER],
        )

    def test_source_is_uppercased(self) -> None:
        self.assertEqual(
            self.list_sources("source=sebi_master"), [ExchangeSource.SEBI_MASTER]
        )

    def test_mixed_case_across_repeated_values(self) -> None:
        self.assertEqual(
            self.list_sources("source=nse&source=SeBi"),
            [ExchangeSource.NSE, ExchangeSource.SEBI],
        )

    def test_surrounding_whitespace_is_stripped(self) -> None:
        self.assertEqual(self.list_sources("source=%20NSE%20"), [ExchangeSource.NSE])

    def test_whitespace_only_source_is_treated_as_absent(self) -> None:
        self.assertIsNone(self.list_sources("source=%20%20%20"))

    def test_empty_source_is_treated_as_absent(self) -> None:
        self.assertIsNone(self.list_sources("source="))

    def test_blank_values_beside_a_real_one_are_dropped(self) -> None:
        self.assertEqual(
            self.list_sources("source=&source=NSE&source=%20"),
            [ExchangeSource.NSE],
        )

    def test_all_sentinel_is_rejected(self) -> None:
        self.assert_source_rejected("source=ALL")

    def test_lowercase_all_is_rejected(self) -> None:
        self.assert_source_rejected("source=all")

    def test_unknown_source_is_rejected(self) -> None:
        self.assert_source_rejected("source=NONSENSE")

    def test_one_invalid_value_rejects_the_whole_query(self) -> None:
        self.assert_source_rejected("source=NSE&source=BOGUS")

    def test_source_rejection_precedes_the_repository_call(self) -> None:
        self.assert_source_rejected("source=NONSENSE")
        self.repository.list_for_persona.assert_not_called()


# ══════════════════════════════════════════════════════════════════════
# Date handling
# ══════════════════════════════════════════════════════════════════════
class PersonaCircularsDateTest(PersonaCircularsRouteTestCase):
    def test_omitted_dates_default_to_the_last_thirty_days(self) -> None:
        self.assertEqual(self.get_list().status_code, 200)
        kwargs = self.repo_kwargs()
        today = date.today()
        self.assertEqual(kwargs["from_date"], today - timedelta(days=30))
        self.assertEqual(kwargs["to_date"], today)

    def test_explicit_dates_are_forwarded_parsed(self) -> None:
        resp = self.get_list("from=2026-01-01&to=2026-06-30")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        kwargs = self.repo_kwargs()
        self.assertEqual(kwargs["from_date"], date(2026, 1, 1))
        self.assertEqual(kwargs["to_date"], date(2026, 6, 30))

    def test_blank_dates_fall_back_to_the_defaults(self) -> None:
        self.assertEqual(self.get_list("from=&to=").status_code, 200)
        kwargs = self.repo_kwargs()
        today = date.today()
        self.assertEqual(kwargs["from_date"], today - timedelta(days=30))
        self.assertEqual(kwargs["to_date"], today)

    def test_single_day_range_is_accepted(self) -> None:
        resp = self.get_list("from=2026-10-15&to=2026-10-15")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        self.assertEqual(self.repo_kwargs()["from_date"], date(2026, 10, 15))

    def test_iso_dates_are_passed_as_date_objects_not_strings(self) -> None:
        self.assertEqual(self.get_list("from=2026-01-01&to=2026-06-30").status_code, 200)
        kwargs = self.repo_kwargs()
        self.assertIsInstance(kwargs["from_date"], date)
        self.assertIsInstance(kwargs["to_date"], date)


# ══════════════════════════════════════════════════════════════════════
# Response shape — must stay identical to GET /api/circulars
# ══════════════════════════════════════════════════════════════════════
class PersonaCircularsResponseTest(PersonaCircularsRouteTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.repository.list_for_persona.return_value = ([make_record()], 1)

    def first_item(self, query: str = "") -> dict:
        resp = self.get_list(query)
        self.assertEqual(resp.status_code, 200, resp.get_json())
        return resp.get_json()["data"]["circulars"][0]

    def test_envelope_has_data_and_pagination(self) -> None:
        payload = self.get_list().get_json()
        self.assertEqual(set(payload), {"data", "pagination"})
        self.assertEqual(set(payload["data"]), {"circulars"})

    def test_item_keys_match_the_circulars_endpoint(self) -> None:
        self.assertEqual(
            set(self.first_item()),
            {
                "id",
                "source",
                "circular_id",
                "full_reference",
                "department",
                "title",
                "issue_date",
                "applicable_to_nse",
                "status",
                "url",
                "signatories",
                "is_active",
            },
        )

    def test_item_fields_are_serialised(self) -> None:
        item = self.first_item()
        self.assertEqual(item["id"], str(RECORD_ID))
        self.assertEqual(item["source"], "NSE")
        self.assertEqual(item["full_reference"], "NSE/HO/MEM/2026/9912")
        self.assertEqual(item["department"], "Membership")
        self.assertEqual(
            item["title"], "Introduction of revised client registration framework"
        )
        self.assertTrue(item["applicable_to_nse"])
        self.assertTrue(item["is_active"])

    def test_issue_date_is_an_iso_string(self) -> None:
        self.assertEqual(self.first_item()["issue_date"], "2026-10-15")

    def test_blank_department_and_url_are_null(self) -> None:
        self.repository.list_for_persona.return_value = (
            [make_record(department="", url="")],
            1,
        )
        item = self.first_item()
        self.assertIsNone(item["department"])
        self.assertIsNone(item["url"])

    def test_signatories_are_mapped_to_name_and_designation(self) -> None:
        self.assertEqual(
            self.first_item()["signatories"],
            [{"name": "A. Sharma", "designation": "GM, Membership"}],
        )

    def test_missing_signatories_become_an_empty_list(self) -> None:
        self.repository.list_for_persona.return_value = (
            [make_record(signatory=None)],
            1,
        )
        self.assertEqual(self.first_item()["signatories"], [])

    def test_empty_signatory_list_survives(self) -> None:
        self.repository.list_for_persona.return_value = (
            [make_record(signatory=[])],
            1,
        )
        self.assertEqual(self.first_item()["signatories"], [])

    def test_no_results_is_an_empty_list_not_an_error(self) -> None:
        self.repository.list_for_persona.return_value = ([], 0)
        payload = self.get_list().get_json()
        self.assertEqual(payload["data"]["circulars"], [])
        self.assertEqual(payload["pagination"]["total"], 0)

    def test_total_counts_the_whole_result_set_not_the_page(self) -> None:
        self.repository.list_for_persona.return_value = ([make_record()], 137)
        page = self.get_list("limit=10").get_json()["pagination"]
        self.assertEqual(page["total"], 137)


# ══════════════════════════════════════════════════════════════════════
# Pagination arithmetic
# ══════════════════════════════════════════════════════════════════════
class PersonaCircularsPaginationTest(PersonaCircularsRouteTestCase):
    def pagination_for(self, query: str, total: int) -> dict:
        self.repository.list_for_persona.return_value = ([], total)
        resp = self.get_list(query)
        self.assertEqual(resp.status_code, 200, resp.get_json())
        return resp.get_json()["pagination"]

    def test_first_page(self) -> None:
        page = self.pagination_for("limit=10&offset=0", total=25)
        self.assertEqual(page["limit"], 10)
        self.assertEqual(page["offset"], 0)
        self.assertEqual(page["total"], 25)
        self.assertTrue(page["hasNext"])
        self.assertFalse(page["hasPrev"])

    def test_middle_page(self) -> None:
        page = self.pagination_for("limit=10&offset=10", total=25)
        self.assertTrue(page["hasNext"])
        self.assertTrue(page["hasPrev"])

    def test_last_page_is_exclusive_at_the_boundary(self) -> None:
        # offset + limit == total, so there is no further page.
        page = self.pagination_for("limit=10&offset=20", total=25)
        self.assertFalse(page["hasNext"])
        self.assertTrue(page["hasPrev"])

    def test_single_page_has_neither_direction(self) -> None:
        page = self.pagination_for("limit=10&offset=0", total=3)
        self.assertFalse(page["hasNext"])
        self.assertFalse(page["hasPrev"])

    def test_exact_multiple_has_no_trailing_empty_page(self) -> None:
        page = self.pagination_for("limit=10&offset=20", total=20)
        self.assertFalse(page["hasNext"])
        self.assertTrue(page["hasPrev"])

    def test_empty_result_set_has_neither_direction(self) -> None:
        page = self.pagination_for("limit=10&offset=0", total=0)
        self.assertFalse(page["hasNext"])
        self.assertFalse(page["hasPrev"])

    def test_offset_beyond_the_end_is_still_accepted(self) -> None:
        # Out-of-range offsets are the caller's problem, not a 400 — the page
        # is simply empty and hasPrev stays true.
        page = self.pagination_for("limit=10&offset=500", total=25)
        self.assertFalse(page["hasNext"])
        self.assertTrue(page["hasPrev"])


# ══════════════════════════════════════════════════════════════════════
# Server-side invariants
# ══════════════════════════════════════════════════════════════════════
class PersonaCircularsInvariantTest(PersonaCircularsRouteTestCase):
    def test_is_active_and_status_are_not_caller_configurable(self) -> None:
        # The route must never forward these — the repository hard-codes
        # `is_active = TRUE AND status = 'FETCHED'`.
        resp = self.get_list("active_only=false&status=FAILED&status=DISCOVERED")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        kwargs = self.repo_kwargs()
        self.assertNotIn("active_only", kwargs)
        self.assertNotIn("status", kwargs)

    def test_only_the_documented_params_reach_the_repository(self) -> None:
        self.assertEqual(
            self.get_list("limit=5&offset=5&source=NSE").status_code, 200
        )
        self.assertEqual(
            set(self.repo_kwargs()),
            {"persona_slug", "sources", "from_date", "to_date", "limit", "offset"},
        )
