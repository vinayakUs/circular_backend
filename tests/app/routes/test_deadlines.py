"""Controller tests for the deadlines endpoints.

    GET /api/deadlines
    GET /api/personas/<slug>/deadlines

Same harness as `test_personas.py`: a bare `Flask()` plus
`register_routes()` so `create_app()` (and its RAG warmup) never runs, with
`patch.object` standing in for the database. No database is touched — SQL
behaviour is exercised in `tests/ingestion/repository/`.

The load-bearing contract under test: OMITTING `limit` means no pagination
— the repo is called with limit=None and reports every match in the date
range — while an explicit positive limit opts back into paging. Both
endpoints must agree, per their "same shape" contract.

Run with:  pytest tests/app/routes/test_deadlines.py
"""
from __future__ import annotations

import unittest
from datetime import date, timedelta
from unittest.mock import patch
from uuid import UUID

from flask import Flask

from app.routes import deadlines, personas
from app.services.personas import Persona

ITEM_ID = UUID("11111111-1111-1111-1111-111111111111")
CIRC_ID = UUID("22222222-2222-2222-2222-222222222222")


def make_row(**overrides) -> tuple:
    """A repo result row: (ai.id, deadline, action_item, source, c.id, title)."""
    defaults = dict(
        item_id=ITEM_ID,
        deadline=date(2026, 11, 12),
        action_item="Submit revised rights-issue timeline",
        source="NSE",
        circ_id=CIRC_ID,
        circ_title="Rights issue framework",
    )
    defaults.update(overrides)
    return (
        defaults["item_id"],
        defaults["deadline"],
        defaults["action_item"],
        defaults["source"],
        defaults["circ_id"],
        defaults["circ_title"],
    )


# ══════════════════════════════════════════════════════════════════════
# GET /api/deadlines — cross-persona
# ══════════════════════════════════════════════════════════════════════
class GlobalDeadlinesRouteTestCase(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        deadlines.register_routes(app)
        app.config.update(TESTING=True)
        self.client = app.test_client()

        for target in ("get_postgres_client", "ActionItemRepository"):
            patcher = patch.object(deadlines, target)
            self.addCleanup(patcher.stop)
            setattr(self, f"_{target}_patcher", patcher)

        db_client = self._get_postgres_client_patcher.start()
        repository_cls = self._ActionItemRepository_patcher.start()

        db_client.return_value.get_pool.return_value = object()
        self.repository = repository_cls.return_value

    # ── helpers ─────────────────────────────────────────────────
    def get_list(self, query: str = ""):
        return self.client.get(f"/api/deadlines{('?' + query) if query else ''}")

    def repo_kwargs(self) -> dict:
        """What the route handed to ActionItemRepository.get_deadlines."""
        self.repository.get_deadlines.assert_called_once()
        return self.repository.get_deadlines.call_args.kwargs

    def configure_rows(self, rows: list, total: int | None = None) -> None:
        """Two calls (COUNT + SELECT) when paginated; one when unbounded."""
        self.repository.get_deadlines.return_value = (
            rows,
            total if total is not None else len(rows),
        )


class GlobalDeadlinesPaginationTest(GlobalDeadlinesRouteTestCase):
    def test_omitting_limit_means_no_pagination(self) -> None:
        self.configure_rows([make_row()])
        resp = self.get_list()
        self.assertEqual(resp.status_code, 200, resp.get_json())
        self.assertIsNone(self.repo_kwargs()["limit"])
        self.assertEqual(self.repo_kwargs()["offset"], 0)

    def test_explicit_limit_opts_into_paging(self) -> None:
        self.configure_rows([make_row()], total=250)
        resp = self.get_list("limit=25&offset=50")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        self.assertEqual(self.repo_kwargs()["limit"], 25)
        self.assertEqual(self.repo_kwargs()["offset"], 50)

    def test_zero_and_negative_limits_are_rejected(self) -> None:
        for query in ("limit=0", "limit=-3"):
            resp = self.get_list(query)
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(
                resp.get_json()["error"],
                "limit must be an integer of at least 1",
            )
            self.repository.get_deadlines.assert_not_called()

    def test_non_integer_limit_is_rejected(self) -> None:
        resp = self.get_list("limit=five")
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(
            resp.get_json()["error"], "limit and offset must be integers"
        )
        self.repository.get_deadlines.assert_not_called()

    def test_offset_without_limit_is_still_accepted(self) -> None:
        # A bare offset shifts the unbounded window: rows after `offset`
        # are all returned, and no COUNT runs on the repo's behalf.
        self.configure_rows([make_row()])
        resp = self.get_list("offset=10")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        self.assertIsNone(self.repo_kwargs()["limit"])
        self.assertEqual(self.repo_kwargs()["offset"], 10)

    def test_negative_offset_is_clamped_to_zero(self) -> None:
        self.configure_rows([])
        resp = self.get_list("offset=-40")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        self.assertEqual(self.repo_kwargs()["offset"], 0)

    def test_non_integer_offset_is_rejected(self) -> None:
        resp = self.get_list("offset=2.5")
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(
            resp.get_json()["error"], "limit and offset must be integers"
        )
        self.repository.get_deadlines.assert_not_called()

    def test_blank_limit_and_offset_count_as_absent(self) -> None:
        self.configure_rows([])
        resp = self.get_list("limit=&offset=")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        self.assertIsNone(self.repo_kwargs()["limit"])
        self.assertEqual(self.repo_kwargs()["offset"], 0)


class GlobalDeadlinesDateTest(GlobalDeadlinesRouteTestCase):
    def test_omitted_dates_default_to_the_next_sixty_days(self) -> None:
        self.configure_rows([])
        self.assertEqual(self.get_list().status_code, 200)
        kwargs = self.repo_kwargs()
        today = date.today()
        self.assertEqual(kwargs["from_date"], today)
        self.assertEqual(kwargs["to_date"], today + timedelta(days=60))

    def test_explicit_dates_are_forwarded_parsed(self) -> None:
        self.configure_rows([])
        resp = self.get_list("from=2026-01-01&to=2026-06-30")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        kwargs = self.repo_kwargs()
        self.assertEqual(kwargs["from_date"], date(2026, 1, 1))
        self.assertEqual(kwargs["to_date"], date(2026, 6, 30))

    def test_to_before_from_is_rejected(self) -> None:
        resp = self.get_list("from=2026-10-01&to=2026-09-01")
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()["error"], "to must be >= from")
        self.repository.get_deadlines.assert_not_called()

    def test_malformed_from_is_rejected(self) -> None:
        resp = self.get_list("from=15-10-2026")
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(
            resp.get_json()["error"], "from must be YYYY-MM-DD, got '15-10-2026'"
        )

    def test_malformed_to_is_rejected(self) -> None:
        resp = self.get_list("to=not-a-date")
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(
            resp.get_json()["error"], "to must be YYYY-MM-DD, got 'not-a-date'"
        )


class GlobalDeadlinesExchangeTest(GlobalDeadlinesRouteTestCase):
    def exchange_sent(self, query: str):
        self.configure_rows([])
        resp = self.get_list(query)
        self.assertEqual(resp.status_code, 200, resp.get_json())
        return self.repo_kwargs()["exchange_csv"]

    def test_csv_is_uppercased_and_whitespace_stripped(self) -> None:
        self.assertEqual(self.exchange_sent("exchange=%20nse%2C%20sebi%20"), "NSE,SEBI")

    def test_absent_exchange_means_no_filter(self) -> None:
        self.assertIsNone(self.exchange_sent(""))

    def test_blank_exchange_means_no_filter(self) -> None:
        self.assertIsNone(self.exchange_sent("exchange="))


class GlobalDeadlinesResponseTest(GlobalDeadlinesRouteTestCase):
    def test_unpaginated_envelope_is_honest(self) -> None:
        rows = [make_row(), make_row(action_item="Second action")]
        self.configure_rows(rows)
        payload = self.get_list().get_json()
        # No limit requested → echo null, and total == len(items): the list
        # IS the whole result set, so the UI can count it directly.
        self.assertEqual(
            set(payload), {"items", "total", "limit", "offset"}
        )
        self.assertIsNone(payload["limit"])
        self.assertEqual(payload["offset"], 0)
        self.assertEqual(payload["total"], len(rows))
        self.assertEqual(len(payload["items"]), len(rows))

    def test_paged_envelope_echoes_limit_and_offset(self) -> None:
        self.configure_rows([make_row()], total=137)
        payload = self.get_list("limit=10").get_json()
        self.assertEqual(payload["limit"], 10)
        self.assertEqual(payload["total"], 137)

    def test_items_are_serialised(self) -> None:
        self.configure_rows([make_row()])
        item = self.get_list().get_json()["items"][0]
        self.assertEqual(item["id"], str(ITEM_ID))
        self.assertEqual(item["date"], "2026-11-12")
        self.assertEqual(item["title"], "Submit revised rights-issue timeline")
        self.assertEqual(item["exchange"], "NSE")
        self.assertEqual(item["url"], f"/circular/{CIRC_ID}")
        self.assertEqual(item["source_circular_id"], str(CIRC_ID))
        self.assertEqual(item["circular_title"], "Rights issue framework")

    def test_empty_result_is_an_empty_list_not_an_error(self) -> None:
        self.configure_rows([])
        payload = self.get_list().get_json()
        self.assertEqual(payload["items"], [])
        self.assertEqual(payload["total"], 0)

    def test_repo_value_error_is_a_400(self) -> None:
        self.repository.get_deadlines.side_effect = ValueError("boom")
        resp = self.get_list()
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()["error"], "boom")


# ══════════════════════════════════════════════════════════════════════
# GET /api/personas/<slug>/deadlines
# ══════════════════════════════════════════════════════════════════════
class PersonaDeadlinesRouteTestCase(unittest.TestCase):
    SLUG = "compliance-officer"

    def setUp(self) -> None:
        app = Flask(__name__)
        personas.register_routes(app)
        app.config.update(TESTING=True)
        self.client = app.test_client()

        for target in (
            "get_postgres_client",
            "ActionItemRepository",
            "get_personas",
        ):
            patcher = patch.object(personas, target)
            self.addCleanup(patcher.stop)
            setattr(self, f"_{target}_patcher", patcher)

        db_client = self._get_postgres_client_patcher.start()
        repository_cls = self._ActionItemRepository_patcher.start()
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

    # ── helpers ─────────────────────────────────────────────────
    def get_list(self, query: str = "", slug: str | None = None):
        base = f"/api/personas/{slug or self.SLUG}/deadlines"
        return self.client.get(f"{base}{('?' + query) if query else ''}")

    def repo_kwargs(self) -> dict:
        self.repository.get_deadlines.assert_called_once()
        return self.repository.get_deadlines.call_args.kwargs

    def configure_rows(self, rows: list, total: int | None = None) -> None:
        self.repository.get_deadlines.return_value = (
            rows,
            total if total is not None else len(rows),
        )


class PersonaDeadlinesValidationTest(PersonaDeadlinesRouteTestCase):
    def test_unknown_persona_is_rejected_first(self) -> None:
        resp = self.get_list("limit=many", slug="nope")
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.get_json()["error"], "Unknown persona: 'nope'")
        self.repository.get_deadlines.assert_not_called()

    def test_zero_limit_is_rejected(self) -> None:
        resp = self.get_list("limit=0")
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(
            resp.get_json()["error"], "limit must be an integer of at least 1"
        )
        self.repository.get_deadlines.assert_not_called()

    def test_non_integer_limit_is_rejected(self) -> None:
        resp = self.get_list("limit=many")
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(
            resp.get_json()["error"], "limit and offset must be integers"
        )

    def test_to_before_from_is_rejected(self) -> None:
        resp = self.get_list("from=2026-10-01&to=2026-09-01")
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()["error"], "to must be >= from")
        self.repository.get_deadlines.assert_not_called()


class PersonaDeadlinesPaginationTest(PersonaDeadlinesRouteTestCase):
    def test_omitting_limit_means_no_pagination(self) -> None:
        self.configure_rows([make_row()])
        resp = self.get_list()
        self.assertEqual(resp.status_code, 200, resp.get_json())
        self.assertIsNone(self.repo_kwargs()["limit"])
        # The persona slug still scopes the query.
        self.assertEqual(self.repo_kwargs()["persona_slug"], self.SLUG)

    def test_explicit_limit_opts_into_paging(self) -> None:
        self.configure_rows([], total=250)
        resp = self.get_list("limit=25&offset=50")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        self.assertEqual(self.repo_kwargs()["limit"], 25)
        self.assertEqual(self.repo_kwargs()["offset"], 50)

    def test_negative_offset_is_clamped_to_zero(self) -> None:
        self.configure_rows([])
        resp = self.get_list("offset=-1")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        self.assertEqual(self.repo_kwargs()["offset"], 0)


class PersonaDeadlinesDateTest(PersonaDeadlinesRouteTestCase):
    def test_omitted_dates_default_to_a_one_year_window(self) -> None:
        # Documented window: the past 30 days plus the coming ~11 months —
        # intentionally wider than /api/deadlines' next-60-days default.
        # The two endpoints agree on `from` semantics ONLY when dates are
        # explicit; bare calls may differ, so always pass dates when
        # cross-endpoint consistency matters.
        self.configure_rows([])
        self.assertEqual(self.get_list().status_code, 200)
        kwargs = self.repo_kwargs()
        today = date.today()
        self.assertEqual(kwargs["from_date"], today - timedelta(days=30))
        self.assertEqual(kwargs["to_date"], today + timedelta(days=335))

    def test_explicit_dates_are_forwarded_parsed(self) -> None:
        self.configure_rows([])
        resp = self.get_list("from=2026-01-01&to=2026-06-30")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        kwargs = self.repo_kwargs()
        self.assertEqual(kwargs["from_date"], date(2026, 1, 1))
        self.assertEqual(kwargs["to_date"], date(2026, 6, 30))


class PersonaDeadlinesResponseTest(PersonaDeadlinesRouteTestCase):
    def test_envelope_includes_persona_slug(self) -> None:
        self.configure_rows([make_row()])
        payload = self.get_list().get_json()
        self.assertEqual(
            set(payload),
            {"persona_slug", "items", "total", "limit", "offset"},
        )
        self.assertEqual(payload["persona_slug"], self.SLUG)
        self.assertIsNone(payload["limit"])
        self.assertEqual(payload["total"], 1)

    def test_paged_envelope_echoes_limit_and_offset(self) -> None:
        self.configure_rows([make_row()], total=137)
        payload = self.get_list("limit=10&offset=20").get_json()
        self.assertEqual(payload["limit"], 10)
        self.assertEqual(payload["offset"], 20)
        self.assertEqual(payload["total"], 137)

    def test_repo_value_error_is_a_400(self) -> None:
        self.repository.get_deadlines.side_effect = ValueError("boom")
        resp = self.get_list()
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()["error"], "boom")
