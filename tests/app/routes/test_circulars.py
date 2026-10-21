"""Controller tests for `app/routes/circulars.py`.

Structured after the Spring/JUnit conventions, translated to Flask:

    Spring                                Flask equivalent used here
    ────────────────────────────────────  ───────────────────────────────────
    mirrored src/test/java package tree   tests/app/routes/test_circulars.py
    @WebMvcTest (slice, no database)      bare Flask() + register_routes()
    MockMvc.perform(get(...))             app.test_client().get(...)
    @MockBean CircularRepository          patch.object(circulars, "CircularRepository")
    @BeforeEach setUp()                   setUp()
    status().isBadRequest()               assertEqual(resp.status_code, 400)

No database is touched. These tests cover the request/response boundary for
the two endpoints that take a `source` filter:

  * GET /api/circulars         — the results list
  * GET /api/circulars/lookup  — the Circular-No picker

Both must accept repeated ?source= params, reject the retired ALL sentinel,
and return an identical validation message. SQL behaviour is exercised
separately against a real database.

Run with:  pytest tests/app/routes/test_circulars.py
"""
from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from flask import Flask

from app.routes import circulars
from ingestion.repository import CircularRepository as RealCircularRepository
from utils.exchange_source import ExchangeSource

LIST_URL = "/api/circulars"
LOOKUP_URL = "/api/circulars/lookup?q=ns&field=full_reference"

#: The one validation message both endpoints must produce for a bad source.
BAD_SOURCE_ERROR = "source must be 'NSE', 'SEBI', 'SEBI_MASTER'."


class CircularsRouteTestCase(unittest.TestCase):
    """Base fixture — the @BeforeEach of every group below."""

    def setUp(self) -> None:
        app = Flask(__name__)
        # @WebMvcTest analogue: register only this route module, and skip
        # create_app() so the RAG warmup never runs. lookup_circulars never
        # touches the generator, so None is safe.
        circulars.register_routes(app, rag_generator=None)
        app.config.update(TESTING=True)
        self.client = app.test_client()

        patcher_db = patch.object(circulars, "get_postgres_client")
        patcher_repo = patch.object(circulars, "CircularRepository")
        self.addCleanup(patcher_db.stop)
        self.addCleanup(patcher_repo.stop)

        db_client = patcher_db.start()
        repository_cls = patcher_repo.start()
        db_client.return_value.get_pool.return_value = object()

        # The lookup route reads LOOKUP_FIELDS off the class to build its
        # error message. Left as a MagicMock it renders as `[]` and rejects
        # every field before source parsing is ever reached.
        repository_cls.LOOKUP_FIELDS = RealCircularRepository.LOOKUP_FIELDS

        self.repository = repository_cls.return_value
        self.repository.list_paginated.return_value = ([], 0)
        self.repository.lookup.return_value = []

    def get_list(self, query: str = ""):
        return self.client.get(f"{LIST_URL}?{query}")

    def get_lookup(self, query: str = ""):
        return self.client.get(f"{LOOKUP_URL}&{query}")

    def list_kwargs(self) -> dict:
        """What the list route handed to CircularRepository.list_paginated."""
        self.repository.list_paginated.assert_called_once()
        return self.repository.list_paginated.call_args.kwargs

    def lookup_kwargs(self) -> dict:
        """What the lookup route handed to CircularRepository.lookup."""
        self.repository.lookup.assert_called_once()
        return self.repository.lookup.call_args.kwargs

    def list_sources(self, query: str = ""):
        """Assert 200, then return the parsed sources from the list route."""
        resp = self.get_list(f"limit=5&{query}")
        self.assertEqual(resp.status_code, 200, resp.get_json())
        return self.list_kwargs()["sources"]

    def lookup_sources(self, query: str = ""):
        """Assert 200, then return the parsed sources from the lookup route."""
        resp = self.get_lookup(query)
        self.assertEqual(resp.status_code, 200, resp.get_json())
        return self.lookup_kwargs()["sources"]

    def assert_source_rejected(self, query: str) -> None:
        resp = self.get_list(query)
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()["error"], BAD_SOURCE_ERROR)
        self.repository.list_paginated.assert_not_called()

    def assert_lookup_source_rejected(self, query: str) -> None:
        resp = self.get_lookup(query)
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()["error"], BAD_SOURCE_ERROR)
        self.repository.lookup.assert_not_called()


# ══════════════════════════════════════════════════════════════════════
# GET /api/circulars — source parsing
# ══════════════════════════════════════════════════════════════════════
class ListSourceParsingTest(CircularsRouteTestCase):
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

    def test_repeated_identical_source_is_preserved(self) -> None:
        # No de-duplication: ANY() does not care and callers should not send
        # duplicates. Pinned so a future de-dup is a deliberate change.
        self.assertEqual(
            self.list_sources("source=NSE&source=NSE"),
            [ExchangeSource.NSE, ExchangeSource.NSE],
        )

    def test_many_repeated_values(self) -> None:
        query = "&".join(["source=NSE"] * 50)

        self.assertEqual(len(self.list_sources(query)), 50)


# ══════════════════════════════════════════════════════════════════════
# GET /api/circulars — source rejection
# ══════════════════════════════════════════════════════════════════════
class ListSourceRejectionTest(CircularsRouteTestCase):
    def test_all_sentinel_is_rejected(self) -> None:
        # "ALL" used to mean "no filter". An absent param already expresses
        # that, so the sentinel is retired and must not silently keep working.
        self.assert_source_rejected("source=ALL")

    def test_lowercase_all_is_rejected(self) -> None:
        self.assert_source_rejected("source=all")

    def test_unknown_source_is_rejected(self) -> None:
        self.assert_source_rejected("source=NONSENSE")

    def test_one_invalid_value_rejects_the_whole_query(self) -> None:
        self.assert_source_rejected("source=NSE&source=BOGUS")

    def test_rejection_precedes_the_repository_call(self) -> None:
        self.assert_source_rejected("source=NONSENSE")
        self.repository.list_paginated.assert_not_called()


# ══════════════════════════════════════════════════════════════════════
# GET /api/circulars — the other filters must be untouched
# ══════════════════════════════════════════════════════════════════════
class ListOtherFiltersTest(CircularsRouteTestCase):
    def test_dates_are_forwarded_as_dates(self) -> None:
        self.get_list("limit=5&from_date=2024-01-01&to_date=2024-12-31")
        kwargs = self.list_kwargs()

        self.assertEqual(kwargs["from_date"], date(2024, 1, 1))
        self.assertEqual(kwargs["to_date"], date(2024, 12, 31))

    def test_applicable_to_nse_false_is_forwarded(self) -> None:
        self.get_list("limit=5&applicable_to_nse=false")

        self.assertIs(self.list_kwargs()["applicable_to_nse"], False)

    def test_department_is_forwarded(self) -> None:
        self.get_list("limit=5&department=Listing")

        self.assertEqual(self.list_kwargs()["department"], "Listing")

    def test_repeated_signatory_is_forwarded_as_a_list(self) -> None:
        self.get_list("limit=5&signatory=A&signatory=B")

        self.assertEqual(self.list_kwargs()["signatory"], ["A", "B"])

    def test_status_limit_and_offset_are_forwarded(self) -> None:
        self.get_list("limit=7&offset=14&status=FETCHED")
        kwargs = self.list_kwargs()

        self.assertEqual(kwargs["status"], "FETCHED")
        self.assertEqual(kwargs["limit"], 7)
        self.assertEqual(kwargs["offset"], 14)

    def test_bad_status_is_still_rejected(self) -> None:
        self.assertEqual(self.get_list("source=NSE&status=BOGUS").status_code, 400)

    def test_bad_limit_is_still_rejected(self) -> None:
        self.assertEqual(self.get_list("source=NSE&limit=abc").status_code, 400)

    def test_negative_offset_is_clamped_to_zero(self) -> None:
        # Unlike a non-integer offset (rejected below), an out-of-range
        # integer is clamped rather than refused — same as limit.
        self.get_list("offset=-1")

        self.assertEqual(self.list_kwargs()["offset"], 0)

    def test_bad_offset_is_still_rejected(self) -> None:
        self.assertEqual(self.get_list("source=NSE&offset=abc").status_code, 400)

    def test_limit_is_clamped_to_100(self) -> None:
        self.get_list("limit=5000")

        self.assertEqual(self.list_kwargs()["limit"], 100)

    def test_limit_is_clamped_to_at_least_one(self) -> None:
        self.get_list("limit=0")

        self.assertEqual(self.list_kwargs()["limit"], 1)


# ══════════════════════════════════════════════════════════════════════
# GET /api/circulars/lookup — source parsing and rejection
# ══════════════════════════════════════════════════════════════════════
class LookupSourceTest(CircularsRouteTestCase):
    def test_absent_source_means_no_filter(self) -> None:
        self.assertIsNone(self.lookup_sources())

    def test_single_source_becomes_a_one_element_list(self) -> None:
        self.assertEqual(self.lookup_sources("source=NSE"), [ExchangeSource.NSE])

    def test_repeated_sources_are_all_collected(self) -> None:
        self.assertEqual(
            self.lookup_sources("source=NSE&source=SEBI"),
            [ExchangeSource.NSE, ExchangeSource.SEBI],
        )

    def test_source_is_uppercased(self) -> None:
        self.assertEqual(self.lookup_sources("source=sebi"), [ExchangeSource.SEBI])

    def test_surrounding_whitespace_is_stripped(self) -> None:
        self.assertEqual(
            self.lookup_sources("source=%20SEBI%20"), [ExchangeSource.SEBI]
        )

    def test_whitespace_only_source_is_treated_as_absent(self) -> None:
        self.assertIsNone(self.lookup_sources("source=%20"))

    def test_limit_is_always_100(self) -> None:
        self.lookup_sources()

        self.assertEqual(self.lookup_kwargs()["limit"], 100)

    def test_all_sentinel_is_rejected(self) -> None:
        self.assert_lookup_source_rejected("source=ALL")

    def test_unknown_source_is_rejected(self) -> None:
        self.assert_lookup_source_rejected("source=NONSENSE")

    def test_one_invalid_value_rejects_the_whole_query(self) -> None:
        self.assert_lookup_source_rejected("source=NSE&source=BOGUS")


# ══════════════════════════════════════════════════════════════════════
# GET /api/circulars/lookup — required params and other filters
# ══════════════════════════════════════════════════════════════════════
class LookupRequestTest(CircularsRouteTestCase):
    def test_missing_q_is_rejected(self) -> None:
        resp = self.client.get(f"{LOOKUP_URL.split('?')[0]}?field=full_reference")

        self.assertEqual(resp.status_code, 400)

    def test_blank_q_is_rejected(self) -> None:
        resp = self.client.get("/api/circulars/lookup?q=&field=full_reference")

        self.assertEqual(resp.status_code, 400)

    def test_missing_field_is_rejected(self) -> None:
        resp = self.client.get("/api/circulars/lookup?q=ns")

        self.assertEqual(resp.status_code, 400)

    def test_unknown_field_is_rejected(self) -> None:
        resp = self.client.get("/api/circulars/lookup?q=ns&field=bogus")

        self.assertEqual(resp.status_code, 400)
        # The message must enumerate the real fields.
        self.assertIn("full_reference", resp.get_json()["error"])

    def test_dates_are_forwarded_as_dates(self) -> None:
        self.lookup_sources("from_date=2024-01-01&to_date=2024-12-31")
        kwargs = self.lookup_kwargs()

        self.assertEqual(kwargs["from_date"], date(2024, 1, 1))
        self.assertEqual(kwargs["to_date"], date(2024, 12, 31))

    def test_malformed_from_date_is_rejected(self) -> None:
        resp = self.get_lookup("from_date=15-01-2024")

        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()["error"], "from_date must be YYYY-MM-DD")

    def test_malformed_to_date_is_rejected(self) -> None:
        resp = self.get_lookup("to_date=nope")

        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()["error"], "to_date must be YYYY-MM-DD")

    def test_applicable_to_nse_false_is_forwarded(self) -> None:
        self.lookup_sources("applicable_to_nse=false")

        self.assertIs(self.lookup_kwargs()["applicable_to_nse"], False)

    def test_applicable_to_nse_is_case_insensitive(self) -> None:
        self.lookup_sources("applicable_to_nse=TRUE")

        self.assertIs(self.lookup_kwargs()["applicable_to_nse"], True)

    def test_malformed_applicable_to_nse_is_rejected(self) -> None:
        resp = self.get_lookup("applicable_to_nse=maybe")

        self.assertEqual(resp.status_code, 400)
        self.assertEqual(
            resp.get_json()["error"],
            "applicable_to_nse must be 'true' or 'false'.",
        )

    def test_department_is_forwarded_and_stripped(self) -> None:
        self.lookup_sources("department=%20Listing%20")

        self.assertEqual(self.lookup_kwargs()["department"], "Listing")


# ══════════════════════════════════════════════════════════════════════
# GET /api/circulars/lookup — response shape
# ══════════════════════════════════════════════════════════════════════
class LookupResponseShapeTest(CircularsRouteTestCase):
    def test_filters_echo_reflects_what_was_applied(self) -> None:
        resp = self.get_lookup(
            "source=NSE&source=SEBI&from_date=2024-01-01&to_date=2024-12-31"
            "&applicable_to_nse=true&department=Listing"
        )

        self.assertEqual(
            resp.get_json()["filters"],
            {
                "sources": ["NSE", "SEBI"],
                "from_date": "2024-01-01",
                "to_date": "2024-12-31",
                "applicable_to_nse": True,
                "department": "Listing",
            },
        )

    def test_filters_echo_is_all_none_when_unfiltered(self) -> None:
        resp = self.get_lookup()

        self.assertEqual(
            resp.get_json()["filters"],
            {
                "sources": None,
                "from_date": None,
                "to_date": None,
                "applicable_to_nse": None,
                "department": None,
            },
        )

    def test_existing_response_keys_are_unchanged(self) -> None:
        body = self.get_lookup().get_json()

        self.assertEqual(body["query"], "ns")
        self.assertEqual(body["field"], "full_reference")
        self.assertEqual(body["count"], 0)
        self.assertEqual(body["matches"], [])

    def test_matches_are_serialised_with_camelCase_keys(self) -> None:
        self.repository.lookup.return_value = [
            {"id": "abc", "matched_field": "full_reference", "matched_value": "NSE/1"}
        ]
        body = self.get_lookup().get_json()

        self.assertEqual(
            body["matches"],
            [{"id": "abc", "matchedField": "full_reference", "matchedValue": "NSE/1"}],
        )
        self.assertEqual(body["count"], 1)


# ══════════════════════════════════════════════════════════════════════
# The two endpoints must not drift apart again
# ══════════════════════════════════════════════════════════════════════
class SourceEndpointsAgreeTest(CircularsRouteTestCase):
    def test_both_reject_the_all_sentinel(self) -> None:
        self.assertEqual(self.get_list("source=ALL").status_code, 400)
        self.assertEqual(self.get_lookup("source=ALL").status_code, 400)

    def test_both_reject_invalid_with_an_identical_message(self) -> None:
        list_error = self.get_list("source=BOGUS").get_json()["error"]
        lookup_error = self.get_lookup("source=BOGUS").get_json()["error"]

        self.assertEqual(list_error, lookup_error)

    def test_both_parse_the_same_query_identically(self) -> None:
        for query in ("source=NSE", "source=nse&source=SEBI", "source=%20NSE%20"):
            with self.subTest(query=query):
                self.repository.reset_mock()
                list_resp = self.get_list(f"limit=5&{query}")
                from_list = self.list_kwargs()["sources"]

                self.repository.reset_mock()
                lookup_resp = self.get_lookup(query)
                from_lookup = self.lookup_kwargs()["sources"]

                self.assertEqual(list_resp.status_code, 200)
                self.assertEqual(lookup_resp.status_code, 200)
                self.assertEqual(from_list, from_lookup)


if __name__ == "__main__":
    unittest.main()