"""Route tests for `app/routes/search.py`. Slice tests: no LLM, no ES, no database."""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from flask import Flask

from app.routes import search

URL = "/api/search"


class SearchRouteTestCase(unittest.TestCase):
    def setUp(self) -> None:
        app = Flask(__name__)
        search.register_routes(app)
        app.config.update(TESTING=True)
        self.client = app.test_client()

        patcher = patch.object(search, "get_engine")
        self.addCleanup(patcher.stop)
        self.get_engine = patcher.start()
        self.engine = MagicMock()
        self.get_engine.return_value = self.engine
        self.engine.run.return_value = {
            "plan": {"intent": "SEARCH", "source": "regex"},
            "response": {"mode": "SEARCH", "understood_as": "Search for margin framework"},
            "route": "run_query",
            "steps": 0,
        }

    def post(self, payload):
        return self.client.post(URL, json=payload)

    def test_returns_plan_and_response(self):
        resp = self.post({"q": "margin framework"})
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertEqual(set(body), {"plan", "response", "route", "steps"})
        self.assertEqual(body["route"], "run_query")
        self.engine.run.assert_called_once_with("margin framework", tags=["api"])

    def test_missing_q_is_400(self):
        resp = self.post({})
        self.assertEqual(resp.status_code, 400)

    def test_blank_q_is_400(self):
        resp = self.post({"q": "   "})
        self.assertEqual(resp.status_code, 400)
        self.engine.run.assert_not_called()

    def test_summaries_flag_forwarded(self):
        self.post({"q": "x", "summaries": True})
        self.get_engine.assert_called_once_with(include_summaries=True)

    def test_summaries_defaults_false(self):
        self.post({"q": "x"})
        self.get_engine.assert_called_once_with(include_summaries=False)

    def test_engine_unavailable_is_503(self):
        self.engine.run.side_effect = RuntimeError("ANTHROPIC_API_KEY is not set")
        resp = self.post({"q": "x"})
        self.assertEqual(resp.status_code, 503)

    def test_unexpected_error_is_500(self):
        self.engine.run.side_effect = ValueError("boom")
        resp = self.post({"q": "x"})
        self.assertEqual(resp.status_code, 500)
