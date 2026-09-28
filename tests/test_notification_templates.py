"""Tests for the new subsystem's JinjaTemplateRenderer loading the
circular and mention notification templates from its own templates dir.

Verifies the templates copied from services/templates/ render with the
expected variable shapes (matches what the worker will pass).
"""
from __future__ import annotations

import unittest
from pathlib import Path

from app.services.notifications.renderers.base import TemplateNotFound
from app.services.notifications.renderers.jinja_renderer import (
    JinjaTemplateRenderer,
)


TEMPLATE_DIR = Path(__file__).parent.parent / "app" / "services" / "notifications" / "templates"


class CircularTemplateRenderingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.renderer = JinjaTemplateRenderer(template_dir=TEMPLATE_DIR)

    def test_renders_circular_template_with_all_variables(self):
        rendered = self.renderer.render(
            "circular_notification.html",
            {
                "full_reference": "SEBI/HO/MRD/2024/123",
                "title": "Margin framework update",
                "department": "Markets",
                "issue_date": "2024-09-28",
                "source": "SEBI",
                "url": "https://example.com/circulars/abc",
                "applicable_to_nse": True,
                "summary_html": "<p>AI summary here</p>",
                "summary_pending": False,
            },
        )
        self.assertIn("SEBI/HO/MRD/2024/123", rendered.body_html)
        self.assertIn("Margin framework update", rendered.body_html)
        self.assertIn("Markets", rendered.body_html)
        self.assertIn("Applicable to NSE", rendered.body_html)
        self.assertIn("AI summary here", rendered.body_html)
        # body_text is auto-derived from body_html
        self.assertIn("SEBI/HO/MRD/2024/123", rendered.body_text)

    def test_renders_circular_template_without_summary(self):
        rendered = self.renderer.render(
            "circular_notification.html",
            {
                "full_reference": "NSE/2024/1",
                "title": "Circular without summary",
                "department": "Surveillance",
                "issue_date": "2024-09-28",
                "source": "NSE",
                "url": "https://example.com/circulars/xyz",
                "applicable_to_nse": False,
                "summary_html": None,
                "summary_pending": True,
            },
        )
        self.assertIn("NSE/2024/1", rendered.body_html)
        self.assertIn("Not Applicable to NSE", rendered.body_html)
        self.assertIn("AI generated summary pending", rendered.body_html)
        # Should NOT have summary content
        self.assertNotIn("AI summary here", rendered.body_html)

    def test_renders_circular_template_with_empty_optional_fields(self):
        # Worst-case rendering: no summary at all (None + False).
        rendered = self.renderer.render(
            "circular_notification.html",
            {
                "full_reference": "X",
                "title": "X",
                "department": "X",
                "issue_date": "X",
                "source": "X",
                "url": "https://x",
                "applicable_to_nse": False,
                "summary_html": None,
                "summary_pending": False,
            },
        )
        self.assertIn("Not Applicable to NSE", rendered.body_html)
        # No summary content block and no pending banner.
        # Note: there's an HTML comment in the template that contains
        # "AI Generated Summary" as part of its text — that's expected
        # to remain (it's just a comment). The actual rendered summary
        # block uses an <h3> with that text, which only renders when
        # summary_html is truthy.
        self.assertNotIn("<h3", rendered.body_html.split("<!-- AI Generated")[1])


class MentionTemplateRenderingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.renderer = JinjaTemplateRenderer(template_dir=TEMPLATE_DIR)

    def test_renders_mention_template_with_all_variables(self):
        rendered = self.renderer.render(
            "mention_notification.html",
            {
                "mentioned_by_user_id": "alice",
                "mentioned_by_name": "Alice Smith",
                "target_label": "@bob",
                "comment_text": "Hey @bob, can you review this circular?",
                "expert_name": "Compliance Review",
                "comment_url": "https://example.com/taskview?id=abc&expertId=1#comment-42",
                "comment_created_at": "28 Sep 2024, 14:30",
                "circular_title": "Margin framework update",
                "circular_full_reference": "SEBI/HO/MRD/2024/123",
            },
        )
        self.assertIn("@alice", rendered.body_html)
        self.assertIn("Alice Smith", rendered.body_html)
        self.assertIn("Hey @bob, can you review this circular?", rendered.body_html)
        self.assertIn("Compliance Review", rendered.body_html)
        self.assertIn("Margin framework update", rendered.body_html)
        self.assertIn("SEBI/HO/MRD/2024/123", rendered.body_html)
        self.assertIn("View Comment", rendered.body_html)

    def test_renders_mention_template_without_mentioned_by_name(self):
        # mentioned_by_name is optional
        rendered = self.renderer.render(
            "mention_notification.html",
            {
                "mentioned_by_user_id": "alice",
                "mentioned_by_name": None,
                "target_label": "@bob",
                "comment_text": "Mention without display name",
                "expert_name": "X",
                "comment_url": "https://x",
                "comment_created_at": "today",
                "circular_title": "X",
                "circular_full_reference": "X",
            },
        )
        self.assertIn("@alice", rendered.body_html)
        self.assertIn("Mention without display name", rendered.body_html)


class TemplateLoadingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.renderer = JinjaTemplateRenderer(template_dir=TEMPLATE_DIR)

    def test_raises_template_not_found_for_missing_template(self):
        with self.assertRaises(TemplateNotFound):
            self.renderer.render(
                "does_not_exist.html",
                {"any": "value"},
            )

    def test_renderer_finds_circular_and_mention_templates(self):
        # Smoke test: both templates are loadable.
        for name in ("circular_notification.html", "mention_notification.html"):
            with self.subTest(template=name):
                payload = self.renderer.render(
                    name,
                    {
                        # minimal valid variables
                        "full_reference": "X", "title": "X",
                        "department": "X", "issue_date": "X",
                        "source": "X", "url": "https://x",
                        "applicable_to_nse": False,
                        "summary_html": None, "summary_pending": False,
                        "mentioned_by_user_id": "x", "mentioned_by_name": None,
                        "target_label": "x", "comment_text": "x",
                        "expert_name": "x", "comment_url": "https://x",
                        "comment_created_at": "x",
                        "circular_title": "x", "circular_full_reference": "x",
                    },
                )
                self.assertTrue(payload.body_html)


if __name__ == "__main__":
    unittest.main()