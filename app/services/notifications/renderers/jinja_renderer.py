"""JinjaTemplateRenderer — concrete TemplateRenderer that uses Jinja2.

Template dispatch by file extension:
    .txt  → render as plain text only (SMS, text-only emails)
    .html → render as HTML, derive plain text via html2text for multipart fallback

Templates live in the templates/ sub-package, loaded once and cached
in memory. Each call to render() reuses the cached compiled Template
object — only the variable substitution is redone.

Exception translation:
    jinja2.TemplateNotFound    → TemplateNotFound
    jinja2.TemplateError       → TemplateRenderError

Failures here happen before any SMTP or HTTP call — render is pure,
side-effect-free. The orchestrator (NotificationService) catches these.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import html2text
import jinja2
from jinja2 import Environment, FileSystemLoader, Template

from app.services.notifications.models import RenderedPayload
from app.services.notifications.renderers.base import (
    TemplateNotFound,
    TemplateRenderError,
    TemplateRenderer,
)


class JinjaTemplateRenderer(TemplateRenderer):
    """Renders Jinja2 templates from a directory on disk.

    Args:
        template_dir: directory containing the .html and .txt template files.
    """

    def __init__(self, template_dir: Path) -> None:
        self._env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=jinja2.select_autoescape(
                enabled_extensions=("html",),
                default_for_string=False,
            ),
            keep_trailing_newline=True,
        )
        self._cache: dict[str, Template] = {}
        self._html2text = html2text.HTML2Text()
        self._html2text.body_width = 0  # don't wrap long lines (SMS-friendly)

    def render(
        self,
        template_name: str,
        variables: Mapping[str, Any],
    ) -> RenderedPayload:
        """Render a Jinja2 template by name.

        Dispatch by extension:
            .txt → plain text only → RenderedPayload(body_text=...)
            .html → HTML + auto-derived plain text → RenderedPayload(
                body_text=..., body_html=...)

        Raises:
            TemplateNotFound: if the template file does not exist.
            TemplateRenderError: if the file extension is unsupported or
                variable substitution fails.
        """
        try:
            template = self._load(template_name)
        except jinja2.TemplateNotFound as e:
            raise TemplateNotFound(
                f"No template named {template_name!r}"
            ) from e

        try:
            rendered = template.render(**variables)
        except jinja2.TemplateError as e:
            raise TemplateRenderError(
                f"Template {template_name!r} failed to render: {e}"
            ) from e

        if template_name.endswith(".txt"):
            return RenderedPayload(body_text=rendered)

        if template_name.endswith(".html"):
            return RenderedPayload(
                body_text=self._html2text.handle(rendered),
                body_html=rendered,
            )

        raise TemplateRenderError(
            f"Template name must end in .html or .txt, got {template_name!r}"
        )

    def _load(self, template_name: str) -> Template:
        """Load and cache a template by name."""
        if template_name not in self._cache:
            self._cache[template_name] = self._env.get_template(template_name)
        return self._cache[template_name]
