"""TemplateRenderer — the Strategy interface for turning templates into payloads.

A TemplateRenderer takes a template name and a variables dict, and
produces a fully-rendered RenderedPayload (text + optional html +
optional subject) ready to be handed to a channel.

The renderer does NOT know about delivery. Channels do not know about
templates.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping

from app.services.notifications.models import RenderedPayload


class TemplateNotFound(LookupError):
    """Raised when the requested template does not exist."""

class TemplateRenderError(RuntimeError):
    """Raised when template rendering fails (syntax error, missing variable, etc)."""


class TemplateRenderer(ABC):
    """Strategy interface for rendering templates into RenderedPayload."""

    @abstractmethod
    def render(
        self,
        template_name: str,
        variables: Mapping[str, Any],
    ) -> RenderedPayload:
        """Render the named template with the given variables.

        Returns:
            A RenderedPayload with body_text (and optionally body_html
            and subject) populated.

        Raises:
            TemplateNotFound: if template_name does not exist.
            TemplateRenderError: if variable substitution fails.

        Note: renderers ARE allowed to raise. Rendering is a pure,
        side-effect-free step — it reads a template file and substitutes
        variables, nothing more. If it fails, no SMTP connection has
        been opened and no HTTP request has been made, so raising
        leaves no partial state behind. NotificationService catches
        these and returns DeliveryResult.fail.
        """
        ...
