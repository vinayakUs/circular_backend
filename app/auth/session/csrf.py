"""Double-submit CSRF protection.

Once auth lives in cookies, any page on the internet can make the browser
POST to the API with those cookies attached. SameSite=Strict blocks that in
Chrome/Firefox/Edge but not in every browser, so the double-submit check is
the independent layer: the csrf_token cookie is readable by JS precisely so
the client can echo it back in X-CSRF-Token, and both values must agree.
"""
from __future__ import annotations

import secrets


CSRF_HEADER = "X-CSRF-Token"

UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


class CsrfTokenService:
    """Stateless: no storage, the token is the value itself."""

    def issue(self) -> str:
        return secrets.token_urlsafe(32)

    def validate(self, cookie_value: str | None, header_value: str | None) -> bool:
        """True iff both are present and equal. Constant-time compare."""
        if not cookie_value or not header_value:
            return False
        return secrets.compare_digest(cookie_value, header_value)

    @staticmethod
    def requires_csrf(method: str) -> bool:
        """Safe methods skip the check, so GET must stay side-effect free."""
        return method.upper() in UNSAFE_METHODS
