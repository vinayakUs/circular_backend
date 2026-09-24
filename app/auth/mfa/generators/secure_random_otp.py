"""SecureRandomOtpGenerator — generates OTP codes using cryptographic randomness."""
from __future__ import annotations

import secrets

from app.auth.mfa.generators.base import OtpGenerator


class SecureRandomOtpGenerator(OtpGenerator):
    """Generate `length` random digits using `secrets.choice`.

    Cryptographically secure; not predictable from prior outputs.
    """

    _DIGITS = "0123456789"

    def generate(self, length: int) -> str:
        if length <= 0:
            raise ValueError("length must be positive")
        return "".join(secrets.choice(self._DIGITS) for _ in range(length))
