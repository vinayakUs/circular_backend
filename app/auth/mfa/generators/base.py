"""OtpGenerator — Strategy interface for generating OTP codes.

The MFA service depends on this ABC; the concrete implementation can
be swapped (random digits, time-based TOTP, etc.) without changing
callers.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class OtpGenerator(ABC):
    @abstractmethod
    def generate(self, length: int) -> str:
        """Return a string of `length` characters representing an OTP code.

        Implementations MUST be cryptographically secure (use `secrets`).
        Returns digits by convention, but any format is fine as long as
        the matching OtpHasher accepts the same shape.
        """
        ...
