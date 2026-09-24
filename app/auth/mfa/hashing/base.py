"""OtpHasher — Strategy interface for hashing OTP codes.

Adds a server-side pepper so a DB leak doesn't immediately expose
valid OTP codes.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class OtpHasher(ABC):
    @abstractmethod
    def hash(self, code: str) -> str:
        """Return a deterministic hash of `code` (hex-encoded string).

        Implementations should use a server-side pepper (HMAC, etc.)
        so that DB-only leaks don't expose codes.
        """
        ...
