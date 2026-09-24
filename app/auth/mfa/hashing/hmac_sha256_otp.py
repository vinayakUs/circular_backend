"""HmacSha256OtpHasher — HMAC-SHA256 of OTP code with a server-side pepper."""
from __future__ import annotations

import hmac
from hashlib import sha256

from app.auth.mfa.hashing.base import OtpHasher


class HmacSha256OtpHasher(OtpHasher):
    """Hashes OTP codes with HMAC-SHA256 using a server-side pepper.

    The pepper is loaded from env (Config.OTP_PEPPER_SECRET) and never
    persisted. Rotating the pepper invalidates all outstanding OTPs —
    acceptable since OTPs are short-lived (5 min).
    """

    def __init__(self, *, pepper: bytes) -> None:
        if not pepper:
            raise ValueError("HmacSha256OtpHasher requires a non-empty pepper")
        self._pepper = pepper

    def hash(self, code: str) -> str:
        return hmac.new(self._pepper, code.encode("utf-8"), sha256).hexdigest()
