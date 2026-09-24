

import string
from typing import override

from app.auth.captcha.renderers.base import AnswerGenerator
import secrets

# Confusable pairs that produce wrong-but-not-evil user mistakes.
_EXCLUDED = frozenset("0O1lI")

class AlphanumericAnswerGenerator(AnswerGenerator):

    # 30-character alphabet computed once at class-load time.
    _ALPHABET: str = "".join(
        c for c in string.ascii_uppercase + string.digits
        if c not in _EXCLUDED
    )

    @override
    def generate(self, length: int) -> str:

        if length <= 0:
            raise ValueError("length must be positive")
        return "".join(
            secrets.choice(self._ALPHABET)
            for _ in range(length)
        )
