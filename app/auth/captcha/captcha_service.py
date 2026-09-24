
import hashlib
import hmac
import logging
import threading
from typing import Callable
from uuid import UUID

from app.auth.captcha.renderers.alphanumeric_answer_generator import AlphanumericAnswerGenerator
from app.auth.captcha.renderers.base import AnswerGenerator, ChallengeRenderer
from app.auth.captcha.renderers.distorted_text import DistortedTextRenderer
from app.auth.captcha.repository.captcha_image_repository import CaptchaImageRepository
from app.auth.captcha.repository.captcha_repository import (
    CaptchaChallenge,
    CaptchaChallengeRepository,
)
from app.auth.captcha.repository.postgres_captcha_image_repository import PostgresCaptchaImageRepository
from app.auth.captcha.repository.postgres_captcha_repository import PostgresCaptchaChallengeRepository
from config import Config
from db.postgres_client import get_postgres_client

class CaptchaService:
    """Orchestrates challenge generation, rendering, and verification.

    Depends only on ABCs (DIP). Single responsibility: lifecycle of a
    captcha challenge — answer, hash, persist, render, persist image, verify.
    """

    def __init__(
        self,
        *,
        renderer: ChallengeRenderer,
        answer_gen: AnswerGenerator,
        repo: CaptchaChallengeRepository,
        image_repo: CaptchaImageRepository,
        hash_fn: Callable[[str], str] | None = None,
        compare_fn: Callable[[str, str], bool] | None = None,
        ttl_seconds: int = Config.CAPTCHA_TTL_SECONDS,
        image_width: int = Config.CAPTCHA_IMAGE_WIDTH,
        image_height: int = Config.CAPTCHA_IMAGE_HEIGHT,
        answer_length: int = Config.CAPTCHA_LENGTH,
    ):
        self._renderer = renderer
        self._answer_gen = answer_gen
        self._repo = repo
        self._image_repo = image_repo
        self._hash = hash_fn or (
            lambda s: hashlib.sha256(
                (s or "").strip().lower().encode()
            ).hexdigest()
        )
        self._compare = compare_fn or hmac.compare_digest
        self._ttl = ttl_seconds
        self._width = image_width
        self._height = image_height
        self._length = answer_length

        self.logger = logging.getLogger(__name__)


    def new_challenge(self) -> tuple[CaptchaChallenge, bytes]:
        """Generate a fresh challenge, persist, render PNG, persist image, return both."""
        answer = self._answer_gen.generate(self._length)
        answer_hash = self._hash(answer)
        challenge = self._repo.create(answer_hash=answer_hash, ttl_seconds=self._ttl)
        png_bytes = self._renderer.render(
            answer=answer,
            width=self._width,
            height=self._height
        )
        self._image_repo.create(
            challenge_id=challenge.id,
            png_bytes=png_bytes,
            content_type=self._renderer.content_type,
        )
        return challenge, png_bytes

    def verify(self, challenge_id: UUID, candidate: str) -> bool:
        row = self._repo.get_unconsumed(challenge_id=challenge_id)
        if row is None:
            return False
        candidate_hash = self._hash(candidate or "")
        if not self._compare(candidate_hash, row.answer_hash):
            return False # do not consume user can retry
        if not self._repo.consume(challenge_id):
            return False # lost race another req consumed first
        return True

    def image_bytes(self, challenge_id: UUID) -> tuple[bytes, str] | None:
        """Return ``(png_bytes, content_type)`` for the challenge, or ``None``.

        Note: this call does NOT check consumed or expiry — those are
        enforced at the *verify* side. Allowing the browser to fetch a
        previously-consumed captcha image is harmless (the verify path
        will reject it).

        The ``png_bytes`` value is normalised to :class:`bytes` because
        ``psycopg2`` returns BYTEA columns as :class:`memoryview` —
        callers (Flask routes) want a stable type.
        """
        result = self._image_repo.get(challenge_id)
        if result is None:
            return None
        png_bytes, content_type = result
        if not isinstance(png_bytes, bytes):
            png_bytes = bytes(png_bytes)
        return png_bytes, content_type


_service: CaptchaService | None = None
_service_lock = threading.Lock()

def make_captcha_service() -> CaptchaService:
    """
    Build a fully-wired CaptchaService.

    The single composition root — the only function that knows which
    concrete classes satisfy the collaborator slots.

    Returns a ready-to-use service.
    """
    pool = get_postgres_client().get_pool()

    return CaptchaService(
        renderer=DistortedTextRenderer(
            font_file=Config.CAPTCHA_FONT_FILE,
        ),
        answer_gen=AlphanumericAnswerGenerator(),
        repo=PostgresCaptchaChallengeRepository(
            db_pool=pool,
        ),
        image_repo=PostgresCaptchaImageRepository(
            db_pool=pool,
        ),
    )

def get_captcha_service() -> CaptchaService:
    """
    Lazy thread-safe singleton.
    """
    global _service

    if _service is None:
        with _service_lock:
            if _service is None:
                _service = make_captcha_service()

    return _service






