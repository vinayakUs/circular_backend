

from abc import ABC, abstractmethod
from uuid import UUID


class CaptchaImageRepository(ABC):
    """Repository for rendered CAPTCHA image bytes.

    Images are keyed by challenge ID and cascade-delete with the challenge.
    """

    @abstractmethod
    def create(
        self,
        *,
        challenge_id: UUID,
        png_bytes: bytes,
        content_type: str,
    ) -> None:
        """Persist image bytes for an existing CAPTCHA challenge."""
        ...

    @abstractmethod
    def get(self, challenge_id: UUID) -> tuple[bytes, str] | None:
        """Return image bytes and content type, or None if not found."""
        ...

    @abstractmethod
    def delete(self, challenge_id: UUID) -> bool:
        """Delete the image for a challenge and return whether it existed."""
        ...