
"""
Persistence boundary for captcha_challenges.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(slots=True)
class CaptchaChallenge:
    id: UUID
    answer_hash: str
    created_at: datetime
    expires_at: datetime
    consumed: bool


class CaptchaChallengeRepository(ABC):
    @abstractmethod
    def create(
        self,
        *,
        answer_hash: str,
        ttl_seconds: int,
    ) -> CaptchaChallenge:
        """Repository abstraction (DIP). Concrete impl uses Postgres."""
        ...

    @abstractmethod
    def get_unconsumed(self, challenge_id: UUID) -> CaptchaChallenge | None:
        """Fetch the row iff it exists, is unconsumed, and not yet expired. Returns None otherwise."""
        ...

    @abstractmethod
    def consume(self, challenge_id: UUID) -> bool:
        """Atomically flip consumed=True. Returns True iff this call did the flip."""
        ...


    @abstractmethod
    def purge_expired(self) -> int: 
        ...
