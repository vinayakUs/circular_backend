from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel


class BaseLLMClient(ABC):
    @abstractmethod
    def create_completions_parallel(
        self,
        prompts: list[str],
        model: str,
        response_model: type[BaseModel],
        max_workers: int = 8,
        max_retries: int = 3,
    ) -> list[Any]:
        """Execute multiple chat completion calls in parallel.

        Args:
            prompts: List of prompt strings (one per call)
            model: Model name to use
            response_model: Pydantic model for structured response
            max_workers: Max concurrent calls (default 8)
            max_retries: Retries per call on failure (default 3)

        Returns:
            List of response objects (one per prompt, in same order).
            Failed calls return None.
        """
        ...
