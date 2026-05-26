"""MiniMax LLM client wrapper with semaphore-based concurrency limiting and instructor support."""

from __future__ import annotations

import logging
import threading
from typing import Any

import instructor
from openai import OpenAI
from pydantic import BaseModel

from config import Config


logger = logging.getLogger(__name__)


class LLMProviderPool:
    """Thread-safe MiniMax LLM client with semaphore-concurrency control.

    Uses MiniMax API which properly supports instructor structured output.
    The semaphore enforces a global max-concurrent limit.
    """

    def __init__(self, max_concurrent: int = 3):
        self._client: instructor.Instructor | None = None
        self._lock = threading.Lock()
        self._semaphore = threading.Semaphore(max_concurrent)
        self._max_concurrent = max_concurrent
        logger.info("LLMProviderPool initialized: max_concurrent=%d", max_concurrent)

    @property
    def max_concurrent(self) -> int:
        return self._max_concurrent

    def _get_client(self) -> instructor.Instructor:
        if self._client is None:
            with self._lock:
                if self._client is None:
                    api_key = Config.MINMAX_API_KEY
                    if not api_key:
                        raise ValueError("MINMAX_API_KEY is not set")

                    openai_client = OpenAI(
                        base_url=Config.MINMAX_BASE_URL,
                        api_key=api_key,
                    )
                    self._client = instructor.from_openai(
                        openai_client, mode=instructor.Mode.JSON
                    )
        return self._client

    def create_completion(
        self,
        prompt: str,
        model: str = "minimaxai/minimax-m2.7",
        response_model: type[BaseModel] | None = None,
        timeout: int = 30,
        max_retries: int = 3,
    ) -> Any:
        """Make a single LLM call, blocking until a slot is available."""
        with self._semaphore:
            logger.info("LLMProviderPool: acquired slot (timeout=%d)", timeout)
            return self._call_with_retry(prompt, model, response_model, max_retries)

    def _call_with_retry(
        self,
        prompt: str,
        model: str,
        response_model: type[BaseModel] | None,
        max_retries: int,
    ) -> Any:
        last_error: Exception | None = None
        client = self._get_client()

        for attempt in range(max_retries):
            try:
                kwargs: dict[str, Any] = {
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                }
                if response_model is not None:
                    kwargs["response_model"] = response_model

                response = client.chat.completions.create(**kwargs)
                return response
            except Exception as e:
                last_error = e
                logger.warning("LLMProviderPool call failed (attempt %d/%d): %s", attempt + 1, max_retries, e)
                if attempt == max_retries - 1:
                    raise
        raise last_error or RuntimeError("LLMProviderPool call failed")
