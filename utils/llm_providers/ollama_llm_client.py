from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from threading import Lock
from typing import Any

import instructor
from openai import OpenAI
from pydantic import BaseModel

from config import Config

from utils.llm_providers.base import BaseLLMClient


class OllamaLLMClient(BaseLLMClient):
    """Singleton wrapper around the Instructor-patched OpenAI client for Ollama."""

    _instance: "OllamaLLMClient | None" = None
    _instance_lock = Lock()

    def __new__(cls) -> "OllamaLLMClient":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._client = None
        return cls._instance

    def get_client(self) -> instructor.Instructor:
        if self._client is None:
            openai_client = OpenAI(
                base_url=Config.OLLAMA_BASE_URL,
                api_key=Config.OLLAMA_API_KEY or "ollama",
            )
            self._client = instructor.from_openai(openai_client, mode=instructor.Mode.JSON)

        return self._client

    def create_completions_parallel(
        self,
        prompts: list[str],
        model: str,
        response_model: type[BaseModel],
        max_workers: int = 8,
        max_retries: int = 3,
        max_tokens: int | None = None,
    ) -> list[Any]:
        logger = logging.getLogger(__name__)
        logger.info("Starting parallel LLM calls: count=%d max_workers=%d", len(prompts), max_workers)
        results: list[Any | None] = [None] * len(prompts)

        def _call_with_retry(prompt: str, idx: int) -> tuple[int, Any | None]:
            logger.info("LLM call starting: idx=%d attempt=0", idx)
            for attempt in range(max_retries):
                try:
                    create_kwargs: dict[str, Any] = {
                        "model": model,
                        "response_model": response_model,
                        "messages": [{"role": "user", "content": prompt}],
                    }
                    if max_tokens is not None:
                        create_kwargs["max_tokens"] = max_tokens
                    response = self.get_client().chat.completions.create(
                        **create_kwargs,
                    )
                    logger.info("LLM call completed: idx=%d attempt=%d", idx, attempt)
                    return idx, response
                except Exception as e:
                    error_short = type(e).__name__
                    logger.info("LLM call failed: idx=%d attempt=%d error=%s", idx, attempt, error_short)
                    if attempt == max_retries - 1:
                        logger.warning("LLM call failed after %d attempts: idx=%d", max_retries, idx)
                        return idx, None
                    continue
            return idx, None

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_call_with_retry, prompt, i)
                for i, prompt in enumerate(prompts)
            ]
            for future in as_completed(futures):
                idx, result = future.result()
                results[idx] = result

        logger.info("Parallel LLM calls completed: count=%d", len(prompts))
        return results


def get_ollama_llm_client() -> "OllamaLLMClient":
    """Helper method to get the singleton Ollama LLM client instance."""
    return OllamaLLMClient()