from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from threading import Lock
from typing import Any

import instructor
from openai import OpenAI
from pydantic import BaseModel

from config import Config

from utils.llm_providers.base import BaseLLMClient


class NvidiaLLMClient(BaseLLMClient):
    """Singleton wrapper around the Instructor-patched OpenAI client for NVIDIA."""

    _instance: "NvidiaLLMClient | None" = None
    _instance_lock = Lock()

    def __new__(cls) -> "NvidiaLLMClient":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._client = None
        return cls._instance

    def get_client(self) -> instructor.Instructor:
        if self._client is None:
            api_key = Config.NVIDIA_API_KEY
            if not api_key:
                raise ValueError("NVIDIA_API_KEY is not set in the configuration.")

            openai_client = OpenAI(
                base_url="https://integrate.api.nvidia.com/v1",
                api_key=api_key,
            )
            self._client = instructor.from_openai(openai_client)

        return self._client

    def create_completions_parallel(
        self,
        prompts: list[str],
        model: str,
        response_model: type[BaseModel],
        max_workers: int = 8,
        max_retries: int = 3,
    ) -> list[Any]:
        logger = logging.getLogger(__name__)
        logger.info("Starting parallel LLM calls: count=%d max_workers=%d", len(prompts), max_workers)
        results: list[Any | None] = [None] * len(prompts)

        def _call_with_retry(prompt: str, idx: int) -> tuple[int, Any | None]:
            logger.info("LLM call starting: idx=%d attempt=0", idx)
            for attempt in range(max_retries):
                try:
                    response = self.get_client().chat.completions.create(
                        model=model,
                        response_model=response_model,
                        messages=[{"role": "user", "content": prompt}],
                    )
                    logger.info("LLM call completed: idx=%d attempt=%d", idx, attempt)
                    return idx, response
                except Exception as e:
                    logger.info("LLM call failed: idx=%d attempt=%d error=%s", idx, attempt, e)
                    if attempt == max_retries - 1:
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


def get_nvidia_llm_client() -> "NvidiaLLMClient":
    """Helper method to get the singleton NVIDIA LLM client instance."""
    return NvidiaLLMClient()
