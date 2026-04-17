from threading import Lock
from typing import Any

import instructor
from openai import OpenAI

from config import Config


class LLMClient:
    """Singleton wrapper around the Instructor-patched OpenAI client for NVIDIA."""

    _instance: "LLMClient | None" = None
    _instance_lock = Lock()

    def __new__(cls) -> "LLMClient":
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


def get_llm_client() -> instructor.Instructor:
    """Helper method to get the singleton LLM client instance."""
    return LLMClient().get_client()
