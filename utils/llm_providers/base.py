from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

if TYPE_CHECKING:
    from llama_index.core.llms.llm import LLM


def build_openai_like_llm(
    api_base: str,
    api_key: str,
    model: str,
    max_tokens: int | None = None,
    temperature: float = 0.0,
) -> "LLM":
    # Lazy: utils.llm_providers is imported by ingestion paths that never touch LlamaIndex.
    from llama_index.llms.openai_like import OpenAILike

    return OpenAILike(
        model=model,
        api_base=api_base,
        api_key=api_key,
        temperature=temperature,
        max_tokens=max_tokens,
        is_chat_model=True,
        max_retries=0,  # callers own retries
    )


class BaseLLMClient(ABC):
    _llama_llms: dict[tuple[str, int | None, float], "LLM"] = {}

    @property
    @abstractmethod
    def api_base(self) -> str: ...

    @property
    @abstractmethod
    def api_key(self) -> str: ...

    def get_llama_llm(
        self,
        model: str,
        max_tokens: int | None = None,
        temperature: float = 0.0,
    ) -> "LLM":
        key = (model, max_tokens, temperature)
        cached = self._llama_llms.get(key)
        if cached is not None:
            return cached
        self._llama_llms[key] = build_openai_like_llm(
            api_base=self.api_base,
            api_key=self.api_key,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return self._llama_llms[key]

    @abstractmethod
    def create_completions_parallel(
        self,
        prompts: list[str],
        model: str,
        response_model: type[BaseModel],
        max_workers: int = 8,
        max_retries: int = 3,
        max_tokens: int | None = None,
    ) -> list[Any]:
        """Execute multiple chat completion calls in parallel.

        Args:
            prompts: List of prompt strings (one per call)
            model: Model name to use
            response_model: Pydantic model for structured response
            max_workers: Max concurrent calls (default 8)
            max_retries: Retries per call on failure (default 3)
            max_tokens: Optional cap on output tokens forwarded to the
                underlying chat.completions.create call. When None, the
                provider default is used and the request payload is
                byte-identical to the pre-existing behavior.

        Returns:
            List of response objects (one per prompt, in same order).
            Failed calls return None.
        """
        ...
