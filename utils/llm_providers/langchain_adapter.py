"""LangChain adapter wrapping existing LLM providers (minimax/nvidia)."""

import json
import logging
from typing import Any, Callable, Iterator, Literal

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.messages import AIMessage
from pydantic import PrivateAttr

from config import Config
from utils.llm_providers import get_llm_provider


class LangChainLLMAdapter(BaseChatModel):
    """Wraps existing MinmaxLLMClient or NvidiaLLMClient as a LangChain BaseChatModel.

    This allows using existing LLM infrastructure with LangChain's summarization
    chains and other utilities.
    """

    model_name: str = None  # type: ignore[assignment]
    _max_tokens: int | None = PrivateAttr(default=None)

    def __init__(
        self,
        provider_name: str = None,
        model_name: str = None,
        custom_get_token_ids: Callable[[str], list[int]] | None = None,
        max_tokens: int | None = None,
        **kwargs,
    ):
        # Caller must supply a `custom_get_token_ids` callable so LangChain
        # chains that size prompts (e.g. map_reduce's collapse pre-check) don't
        # try to download GPT-2 from HuggingFace. We deliberately do not fall
        # back to the HF download — misconfiguration should surface as a clear
        # error rather than a silent network call.
        if custom_get_token_ids is None:
            raise ValueError(
                "LangChainLLMAdapter requires `custom_get_token_ids` to be set "
                "(e.g. backed by a locally-cached tokenizer) so LangChain's "
                "internal token counting does not try to download GPT-2 from "
                "HuggingFace at runtime."
            )
        kwargs["custom_get_token_ids"] = custom_get_token_ids
        super().__init__(**kwargs)
        self._provider_name = provider_name or Config.LLM_PROVIDER
        self._model_name = model_name or Config.RAG_MODEL
        self._max_tokens = max_tokens
        self._client = get_llm_provider(self._provider_name)

    @property
    def _llm_type(self) -> str:
        return f"adapter-{self._provider_name}"

    def _convert_messages_to_prompt(self, messages: list[BaseMessage]) -> str:
        """Convert LangChain messages to a single prompt string."""
        parts = []
        for msg in messages:
            if isinstance(msg, SystemMessage):
                parts.append(f"System: {msg.content}")
            elif isinstance(msg, HumanMessage):
                parts.append(f"User: {msg.content}")
            elif isinstance(msg, AIMessage):
                parts.append(f"Assistant: {msg.content}")
            else:
                parts.append(f"{msg.type}: {msg.content}")
        return "\n\n".join(parts)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        """Generate a chat completion from messages."""
        logger = logging.getLogger(__name__)
        prompt = self._convert_messages_to_prompt(messages)

        response_text = self._client.create_completions_parallel(
            prompts=[prompt],
            model=self._model_name,
            response_model=str,  # Return raw string
            max_tokens=self._max_tokens,
            max_workers=1,
        )[0]

        if response_text is None:
            # The LLM client returned None after exhausting retries (the underlying
            # InstructorRetryException was swallowed inside create_completions_parallel).
            # Surface it as a hard failure so callers can detect the LLM outage and
            # avoid treating str(None) == "None" as a valid chat completion.
            logger.error(
                "metric=langchain_adapter_llm_failure — LLM returned no response after retries"
            )
            raise RuntimeError("LLM call returned no response after retries")

        if isinstance(response_text, str):
            ai_message = AIMessage(content=response_text)
        else:
            ai_message = AIMessage(content=str(response_text))

        generation = ChatGeneration(message=ai_message)
        return ChatResult(generations=[generation])

    def _stream(self, messages: list[BaseMessage], **kwargs: Any) -> Iterator[ChatGeneration]:
        """Streaming not supported - yields single generation."""
        result = self._generate(messages, **kwargs)
        yield from result.generations