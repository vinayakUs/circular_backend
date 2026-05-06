"""LangChain adapter wrapping existing LLM providers (minimax/nvidia)."""

import json
from typing import Any, Iterator, Literal

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.messages import AIMessage

from config import Config
from utils.llm_providers import get_llm_provider


class LangChainLLMAdapter(BaseChatModel):
    """Wraps existing MinmaxLLMClient or NvidiaLLMClient as a LangChain BaseChatModel.

    This allows using existing LLM infrastructure with LangChain's summarization
    chains and other utilities.
    """

    model_name: str = None  # type: ignore[assignment]

    def __init__(self, provider_name: str = None, model_name: str = None, **kwargs):
        super().__init__(**kwargs)
        self._provider_name = provider_name or Config.LLM_PROVIDER
        self._model_name = model_name or Config.RAG_MODEL
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
        prompt = self._convert_messages_to_prompt(messages)

        response_text = self._client.create_completions_parallel(
            prompts=[prompt],
            model=self._model_name,
            response_model=str,  # Return raw string
            max_workers=1,
        )[0]

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