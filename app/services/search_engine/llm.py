"""Chat models for the search engine (Anthropic via langchain-anthropic)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from config import Config
from app.services.search_engine import settings


@dataclass
class Models:
    """Raw chat models. SearchEngine binds tools / forced tool-calls itself."""
    parser: Any
    agent: Any
    summary: Any


def build_models() -> Models:
    if not Config.ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY is not set (see config.py / environment).")
    from langchain_anthropic import ChatAnthropic

    def make(model: str, max_tokens: int, timeout: int) -> Any:
        return ChatAnthropic(model=model, temperature=0, max_tokens=max_tokens, timeout=timeout,
                             max_retries=1, api_key=Config.ANTHROPIC_API_KEY, base_url=Config.ANTHROPIC_BASE_URL or None)

    return Models(parser=make(settings.PARSER_MODEL, 500, 15),
                  agent=make(settings.AGENT_MODEL, 1200, 30),
                  summary=make(settings.SUMMARY_MODEL, 600, 30))
