"""OpenAI chat models for the circular search engine."""

from __future__ import annotations

from dataclasses import dataclass

from langchain_openai import ChatOpenAI

from app.services.search_engine import settings
from config import Config


@dataclass
class Models:
    parser: ChatOpenAI
    agent: ChatOpenAI
    summary: ChatOpenAI


def build_models() -> Models:

    if not Config.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not set (see config.py / environment).")

    def make(model: str, max_tokens: int, timeout: int) -> ChatOpenAI:
        return ChatOpenAI(
            model=model,
            max_completion_tokens=max_tokens,
            timeout=timeout,
            max_retries=1,
            temperature=0,
            api_key=Config.OPENAI_API_KEY,
            base_url=Config.OPENAI_BASE_URL
        )

    return Models(
        parser=make(settings.PARSER_MODEL, 500, 15),
        agent=make(settings.AGENT_MODEL, 1200, 30),
        summary=make(settings.SUMMARY_MODEL, 600, 30),
    )