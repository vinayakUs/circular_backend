"""Persona config loader. Validates config/personas.json at startup.

Each persona describes a role (Compliance Officer, Trading Desk, etc.) and the
filters that select the circulars relevant to that role — a list of (source,
departments, categories) tuples plus keywords.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

_PERSONAS_FILE = Path(__file__).parent.parent.parent / "config" / "personas.json"


class PersonaFilter(BaseModel):
    """One source's filter contribution: which departments + categories match."""

    source: str = Field(min_length=1)
    departments: List[str] = Field(default_factory=list)
    categories: List[str] = Field(default_factory=list)


class Persona(BaseModel):
    """A user role with the filters that match circulars they care about."""

    slug: str = Field(min_length=1)
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    icon: str = Field(min_length=1)
    filters: List[PersonaFilter] = Field(default_factory=list)
    keywords: List[str] = Field(default_factory=list)

    @field_validator("slug")
    @classmethod
    def _slug_must_be_kebab(cls, v: str) -> str:
        if not all(c.isalnum() or c == "-" for c in v) or "--" in v or v.startswith("-") or v.endswith("-"):
            raise ValueError(f"slug must be kebab-case: {v!r}")
        return v


# ─── Cached load ─────────────────────────────────────────────────────────
_cached: Optional[List[Persona]] = None
_cached_mtime: Optional[float] = None


def _read_file() -> List[Persona]:
    """Read + parse + validate the personas JSON file."""
    if not _PERSONAS_FILE.exists():
        raise FileNotFoundError(f"Personas config not found: {_PERSONAS_FILE}")
    raw = json.loads(_PERSONAS_FILE.read_text(encoding="utf-8"))
    return [Persona(**item) for item in raw]


def get_personas() -> List[Persona]:
    """Return the persona list. Cached, with mtime-based reload for dev hot-edit."""
    global _cached, _cached_mtime
    try:
        mtime = _PERSONAS_FILE.stat().st_mtime
    except FileNotFoundError:
        if _cached is not None:
            logger.warning("Personas config deleted at runtime; using stale cache")
            return _cached
        raise

    if _cached is None or _cached_mtime != mtime:
        new_list = _read_file()
        logger.info("Loaded %d personas from %s", len(new_list), _PERSONAS_FILE)
        _cached = new_list
        _cached_mtime = mtime
    return _cached


def reset_cache() -> None:
    """Force the next get_personas() call to re-read the file. For tests."""
    global _cached, _cached_mtime
    _cached = None
    _cached_mtime = None