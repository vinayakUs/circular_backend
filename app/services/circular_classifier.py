"""Circular → personas classifier.

Pure logic — takes a `CircularRef` and returns the list of persona slugs
it belongs in, based on the configured `config/personas.json` filters.

Match rule:
    - circular.source == filter.source (case-insensitive)
    - circular.department EQUALS one of filter.departments
      (case-insensitive, no substring)

Empty filter.departments = "match anything for this source".

This is pure (no DB, no HTTP), so it's trivial to test in isolation.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

from app.services.personas import Persona, get_personas


@dataclass(frozen=True)
class CircularRef:
    """Minimal projection of Circular for classification."""
    source: str
    department: Optional[str] = None
    full_reference: Optional[str] = None  # unused for now; available for future
    category: Optional[str] = None        # unused; column doesn't exist yet


def _norm(s: Optional[str]) -> str:
    """Lowercase + strip for comparison."""
    return (s or "").strip().lower()


def matches(circular: CircularRef, persona: Persona) -> bool:
    """True iff circular.department exactly matches a filter department."""
    src = _norm(circular.source)
    dept = _norm(circular.department)

    for f in persona.filters:
        if _norm(f.source) != src:
            continue
        if not f.departments:
            return True
        if any(dept == _norm(d) for d in f.departments):
            return True
    return False


def classify(
    circular: CircularRef,
    personas: Optional[Iterable[Persona]] = None,
) -> List[str]:
    """Return the slugs of personas this circular belongs in."""
    if personas is None:
        personas = get_personas()
    return [p.slug for p in personas if matches(circular, p)]