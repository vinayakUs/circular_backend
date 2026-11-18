"""
Topic keyword query syntax.

Grammar (each stored keyword string is parsed):
- commas separate groups; every positive group must match (AND)
- OR (uppercase) or | alternates inside a group
- "quoted" is an exact phrase; bare words AND together, any order
- -term, -"phrase", NOT term exclude — always global to the query
- uppercase AND is a no-op synonym for the separators
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True, slots=True)
class Atom:
    text: str
    phrase: bool = False


@dataclass(frozen=True, slots=True)
class ParsedKeywords:
    # comma-groups of OR-alternatives; each alternative is a conjunction of atoms
    groups: tuple[tuple[tuple[Atom, ...], ...], ...]
    exclusions: tuple[Atom, ...]


_TOKEN_RE = re.compile(
    r'(?P<neg>-)?\s*"(?P<phrase>[^"]*)"'
    r'|(?P<neg2>-)?(?P<bare>[^\s"]+)'
)


def _split_commas(text: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    in_quotes = False
    for ch in text:
        if ch == '"':
            in_quotes = not in_quotes
            current.append(ch)
        elif ch == "," and not in_quotes:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    return parts


def _normalize_pipes(group: str) -> str:
    out: list[str] = []
    in_quotes = False
    for ch in group:
        if ch == '"':
            in_quotes = not in_quotes
            out.append(ch)
        elif ch == "|" and not in_quotes:
            out.append(" OR ")
        else:
            out.append(ch)
    return "".join(out)


def _parse_group(group: str) -> tuple[list[tuple[Atom, ...]], list[Atom]]:
    alternatives: list[list[Atom]] = [[]]
    exclusions: list[Atom] = []
    negate_next = False

    for m in _TOKEN_RE.finditer(_normalize_pipes(group)):
        if m.group("phrase") is not None:
            text = m.group("phrase").strip()
            if not text:
                negate_next = False
                continue
            atom = Atom(text, phrase=True)
            negated = bool(m.group("neg")) or negate_next
        else:
            word = m.group("bare")
            if word == "AND" or word == "-":
                continue
            if word == "NOT":
                negate_next = True
                continue
            if word == "OR":
                if alternatives[-1]:
                    alternatives.append([])
                continue
            atom = Atom(word)
            negated = bool(m.group("neg2")) or negate_next

        negate_next = False
        if negated:
            exclusions.append(atom)
        else:
            alternatives[-1].append(atom)

    return [tuple(a) for a in alternatives if a], exclusions


def parse_keywords(keywords: Sequence[str]) -> ParsedKeywords:
    groups: list[tuple[tuple[Atom, ...], ...]] = []
    exclusions: list[Atom] = []
    for kw in keywords:
        for raw in _split_commas(kw):
            alternatives, group_excl = _parse_group(raw.strip())
            if alternatives:
                groups.append(tuple(alternatives))
            exclusions.extend(group_excl)
    return ParsedKeywords(groups=tuple(groups), exclusions=tuple(exclusions))