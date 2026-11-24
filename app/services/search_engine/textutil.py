"""Small text helpers shared by retrieval and quality checks (stdlib only)."""
from __future__ import annotations

import re
from difflib import SequenceMatcher

STOP = set("a an the of for in on at to and or is are was what which show all me list latest circular circulars about from with by".split())
SYNONYMS = {"f&o": "futures and options", "fno": "futures and options", "mf": "mutual fund"}


def norm(t: str | None) -> str:
    t = (t or "").lower()
    for k, v in SYNONYMS.items():
        t = re.sub(rf"(?<!\w){re.escape(k)}(?!\w)", v, t)
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", t)).strip()


def content_tokens(t: str | None) -> list[str]:
    return [w for w in norm(t).split() if w not in STOP]


def tok_in(t: str, words: set[str]) -> bool:
    """Token present, or shares a 5-char stem (cheap stemming for 'restrictions' vs 'restriction')."""
    return any(w == t or (len(t) >= 5 and w.startswith(t[:5])) for w in words)


def coverage(toks: list[str], words: set[str]) -> float:
    """Share of query tokens found in ``words`` (exact, stem or >=0.85 similarity for typos)."""
    if not toks:
        return 0.0
    hit = sum(1 for t in toks if t in words or tok_in(t, words)
              or any(SequenceMatcher(None, t, x).ratio() >= 0.85 for x in words))
    return hit / len(toks)
