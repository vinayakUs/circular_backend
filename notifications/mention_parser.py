"""
Parse @mentions out of comment text. Pure function — no DB.

Syntax:
    @alice                       → ('user', 'alice')
    @alice@gmail.com             → ('user', 'alice@gmail.com')   (LDAP email-style uid)
    @dep:compliance              → ('department', 'compliance')

A mention requires:
  - a leading `@`
  - not immediately preceded by a word char or another `@` (so plain email
    addresses inside prose don't trigger)
"""
from __future__ import annotations

import re

# (?<![\w@])   negative lookbehind: no word-char or '@' directly before the @
# @             the trigger
# ((?:dep:)?)   optional 'dep:' prefix  (group 1)
# ([A-Za-z0-9_.\-@]+)   bare identifier, may itself contain '@' for email-style uids
_MENTION_RE = re.compile(r"(?<![\w@])@((?:dep:)?)([A-Za-z0-9_.\-@]+)")


def parse_mentions(text: str) -> list[tuple[str, str]]:
    """Return ordered, deduplicated list of (target_type, target_id)."""
    out: list[tuple[str, str]] = []
    for m in _MENTION_RE.finditer(text):
        prefix, ident = m.group(1), m.group(2)
        if prefix == "dep:":
            out.append(("department", ident))
        else:
            out.append(("user", ident))
    return list(dict.fromkeys(out))
