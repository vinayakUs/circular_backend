"""
Notification module — domain models.

Plain dataclasses only. No ORM. All DB access lives in
``notifications.repository`` and uses raw SQL via psycopg2.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from uuid import UUID


# ──────────────────────────── Trigger ────────────────────────────

@dataclass
class NotificationTrigger:
    """A user-defined rule that fires an email when matched."""

    user_id: int
    name: str
    sources: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    match_logic: str = "AND"           # "AND" | "OR"
    active: bool = True

    id: Optional[int] = None           # BIGSERIAL — set by DB
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        if self.match_logic not in ("AND", "OR"):
            raise ValueError("match_logic must be 'AND' or 'OR'")
        # store keywords lower-cased; comparison is case-insensitive
        self.keywords = [k.strip().lower() for k in self.keywords if k.strip()]
        # store sources upper-cased; sources in the codebase are uppercase codes
        self.sources = [s.strip().upper() for s in self.sources if s.strip()]


# ──────────────────────────── Log ────────────────────────────────

@dataclass
class NotificationLog:
    """One row per email attempt. The UNIQUE(user, circular, trigger) index
    on this table is the dedup guarantee — the matcher relies on it."""

    user_id: int
    circular_id: UUID
    to_email: str
    status: str                        # "sent" | "failed" | "skipped_dup"
    trigger_id: Optional[int] = None
    matched_keywords: list[str] = field(default_factory=list)
    error: Optional[str] = None
    id: Optional[int] = None
    sent_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        if self.status not in ("sent", "failed", "skipped_dup", "pending"):
            raise ValueError(f"invalid status: {self.status!r}")


# ──────────────────────────── Comment ────────────────────────────

@dataclass
class Comment:
    """A comment on an expert task."""

    id: UUID
    expert_id: UUID
    user_id: str
    user_name: str
    text: str
    created_at: datetime