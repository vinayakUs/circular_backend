"""
Notification module — request / response schemas (Pydantic v2).

These are the API contract only. They do not touch the DB.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ─────────────────────── Trigger payloads ────────────────────────

MatchLogic = Literal["AND", "OR"]
Channel    = Literal["inbox", "email"]   # kept for forward-compat; only "email" used now


class TriggerBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    sources: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    match_logic: MatchLogic = "AND"
    active: bool = True

    @field_validator("keywords")
    @classmethod
    def _clean_keywords(cls, v: list[str]) -> list[str]:
        cleaned = [k.strip().lower() for k in v if k and k.strip()]
        if any(len(k) < 3 for k in cleaned):
            raise ValueError("each keyword must be at least 3 characters")
        return cleaned

    @field_validator("sources")
    @classmethod
    def _clean_sources(cls, v: list[str]) -> list[str]:
        return [s.strip().upper() for s in v if s and s.strip()]


class TriggerCreate(TriggerBase):
    """Body for POST /api/notifications/triggers.

    ``apply_to_last_n_days`` is an optional backfill hint: when > 0, the
    matcher is invoked once against circulars from the last N days so
    the new trigger doesn't look broken until fresh circulars arrive.
    """
    apply_to_last_n_days: int = Field(default=0, ge=0, le=90)


class TriggerUpdate(BaseModel):
    """Body for PUT /api/notifications/triggers/<id> — every field optional."""
    name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    sources: Optional[list[str]] = None
    keywords: Optional[list[str]] = None
    match_logic: Optional[MatchLogic] = None
    active: Optional[bool] = None

    @field_validator("keywords")
    @classmethod
    def _clean_keywords(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        if v is None:
            return v
        cleaned = [k.strip().lower() for k in v if k and k.strip()]
        if any(len(k) < 3 for k in cleaned):
            raise ValueError("each keyword must be at least 3 characters")
        return cleaned

    @field_validator("sources")
    @classmethod
    def _clean_sources(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        if v is None:
            return v
        return [s.strip().upper() for s in v if s and s.strip()]


class TriggerOut(TriggerBase):
    """Response shape — what the API returns."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime
    updated_at: datetime


# ─────────────────────── Test-trigger payload ────────────────────

class TriggerTestIn(BaseModel):
    """Body for POST /api/notifications/triggers/<id>/test.

    Dry-run: paste a sample title / summary and see whether the trigger
    would match, without writing anything to the DB.
    """
    title: str = Field(..., min_length=1)
    summary: Optional[str] = ""
    source: Optional[str] = None         # if omitted, source filter is skipped


class TriggerTestOut(BaseModel):
    matched: bool
    matched_keywords: list[str] = Field(default_factory=list)
    reason: Optional[str] = None         # human-readable "why not" when matched=False


# ─────────────────────── Notification log ───────────────────────

class NotificationLogOut(BaseModel):
    """Read-only audit row: 'what emails did I get?' """
    model_config = ConfigDict(from_attributes=True)

    id: int
    circular_id: UUID
    trigger_id: Optional[int]
    to_email: str
    matched_keywords: list[str]
    status: Literal["sent", "failed", "skipped_dup", "pending"]
    error: Optional[str] = None
    sent_at: datetime


class NotificationLogListOut(BaseModel):
    items: list[NotificationLogOut]
    total: int
    limit: int
    offset: int