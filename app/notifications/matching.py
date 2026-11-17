"""
Topic matching for digest fan-out — one topic in, the circulars it matches out.

Backed by Elasticsearch: a topic's keywords become one search over circular
full text (chunk_text + title), filtered by source and issue date. Matching
against the PDF body is the whole point of routing this through ES instead
of a Python substring check on titles.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from app.topics.user_topics_repository import UserTopic


@dataclass(slots=True, frozen=True)
class MatchedCircular:
    id: str
    circular_id: str
    full_reference: str
    title: str
    source: str
    issue_date: str

    def to_params(self) -> dict[str, str]:
        return {
            "id": self.id,
            "full_reference": self.full_reference,
            "title": self.title,
            "source": self.source,
            "issue_date": self.issue_date,
        }


class TopicMatcher:
    """One concrete class — a Protocol with one implementation is ceremony.
    The seam is this method: immediate alerts reuse it later unchanged."""

    def __init__(self, es_client: Any):
        self._es = es_client

    def match_window(
        self, topic: UserTopic, *, now: datetime, window_days: int
    ) -> list[MatchedCircular]:
        hits = self._es.search_any_keywords(
            topic.keywords,
            sources=topic.sources,
            since=now - timedelta(days=window_days),
            size=70
        )
        out = []
        for hit in hits:
            doc = hit.document
            out.append(
                MatchedCircular(
                    id=doc.circular_db_id,
                    circular_id=doc.circular_id,
                    full_reference=doc.full_reference,
                    title=doc.title,
                    source=doc.source,
                    issue_date=str(doc.issue_date) if doc.issue_date else "",
                )
            )
        return out
