from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from langchain_core.messages import AIMessage


class FakeChat:
    """Scripted chat model: returns queued AIMessages; bind_tools is a no-op."""

    def __init__(self, replies: list[AIMessage] | None = None) -> None:
        self.replies = list(replies or [])
        self.calls: list[Any] = []

    def bind_tools(self, *a: Any, **k: Any) -> "FakeChat":
        return self

    def invoke(self, messages: Any) -> AIMessage:
        self.calls.append(messages)
        if not self.replies:
            raise RuntimeError("no scripted reply left")
        return self.replies.pop(0)


def tool_call(name: str, args: dict, id: str = "t1") -> AIMessage:
    return AIMessage(content="", tool_calls=[{"name": name, "args": args, "id": id}])


@dataclass
class Rec:
    circular_id: str
    source: str
    title: str
    issue_date: date
    full_reference: str = ""
    department: str = "Dept"
    status: str = "FETCHED"
    is_active: bool = True
    id: UUID = field(default_factory=uuid4)
    url: str = "http://x"
    pdf_url: str = "http://x.pdf"


class FakeRepo:
    def __init__(self, recs: list[Rec]) -> None:
        self.recs = recs

    def get_records_by_ids(self, ids: list[UUID]) -> list[Rec]:
        return [r for r in self.recs if r.id in ids]

    def get_record_by_full_reference(self, ref: str) -> Rec | None:
        return next((r for r in self.recs if r.full_reference.upper() == ref.upper()), None)

    def get_record_by_circular_id(self, cid: str, source: str | None = None) -> Rec | None:
        return next((r for r in self.recs if r.circular_id.upper() == cid.upper()), None)


def make_doc(rec: Rec, text: str = "", idx: int = 0) -> SimpleNamespace:
    return SimpleNamespace(circular_db_id=str(rec.id), circular_id=rec.circular_id, title=rec.title,
                           chunk_text=text, chunk_index=idx, source=rec.source)


def make_hit(rec: Rec, text: str = "", idx: int = 0, score: float = 1.0) -> SimpleNamespace:
    return SimpleNamespace(es_id=f"{rec.circular_id}:{idx}", score=score, document=make_doc(rec, text, idx))


class FakeES:
    """Mimics the parts of ElasticsearchClient the retrieval layer touches."""

    index_name = "test"

    def __init__(self) -> None:
        self.client = self
        self.search_calls: list[dict] = []
        self.responses: list[list[Any]] = []   # queued hit lists for client.search
        self.hybrid_hits: list[Any] = []
        self.hybrid_calls: list[tuple] = []

    # real helpers re-used so filter wiring is genuinely exercised
    def _build_filters(self, metadata: dict) -> list[dict]:
        from ingestion.indexer.es_client import ElasticsearchClient
        return ElasticsearchClient._build_filters(self, metadata)  # type: ignore[arg-type]

    def _build_v2_title_query(self, q: str, filters: list) -> dict:
        return {"title_query": q, "filter": filters}

    def _build_v2_phrase_query(self, q: str, filters: list) -> dict:
        return {"phrase_query": q, "filter": filters}

    def search(self, *args: Any, **kwargs: Any) -> Any:
        # doubles as client.search(index=..., query=...) and ElasticsearchClient.search(query, meta, ...)
        if "index" in kwargs:
            self.search_calls.append(kwargs)
            return self.responses.pop(0) if self.responses else []
        self.hybrid_calls.append((args, kwargs))
        return self.hybrid_hits

    def _parse_hits(self, resp: Any) -> list[Any]:
        return resp


@pytest.fixture
def recs() -> list[Rec]:
    return [
        Rec("ISC77375", "NSE", "Guidelines in pursuance of amendment to SEBI KYC (Know Your Client) Registration Agency (KRA) Regulations, 2011",
            date(2026, 11, 9), "NSE/ISC/77375", "Investor Services Cell"),
        Rec("FAOP74180", "NSE", "DIVIDEND: Adjustment of Futures and Options contracts in the security BANKINDIA",
            date(2026, 5, 12), "NSE/FAOP/74180", "Futures & Options"),
        Rec("MFSS74181", "NSE", "API for Mutual Fund Global MIS by Payment Aggregators",
            date(2026, 5, 12), "NCL/MFSS/74181", "Mutual Fund"),
    ]
