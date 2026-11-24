"""The agent's tools. Thin wrappers over Retrieval; they return compact JSON, never full documents."""
from __future__ import annotations

import json
from typing import Any, Optional

from langchain_core.tools import tool

from app.services.search_engine.dates import resolve_dates, today_ist
from app.services.search_engine.plan import normalize_issuer
from app.services.search_engine.settings import ALLOWED_ISSUERS


def compact(h: dict[str, Any]) -> dict[str, Any]:
    return {"circular_id": h["circular_id"], "circular_no": h["full_reference"], "source": h["source"],
            "department": h["department"], "title": h["title"], "issue_date": h["issue_date"],
            "status": h["status"], "summary": h.get("short_summary")}


def _issuer(name: Optional[str]) -> tuple[list[str], Optional[str]]:
    if not name:
        return [], None
    norm_name = normalize_issuer(name)
    if norm_name:
        return [norm_name], None
    return [], f"Unknown issuer '{name}'. Allowed: {', '.join(ALLOWED_ISSUERS)}"


def _dates(date_from: Optional[str], date_to: Optional[str]) -> Optional[dict[str, str]]:
    return {"from": date_from, "to": date_to} if date_from and date_to else None


def build_tools(rt: Any) -> list[Any]:
    """``rt`` is a Retrieval (or anything with the same methods)."""

    @tool
    def search_circulars(query: str, issuer: Optional[str] = None,
                         date_from: Optional[str] = None, date_to: Optional[str] = None) -> str:
        """Title / keyword search over circulars. Use for any topic, title or keyword search.
        issuer is optional (SEBI, NSE, SEBI_MASTER). date_from/date_to are optional ISO dates (YYYY-MM-DD) and must
        come from resolve_dates, never computed by you. Returns up to 8 circulars ranked by match, plus match quality.
        Quality weak=true means nothing close was found."""
        issuers, err = _issuer(issuer)
        if err:
            return json.dumps({"error": err})
        hits, quality = rt.title_search(query, issuers, _dates(date_from, date_to), limit=8)
        return json.dumps({"results": [compact(h) for h in hits], "quality": quality}, default=str)

    @tool
    def filter_circulars(issuer: Optional[str] = None, date_from: Optional[str] = None,
                         date_to: Optional[str] = None) -> str:
        """List circulars by issuer and/or date range (ISO dates from resolve_dates), newest first.
        Use when the user wants everything from a source or period, not a topic. At least one argument is required."""
        issuers, err = _issuer(issuer)
        if err:
            return json.dumps({"error": err})
        if not issuers and not (date_from and date_to):
            return json.dumps({"error": "provide issuer or both date_from and date_to"})
        hits = rt.filter(issuers, _dates(date_from, date_to), limit=20)
        return json.dumps({"results": [compact(h) for h in hits]}, default=str)

    @tool
    def search_chunks(query: str, issuer: Optional[str] = None, date_from: Optional[str] = None,
                      date_to: Optional[str] = None, circular_id: Optional[str] = None) -> str:
        """Search the TEXT of circulars (PDF passages). Use for what a circular says: deadlines, thresholds, who is affected.
        Optional circular_id limits it to one circular. Returns up to 6 passages with circular_id, source, date and text.
        Cite a passage by writing its circular_id in square brackets."""
        issuers, err = _issuer(issuer)
        if err:
            return json.dumps({"error": err})
        ps = rt.chunk_search(query, issuers, _dates(date_from, date_to), [circular_id] if circular_id else None, limit=6)
        return json.dumps({"results": [
            {"circular_id": p["circular_id"], "circular_no": p["full_reference"], "source": p["source"],
             "issue_date": p["issue_date"], "title": p["title"], "text": p["text"]} for p in ps]}, default=str)

    @tool
    def find_phrase(phrase: str, issuer: Optional[str] = None, date_from: Optional[str] = None,
                    date_to: Optional[str] = None) -> str:
        """Find circulars containing an EXACT phrase (case and punctuation ignored, word order kept). Use whenever the user put
        text in quotes. Pass the phrase exactly as written and never loosen it. Returns circulars with snippets."""
        issuers, err = _issuer(issuer)
        if err:
            return json.dumps({"error": err})
        hits, matches = rt.phrase_search([phrase], issuers, _dates(date_from, date_to))
        out = []
        for h in hits[:8]:
            m = matches[h["circular_id"]]
            out.append({"circular_id": h["circular_id"], "circular_no": h["full_reference"], "source": h["source"],
                        "issue_date": h["issue_date"], "title": h["title"], "phrase_in_title": m["in_title"],
                        "mentions_in_body": m["count"],
                        "snippets": [{"text": f"{x['before']} {x['match']} {x['after']}"} for x in m["snippets"][:2]]})
        return json.dumps({"results": out}, default=str)

    @tool
    def get_circular(circular_id: str) -> str:
        """Fetch one circular by circular_id (e.g. FAOP74180) or circular number (e.g. NSE/FAOP/74180), with its PDF link."""
        rows = rt.lookup(circular_id)
        return json.dumps({"results": [{**compact(r), "pdf_url": r.get("pdf_url")} for r in rows]}, default=str)

    @tool("resolve_dates")
    def resolve_dates_tool(text: str) -> str:
        """Convert a natural-language period ('last week', 'October', '12 May 2026') into ISO dates in IST.
        Always use this instead of computing dates yourself."""
        return json.dumps({"dates": resolve_dates(text), "today": today_ist().isoformat()})

    @tool
    def final_answer(answer: Optional[str] = None, circular_ids: Optional[list[str]] = None,
                     clarification: Optional[str] = None) -> str:
        """Finish. answer: short text grounded ONLY in tool results (or null if you are just returning circulars).
        circular_ids: circular_id values (from tool results) that support your answer or that should be shown to the user.
        clarification: a single question to ask the user if the request is too vague; then leave answer null."""
        return "done"

    return [search_circulars, search_chunks, find_phrase, filter_circulars, get_circular, resolve_dates_tool, final_answer]
