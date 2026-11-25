"""Prompts, citation handling and response composition."""
from __future__ import annotations

import re
from collections import Counter
from typing import Any, Callable

from langsmith import traceable

from app.services.search_engine.plan import default_understood
from app.services.search_engine.textutil import norm

AGENT_PROMPT = """You are a search agent for Indian market regulatory circulars (SEBI, NSE, SEBI master circulars).
You receive the user's query, a parsed plan, and optionally low-confidence prefetched results.

Rules:
- Use ONLY information returned by tools. You see titles, numbers, dates, departments and status, and through search_chunks
  the passage text of circulars. If something is not in tool results, say so instead of guessing.
- For what a circular says (deadlines, thresholds, who is affected) call search_chunks.
- If the user quoted exact text, call find_phrase with the phrase exactly as written. Never loosen a quoted phrase.
- Write one factual claim per line. Use passage evidence for substantive claims.
- Cite inline: put the circular_id in square brackets right after each claim, e.g. [ISC77375]. One id per bracket.
  Only cite ids that tools returned, and also list them in final_answer.circular_ids.
- Dates: call resolve_dates for any period. Never compute dates yourself.
- Issuers: pass them to tools; for comparisons across issuers, search each issuer separately.
- If searches return nothing relevant (weak=true or off-topic), say that nothing matching was found. Do not pad results.
- If the request is too vague to search (no topic, issuer or period), call final_answer with a clarification question.
- Be efficient: at most 4 tool calls, then call final_answer. You MUST end by calling final_answer.
- The answer is 2-6 short sentences or bullets, plain text."""

SUMMARY_PROMPT = """You write a short factual summary for a search over Indian market regulatory circulars.
You get the user's query and passages, each tagged with a circular_id in square brackets.
Rules:
- Use ONLY the passages. No outside knowledge.
- Cite every claim with the circular_id in square brackets, e.g. [ISC77375]. One id per bracket.
- Put the most recent circular first. Copy dates, deadlines and thresholds exactly as written.
- If the passages do not answer the query, say so in one sentence and cite nothing.
- Under 150 words. Short bullets are fine. Plain text, no headings."""

PHRASE_SUMMARY_PROMPT = """You explain how an exact phrase is used in Indian market regulatory circulars.
You get the phrase and snippets, each tagged with a circular_id in square brackets, with the phrase marked «like this».
Rules:
- Use ONLY the snippets. No outside knowledge.
- Say where the phrase appears and what role it plays: the title of a circular, a reference to another circular (give its number
  and date if the snippet states them), or part of a rule or instruction.
- Cite with the circular_id in square brackets after each claim. One id per bracket.
- Newest circular first. Copy numbers and dates exactly as written.
- 1 to 3 sentences. Plain text."""

CITE_RE = re.compile(r"\[([^\[\]]{2,80})\]")


def message_text(msg: Any) -> str:
    c = msg.content
    return c if isinstance(c, str) else "".join(b.get("text", "") for b in c if isinstance(b, dict))


def src_row(row: dict[str, Any], n: int) -> dict[str, Any]:
    return {"n": n, "circular_id": row["circular_id"], "source": row["source"], "circular_no": row["full_reference"],
            "date": row["issue_date"], "title": row["title"], "url": row.get("pdf_url") or row.get("url")}


@traceable(name="finalize_citations", run_type="parser")
def finalize_citations(text: str | None, allowed: set[str], lookup: Callable[[str], list[dict]],
                       order: list[str] | None = None) -> tuple[str, list[dict], list[str]]:
    """[circular_id] tags -> [1], [2] in order of first use. Ids not in ``allowed`` are removed."""
    order, warnings = list(order or []), []

    def repl(m: re.Match) -> str:
        toks = [t.strip() for t in re.split(r"[,;]", m.group(1)) if t.strip()]
        if not toks or not all(t in allowed or re.fullmatch(r"[A-Za-z]{2,}\d{3,}", t) for t in toks):
            return m.group(0)  # not a citation (e.g. "[On-Hold]")
        nums = []
        for t in toks:
            if t not in allowed:
                warnings.append(f"removed citation {t}: not in retrieved sources")
                continue
            if t not in order:
                order.append(t)
            nums.append(order.index(t) + 1)
        return "".join(f"[{n}]" for n in nums)

    out = re.sub(r" {2,}", " ", CITE_RE.sub(repl, text or "")).strip()
    sources = []
    for i, cid in enumerate(order, 1):
        rows = lookup(cid)
        if rows:
            sources.append(src_row(rows[0], i))
    return out, sources, warnings


def to_result(h: dict[str, Any]) -> dict[str, Any]:
    return {"id": h["id"], "circular_id": h["circular_id"], "source": h["source"],
            "circular_no": h["full_reference"], "department": h["department"], "title": h["title"],
            "issue_date": h["issue_date"], "status": h["status"], "short_summary": h.get("short_summary"),
            "url": h.get("pdf_url") or h.get("url"), "match": h.get("match"), "score": h.get("score")}


def group_hits(hits: list[dict]) -> list[dict]:
    """One card per (source, title). Same-title notices from the same source collapse into '+N similar'."""
    groups: dict[tuple, list[dict]] = {}
    for h in hits:
        groups.setdefault((h["source"], norm(h["title"])), []).append(h)
    out = []
    for members in groups.values():
        members = sorted(members, key=lambda m: m["issue_date"], reverse=True)
        out.append({"latest": to_result(members[0]), "similar_count": len(members) - 1,
                    "similar": [{"circular_id": m["circular_id"], "circular_no": m["full_reference"],
                                 "issue_date": m["issue_date"]} for m in members[1:]]})
    return out


def facets_for(hits: list[dict]) -> dict[str, dict]:
    return {"source": dict(Counter(h["source"] for h in hits)),
            "year": dict(Counter(h["issue_date"][:4] for h in hits))}


def phrase_summary(hits: list[dict], matches: dict) -> tuple[str | None, list[dict]]:
    """Extractive and deterministic: counts and sentences copied from the circulars. No LLM."""
    n = len(hits)
    if not n:
        return None, []
    m_ = lambda h: matches[h["circular_id"]]  # noqa: E731
    t = sum(1 for h in hits if m_(h)["in_title"])
    b = sum(1 for h in hits if m_(h)["count"] > 0)
    x = sum(m_(h)["count"] + (1 if m_(h)["in_title"] else 0) for h in hits)
    parts = ([f"{t} with the phrase in the title"] if t else []) + ([f"{b} with it in the body"] if b else [])
    lines = [f"Found in {n} circular{'s' if n != 1 else ''} ({', '.join(parts)}; {x} mention{'s' if x != 1 else ''} in total)."]
    sources = []
    for i, h in enumerate(hits[:5], 1):
        m = m_(h)
        sources.append(src_row(h, i))
        if m["snippets"]:
            sn = m["snippets"][0]
            where, ctx = "body", f"{sn['before']} «{sn['match']}» {sn['after']}"
        else:
            where, ctx = "title", m["title_marked"]
        lines.append(f"- [{i}] {h['source']} {h['circular_id']} ({h['issue_date']}, {where}): {ctx}")
    if n > 5:
        lines.append(f"+{n - 5} more circulars in the list below.")
    return "\n".join(lines), sources


def compose_response(state: dict[str, Any]) -> dict[str, Any]:
    plan, hits, answer = state["plan"], state.get("hits", []), state.get("answer")
    clar, matches = state.get("clarification"), state.get("matches") or {}
    mode = "ANSWER" if (clar or plan["intent"] not in ("LOOKUP", "FILTER", "SEARCH", "PHRASE")) else plan["intent"]
    msg = clar
    if not msg and not hits and not answer:
        if plan.get("phrases"):
            msg = ("No circular contains the exact phrase " + " and ".join(f'"{p}"' for p in plan["phrases"])
                   + " (matching ignores case and punctuation). Try the search without quotes for a broader match.")
        else:
            parts = []
            if plan.get("issuers"):
                parts.append("issuer " + "/".join(plan["issuers"]))
            if plan.get("dates"):
                parts.append(f"dates {plan['dates']['from']} to {plan['dates']['to']}")
            msg = "No circulars found" + (" for " + ", ".join(parts) if parts else "") + "."
    groups = group_hits(hits)
    for g_ in groups:
        g_["latest"]["matches"] = matches.get(g_["latest"]["circular_id"])
    return {
        "mode": mode, "understood_as": plan.get("understood_as") or default_understood(plan),
        "answer": answer,
        "summary": {"text": answer, "sources": state.get("sources", []), "label": "AI summary: verify in the source"} if answer else None,
        "message": msg, "results": [to_result(h) for h in hits], "groups": groups, "facets": facets_for(hits),
        "phrase_matches": matches or None,
        "exact_matches": {"text": state.get("exact"), "sources": state.get("sources", []),
                          "label": "Exact matches: text copied from the circulars"} if state.get("exact") else None,
        "related": [], "warnings": state.get("warnings", []),
    }
