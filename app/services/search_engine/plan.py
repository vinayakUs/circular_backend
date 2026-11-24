"""Query -> structured plan. Regex first, LLM second, rule-based fallback; code validates everything."""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Literal, Optional

from langchain_core.messages import HumanMessage, SystemMessage
from langsmith import traceable
from pydantic import BaseModel, Field

from app.services.search_engine.dates import resolve_dates
from app.services.search_engine.settings import ALLOWED_ISSUERS, ISSUER_ALIASES

logger = logging.getLogger(__name__)

# e.g. SEBI/HO/MRD/MRD-PoD-3/P/CIR/2026/123 or NSE/FAOP/74180 (prefix + 2 or more "/" segments)
CIRCULAR_NO_RE = re.compile(r"\b[A-Z]{2,6}(?:/[A-Za-z0-9._\-]+){2,}")


class QueryPlan(BaseModel):
    intent: Literal["LOOKUP", "FILTER", "SEARCH", "QUESTION", "COMPARE", "VAGUE"]
    circular_no: Optional[str] = None
    issuers: list[str] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)
    date_phrase: Optional[str] = None  # raw wording only; code resolves it
    clean_query: str = ""
    keywords: list[str] = Field(default_factory=list)
    is_question: bool = False
    needs_clarification: bool = False
    phrases: list[str] = Field(default_factory=list)  # filled by code from "quoted text", never by the LLM
    understood_as: str = Field(
        default="",
        description="One short sentence restating the search, e.g. 'Search for margin framework circulars'",
    )


PARSER_PROMPT = """You extract a structured search plan from a user's query about Indian market regulatory circulars (SEBI, NSE).

Rules:
- Extract fields only. Never answer the question.
- date_phrase: copy the date wording exactly as the user wrote it (e.g. "last month", "15 Nov 2026"). Never convert to ISO dates.
- clean_query: the query with issuer names, date wording and filler words (show, all, circulars, about, from) removed. For a title-like query keep the full title.
- issuers: only from this list: """ + ", ".join(ALLOWED_ISSUERS) + """ (SEBI_MASTER means SEBI master circulars).
- intent: LOOKUP (a circular number is given) | FILTER (only issuer/date/category, no topic words) | SEARCH (any topic or title words; do NOT try to tell titles from topics) | QUESTION (what/how/why/can/does...) | COMPARE (comparison across issuers or time) | VAGUE (no usable topic, issuer or date).
- For SEARCH, clean_query is the topic or title text with issuer names, dates and filler removed. categories are optional hints only.
- Always respond by calling the QueryPlan tool. Never reply with plain text or JSON in the message body.
- Text in double quotes has already been removed from the query you see; exact phrases are handled elsewhere.
- understood_as: one short sentence restating what the user wants, starting with "Search for", "List", "Compare" or "Find".
- needs_clarification is true only for VAGUE. is_question is true for QUESTION and COMPARE.

Examples:
Q: show all circulars for 15 Nov 2026
A: {"intent":"FILTER","issuers":[],"categories":[],"date_phrase":"15 Nov 2026","clean_query":"","keywords":[],"is_question":false,"needs_clarification":false}
Q: NSE circulars last week
A: {"intent":"FILTER","issuers":["NSE"],"categories":[],"date_phrase":"last week","clean_query":"","keywords":[],"is_question":false,"needs_clarification":false}
Q: Margin Framework for Commodity Derivatives Segment
A: {"intent":"SEARCH","issuers":[],"categories":[],"date_phrase":null,"clean_query":"Margin Framework for Commodity Derivatives Segment","keywords":["margin framework","commodity derivatives"],"is_question":false,"needs_clarification":false}
Q: SEBI circulars about KYC from last month
A: {"intent":"SEARCH","issuers":["SEBI"],"categories":["KYC"],"date_phrase":"last month","clean_query":"KYC","keywords":["KYC"],"is_question":false,"needs_clarification":false}
Q: What is the peak margin requirement for commodity derivatives?
A: {"intent":"QUESTION","issuers":[],"categories":["margin"],"date_phrase":null,"clean_query":"peak margin requirement for commodity derivatives","keywords":["peak margin","commodity derivatives"],"is_question":true,"needs_clarification":false}
Q: Compare NSE and SEBI rules on algo trading
A: {"intent":"COMPARE","issuers":["NSE","SEBI"],"categories":["algo trading"],"date_phrase":null,"clean_query":"rules on algo trading","keywords":["algo trading"],"is_question":true,"needs_clarification":false}
Q: latest changes for brokers
A: {"intent":"VAGUE","issuers":[],"categories":[],"date_phrase":null,"clean_query":"changes for brokers","keywords":["brokers"],"is_question":false,"needs_clarification":true}
"""


def empty_raw() -> dict[str, Any]:
    return {"intent": "VAGUE", "circular_no": None, "issuers": [], "categories": [], "date_phrase": None,
            "clean_query": "", "keywords": [], "is_question": False, "needs_clarification": False,
            "understood_as": "", "phrases": []}


@traceable(name="regex_precheck", run_type="parser")
def regex_precheck(q: str) -> dict[str, Any] | None:
    """Circular numbers are exact patterns: no LLM needed."""
    m = CIRCULAR_NO_RE.search(q)
    if not m:
        return None
    no = m.group(0)
    issuer = ISSUER_ALIASES.get(no.split("/")[0].lower())
    raw = empty_raw()
    raw.update(intent="LOOKUP", circular_no=no, issuers=[issuer] if issuer else [])
    return raw


def _message_text(msg: Any) -> str:
    c = msg.content
    return c if isinstance(c, str) else "".join(b.get("text", "") for b in c if isinstance(b, dict))


@traceable(name="llm_extract", run_type="chain")
def llm_extract(llm: Any, q: str) -> dict[str, Any]:
    """``llm`` must already be bound to the QueryPlan tool (forced)."""
    last_err: Exception | None = None
    for _ in range(2):  # one retry
        msg = llm.invoke([SystemMessage(content=PARSER_PROMPT), HumanMessage(content=q)])
        if getattr(msg, "tool_calls", None):
            try:
                return QueryPlan(**msg.tool_calls[0]["args"]).model_dump()
            except Exception as e:  # noqa: BLE001
                last_err = e
                continue
        m = re.search(r"\{.*\}", _message_text(msg), re.S)  # salvage JSON if the model replied in text
        if m:
            try:
                return QueryPlan(**json.loads(m.group(0))).model_dump()
            except Exception as e:  # noqa: BLE001
                last_err = e
    raise ValueError(f"no structured plan from LLM: {last_err}")


FILLER = r"\b(show|all|me|the|circulars?|about|from|for|on|in|of|latest)\b"
DATE_RE = re.compile(
    r"(today|yesterday|this week|last week|this month|last month|(?:last|past) \d+ days?|"
    r"\d{1,2}[ /-](?:[a-z]{3,9}|\d{1,2})(?:[ /-]\d{2,4})?)", re.I)


@traceable(name="rule_based_fallback", run_type="parser")
def rule_based_parse(q: str) -> dict[str, Any]:
    """Used only if the LLM call fails, so search never breaks because of the parser."""
    low = q.lower()
    raw = empty_raw()
    raw["issuers"] = sorted({v for k, v in ISSUER_ALIASES.items() if re.search(rf"\b{re.escape(k)}\b", low)})
    dm = DATE_RE.search(q)
    raw["date_phrase"] = dm.group(0) if dm else None
    clean = low
    for k in sorted(ISSUER_ALIASES, key=len, reverse=True):
        clean = re.sub(rf"\b{re.escape(k)}\b", " ", clean)
    if dm:
        clean = clean.replace(dm.group(0).lower(), " ")
    clean = re.sub(FILLER, " ", clean)
    raw["clean_query"] = re.sub(r"\s+", " ", clean).strip(" ?.")
    raw["is_question"] = bool(re.match(r"(what|how|why|when|which|can|does|do|is|are|explain|list)\b", low)) or q.strip().endswith("?")
    if "compare" in low or " vs " in low:
        raw["intent"], raw["is_question"] = "COMPARE", True
    elif raw["is_question"]:
        raw["intent"] = "QUESTION"
    elif not raw["clean_query"]:
        raw["intent"] = "FILTER" if (raw["date_phrase"] or raw["issuers"]) else "VAGUE"
    else:
        raw["intent"] = "SEARCH"
    raw["keywords"] = raw["clean_query"].split()[:5]
    raw["needs_clarification"] = raw["intent"] == "VAGUE"
    return raw


def default_understood(p: dict[str, Any]) -> str:
    who = " from " + "/".join(p.get("issuers") or []) if p.get("issuers") else ""
    when = f" between {p['dates']['from']} and {p['dates']['to']}" if p.get("dates") else ""
    if p.get("phrases"):
        quoted = " and ".join(f'"{x}"' for x in p["phrases"])
        return f"Find circulars containing the exact phrase {quoted}{who}{when}"
    if p.get("circular_no"):
        return f"Look up circular {p['circular_no']}"
    if p.get("clean_query"):
        return f"Search for {p['clean_query']}{who}{when}"
    return f"List circulars{who}{when}"


def normalize_issuer(name: str | None) -> str | None:
    if not name:
        return None
    return ISSUER_ALIASES.get(name.strip().lower()) or (name.upper() if name.upper() in ALLOWED_ISSUERS else None)


@traceable(name="validate_plan", run_type="parser")
def validate_plan(raw: dict[str, Any], source: str) -> dict[str, Any]:
    """Code owns validation: issuer list, dates, intent sanity checks."""
    plan = dict(raw)
    warnings: list[str] = []

    issuers = []
    for i in raw.get("issuers", []):
        norm = normalize_issuer(i)
        if norm:
            issuers.append(norm)
        else:
            warnings.append(f"dropped unknown issuer: {i}")
    plan["issuers"] = list(dict.fromkeys(issuers))

    plan["dates"] = resolve_dates(raw.get("date_phrase"))
    if raw.get("date_phrase") and not plan["dates"]:
        warnings.append(f"could not resolve date phrase: {raw['date_phrase']}")

    intent = plan["intent"]
    if intent == "LOOKUP" and not plan.get("circular_no"):
        intent = "SEARCH"
        warnings.append("LOOKUP without circular_no -> SEARCH")
    if not plan.get("clean_query") and (plan["dates"] or plan["issuers"]) and intent in ("SEARCH", "FILTER", "VAGUE"):
        intent = "FILTER"
    plan["intent"] = intent
    plan["needs_clarification"] = intent == "VAGUE"
    if not plan.get("understood_as"):
        plan["understood_as"] = default_understood(plan)
    plan["source"] = source
    plan["warnings"] = warnings
    return plan


PHRASE_RE = re.compile(r'"([^"\n]{2,200})"|“([^”\n]{2,200})”')
PHRASE_FILLER = FILLER + (r"|\b(find|search|containing|contains|contain|with|exact|exactly|phrase|that|mention|"
                          r"mentions|mentioning|which|has|have|text|words?|and)\b")


def extract_phrases(q: str) -> tuple[list[str], str]:
    """Quoted text is an explicit request for exact wording, so code (not the LLM) pulls it out."""
    phrases = [(x or y).strip() for x, y in PHRASE_RE.findall(q)]
    rest = re.sub(r"\s+", " ", PHRASE_RE.sub(" ", q)).strip()
    return [p for p in phrases if re.search(r"\w", p)], rest


def parse_query_text(q: str, llm: Any) -> dict[str, Any]:
    """Full parse: phrases -> regex -> LLM -> fallback -> validate."""
    q = q.strip()
    phrases, rest = extract_phrases(q)
    raw, source = regex_precheck(rest), "regex"
    if raw is None:
        if phrases and not re.sub(PHRASE_FILLER, " ", rest, flags=re.I).strip(" ?.,:-"):
            raw, source = empty_raw(), "rules"  # only the quoted text: no LLM call needed
        else:
            try:
                raw, source = llm_extract(llm, rest), "llm"
            except Exception as e:  # noqa: BLE001
                logger.warning("LLM parser failed, using fallback: %r", e)
                raw, source = rule_based_parse(rest), "fallback"
    if phrases:
        raw = {**raw, "phrases": phrases, "intent": "PHRASE", "understood_as": ""}  # circular_no (if any) restricts the search
    return validate_plan(raw, source)
