import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from langchain_core.messages import AIMessage

from app.services.search_engine.plan import (
    extract_phrases, parse_query_text, regex_precheck, rule_based_parse, validate_plan,
)
from conftest import FakeChat, tool_call


def plan_args(**kw):
    base = {"intent": "SEARCH", "issuers": [], "categories": [], "date_phrase": None, "clean_query": "", "keywords": []}
    return {**base, **kw}


def test_regex_precheck_finds_circular_numbers():
    r = regex_precheck("SEBI/HO/MRD/MRD-PoD-3/P/CIR/2026/123")
    assert r["intent"] == "LOOKUP" and r["issuers"] == ["SEBI"]
    assert regex_precheck("NSE/FAOP/74180")["circular_no"] == "NSE/FAOP/74180"
    assert regex_precheck("margin framework") is None


def test_lookup_skips_llm():
    llm = FakeChat()
    plan = parse_query_text("NSE/FAOP/74180", llm)
    assert plan["intent"] == "LOOKUP" and plan["source"] == "regex" and llm.calls == []


def test_llm_plan_is_validated_by_code():
    llm = FakeChat([tool_call("QueryPlan", plan_args(issuers=["sebi", "RBI"], date_phrase="october", clean_query="KYC"))])
    plan = parse_query_text("SEBI KYC circulars in october", llm)
    assert plan["source"] == "llm"
    assert plan["issuers"] == ["SEBI"]                      # RBI is not a stored source -> dropped
    assert any("RBI" in w for w in plan["warnings"])
    assert plan["dates"]["from"].endswith("-10-01")


def test_filter_intent_is_forced_when_no_topic():
    llm = FakeChat([tool_call("QueryPlan", plan_args(issuers=["NSE"], date_phrase="last week", intent="VAGUE"))])
    assert parse_query_text("NSE last week", llm)["intent"] == "FILTER"


def test_llm_json_in_text_is_salvaged():
    llm = FakeChat([AIMessage(content='sure {"intent":"SEARCH","clean_query":"kyc"}')])
    plan = parse_query_text("kyc", llm)
    assert plan["source"] == "llm" and plan["clean_query"] == "kyc"


def test_fallback_when_llm_fails():
    llm = FakeChat([])  # raises on invoke
    plan = parse_query_text("NSE circulars about margin", llm)
    assert plan["source"] == "fallback"
    assert plan["issuers"] == ["NSE"] and plan["intent"] == "SEARCH" and plan["clean_query"] == "margin"


def test_quoted_text_becomes_phrase_intent_without_llm():
    llm = FakeChat()
    plan = parse_query_text('"permitted to trade"', llm)
    assert plan["intent"] == "PHRASE" and plan["phrases"] == ["permitted to trade"]
    assert plan["source"] == "rules" and llm.calls == []


def test_quote_with_filters_uses_llm_and_keeps_phrase():
    llm = FakeChat([tool_call("QueryPlan", plan_args(issuers=["NSE"], date_phrase="November 2026", intent="FILTER"))])
    plan = parse_query_text('NSE circulars in November 2026 "KRA"', llm)
    assert plan["intent"] == "PHRASE" and plan["phrases"] == ["KRA"] and plan["issuers"] == ["NSE"]


def test_extract_phrases_handles_curly_quotes():
    assert extract_phrases("find “exact words” please")[0] == ["exact words"]


def test_rule_based_question_and_compare():
    assert rule_based_parse("What is peak margin?")["intent"] == "QUESTION"
    assert rule_based_parse("compare nse vs sebi on algo")["intent"] == "COMPARE"
    assert validate_plan(rule_based_parse("latest changes"), "fallback")["intent"] in ("SEARCH", "VAGUE")
