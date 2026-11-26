import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from types import SimpleNamespace

from langchain_core.messages import AIMessage

from app.services.search_engine.answer import finalize_citations
from app.services.search_engine.graph import SearchEngine
from app.services.search_engine.retrieval import record_to_row
from conftest import FakeChat, tool_call


def plan_call(**kw):
    base = {"intent": "SEARCH", "issuers": [], "categories": [], "date_phrase": None, "clean_query": "", "keywords": []}
    return tool_call("QueryPlan", {**base, **kw}, id="p1")


class FakeRetrieval:
    """Same interface as Retrieval, backed by in-memory rows."""

    def __init__(self, recs):
        self.rows = [record_to_row(r) for r in recs]
        self.title_result = ([], {"weak": True, "reason": "none"})
        self.passages: list = []
        self.phrase_result = ([], {})
        self.calls: list = []

    def lookup(self, no):
        self.calls.append(("lookup", no))
        return [r for r in self.rows if no.upper() in (r["full_reference"].upper(), r["circular_id"].upper())]

    def filter(self, issuers, dates, limit=50):
        self.calls.append(("filter", list(issuers), dates))
        return [r for r in self.rows if not issuers or r["source"] in issuers]

    def title_search(self, q, issuers=(), dates=None, limit=10):
        self.calls.append(("title", q))
        return self.title_result

    def chunk_search(self, q, issuers=(), dates=None, circular_ids=None, limit=6, per_circular=2):
        self.calls.append(("chunks", q, circular_ids))
        return self.passages

    def chunks_for(self, ids, per=2):
        return []

    def phrase_search(self, phrases, *a, **k):
        return self.phrase_result

    def attach_summaries(self, rows, top=5):
        pass


def engine(recs, parser=None, agent=None, summary=None, rt=None):
    rt = rt or FakeRetrieval(recs)
    models = SimpleNamespace(parser=parser or FakeChat(), agent=agent or FakeChat(), summary=summary or FakeChat())
    return SearchEngine(rt, models), rt


def test_lookup_goes_straight_to_compose(recs):
    eng, rt = engine(recs)
    out = eng.run("NSE/FAOP/74180")
    assert out["route"] == "run_query" and out["response"]["mode"] == "LOOKUP"
    assert out["response"]["results"][0]["circular_id"] == "FAOP74180"
    assert out["response"]["answer"] is None


def test_filter_query(recs):
    eng, _ = engine(recs, parser=FakeChat([plan_call(intent="FILTER", issuers=["NSE"], date_phrase="12 May 2026")]))
    out = eng.run("NSE circulars 12 May 2026")
    assert out["response"]["mode"] == "FILTER" and len(out["response"]["results"]) == 3
    assert out["response"]["facets"]["source"] == {"NSE": 3}


def test_not_found_message(recs):
    eng, _ = engine([], parser=FakeChat([plan_call(intent="FILTER", issuers=["NSE"])]))
    assert eng.run("NSE circulars")["response"]["message"] == "No circulars found for issuer NSE."


def test_search_adds_cited_summary_and_numbers_citations(recs):
    rt = FakeRetrieval(recs)
    kyc = rt.rows[0]
    rt.title_result = ([{**kyc, "match": "title", "score": 1.0}], {"weak": False, "reason": "ok"})
    rt.passages = [{**kyc, "text": "PANs on hold cannot trade from November 21, 2026.", "chunk_index": 0, "page": None}]
    summary = FakeChat([AIMessage(content="PANs that are On-Hold cannot trade from November 21, 2026 [ISC77375]. Also [ZZZ99999].")])
    eng, _ = engine(recs, parser=FakeChat([plan_call(clean_query="KYC")]), summary=summary, rt=rt)
    out = eng.run("KYC circulars")
    r = out["response"]
    assert r["summary"]["text"].startswith("PANs that are On-Hold cannot trade from November 21, 2026 [1].")
    assert "ZZZ99999" not in r["summary"]["text"]
    assert [s["circular_id"] for s in r["summary"]["sources"]] == ["ISC77375"]
    assert any("ZZZ99999" in w for w in r["warnings"])


def test_weak_search_escalates_to_agent_and_verify_drops_unseen_ids(recs):
    rt = FakeRetrieval(recs)
    agent = FakeChat([
        tool_call("get_circular", {"circular_id": "FAOP74180"}, id="a1"),
        tool_call("final_answer", {"answer": "The dividend adjustment is described in [FAOP74180] and [MFSS74181].",
                                   "circular_ids": ["FAOP74180", "MFSS74181", "INVENTED1"]}, id="a2"),
    ])
    eng, _ = engine(recs, parser=FakeChat([plan_call(clean_query="dividend adjustment")]), agent=agent, rt=rt)
    out = eng.run("dividend adjustment")
    assert out["route"] == "agent (escalated)" and out["steps"] == 2
    r = out["response"]
    assert [x["circular_id"] for x in r["results"]] == ["FAOP74180"]          # only tool-returned ids survive
    assert r["answer"] == "The dividend adjustment is described in [1] and ."   # [MFSS74181] was never returned by a tool
    assert r["summary"]["sources"][0]["circular_id"] == "FAOP74180"
    assert any("INVENTED1" in w for w in r["warnings"])


def test_agent_clarification_for_vague_query(recs):
    agent = FakeChat([tool_call("final_answer", {"clarification": "Which exchange and what period?"})])
    eng, _ = engine(recs, parser=FakeChat([plan_call(intent="VAGUE", clean_query="changes")]), agent=agent)
    r = eng.run("latest changes")["response"]
    assert r["mode"] == "ANSWER" and r["message"] == "Which exchange and what period?"


def test_agent_step_cap(recs):
    loop = [tool_call("get_circular", {"circular_id": "FAOP74180"}, id=f"c{i}") for i in range(10)]
    eng, _ = engine(recs, parser=FakeChat([plan_call(intent="QUESTION", clean_query="x")]), agent=FakeChat(loop))
    out = eng.run("what is x?")
    assert out["steps"] == 5 and "agent hit the step cap without a final answer" in out["response"]["warnings"]


def test_phrase_query_never_calls_agent(recs):
    rt = FakeRetrieval(recs)
    row = {**rt.rows[0], "match": "phrase_body", "score": 2}
    snippet = {"page": None, "chunk": 0, "before": "shall not be", "match": "Permitted to Trade", "after": "effective"}
    rt.phrase_result = ([row], {"ISC77375": {"in_title": False, "count": 1, "snippets": [snippet], "title_marked": None,
                                              "phrases": ["permitted to trade"]}})
    summary = FakeChat([AIMessage(content="The phrase is part of a trading restriction rule [ISC77375].")])
    agent = FakeChat()
    eng, _ = engine(recs, summary=summary, agent=agent, rt=rt)
    out = eng.run('"permitted to trade"')
    r = out["response"]
    assert out["route"] == "run_query" and agent.calls == [] and r["mode"] == "PHRASE"
    assert "[1]" in r["summary"]["text"] and "Found in 1 circular" in r["exact_matches"]["text"]


def test_phrase_without_match_is_not_escalated(recs):
    eng, _ = engine(recs)
    r = eng.run('"margin framework"')["response"]
    assert "exact phrase" in r["message"] and r["results"] == []


def test_finalize_citations_leaves_non_citations_alone(recs):
    rt = FakeRetrieval(recs)
    text, sources, warnings = finalize_citations("Status [On-Hold] then [ISC77375]", {"ISC77375"}, rt.lookup)
    assert text == "Status [On-Hold] then [1]" and sources[0]["n"] == 1 and warnings == []


def test_group_hits_collapses_same_title(recs):
    from app.services.search_engine.answer import group_hits
    a = record_to_row(recs[1])
    b = {**a, "circular_id": "FAOP74179", "issue_date": "2026-05-10"}
    groups = group_hits([b, a])
    assert len(groups) == 1 and groups[0]["similar_count"] == 1 and groups[0]["latest"]["circular_id"] == "FAOP74180"


def test_cli_render_and_trace_guard(recs, monkeypatch, capsys):
    from app.services.search_engine.__main__ import main, render
    eng, _ = engine(recs)
    out = eng.run("NSE/FAOP/74180")
    text = render(out)
    assert "intent=LOOKUP" in text and "FAOP74180" in text and "CIRCULARS (1 cards)" in text
    monkeypatch.delenv("LANGSMITH_API_KEY", raising=False)
    assert main(["--trace", "kyc"]) == 2
