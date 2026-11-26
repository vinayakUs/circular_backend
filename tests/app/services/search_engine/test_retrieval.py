import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from contextlib import contextmanager

from app.services.search_engine.retrieval import Retrieval, phrase_regex
from conftest import FakeES, FakeRepo, make_hit

KYC_TEXT = ("Trading Members are advised that PANs whose status is On-Hold shall not be Permitted to Trade "
            "effective November 21, 2026. See the KYC (Know Your\nClient) Registration Agency list.")


class FakePool:
    def __init__(self, ids):
        self.ids, self.sql = ids, []

    @contextmanager
    def acquire(self):
        pool = self

        class Cur:
            def execute(self, sql, params=()):
                pool.sql.append((sql, params))

            def fetchall(self):
                return [(i,) for i in pool.ids]

        class Conn:
            def cursor(self):
                return Cur()

        yield Conn()


def make(recs, ids=()):
    es, pool = FakeES(), FakePool(list(ids))
    return Retrieval(FakeRepo(recs), es, pool), es, pool


def test_lookup_by_full_reference_then_circular_id(recs):
    rt, _, _ = make(recs)
    assert rt.lookup("nse/faop/74180")[0]["circular_id"] == "FAOP74180"
    assert rt.lookup("MFSS74181")[0]["match"] == "exact_reference"
    assert rt.lookup("nope") == []


def test_lookup_hides_inactive(recs):
    recs[0].is_active = False
    rt, _, _ = make(recs)
    assert rt.lookup("ISC77375") == []


def test_filter_builds_sql_and_keeps_order(recs):
    rt, _, pool = make(recs, ids=[recs[1].id, recs[0].id])
    rows = rt.filter(["NSE"], {"from": "2026-05-01", "to": "2026-05-31"}, limit=7)
    assert [r["circular_id"] for r in rows] == ["FAOP74180", "ISC77375"]
    sql, params = pool.sql[0]
    assert "source = ANY" in sql and "issue_date BETWEEN" in sql and "ORDER BY issue_date DESC" in sql
    assert params == (["NSE"], "2026-05-01", "2026-05-31", 7)


def test_title_search_passes_filters_and_scores_quality(recs):
    rt, es, _ = make(recs)
    es.responses = [[make_hit(recs[1], score=7.0), make_hit(recs[2], score=1.0)]]
    rows, quality = rt.title_search("dividend adjustment BANKINDIA", ["NSE"], {"from": "2026-05-01", "to": "2026-05-31"})
    flt = es.search_calls[0]["query"]["filter"]
    assert {"terms": {"source": ["NSE"]}} in flt
    assert any("range" in f and "gte" in f["range"]["issue_date"] for f in flt)
    assert [r["circular_id"] for r in rows] == ["FAOP74180"]    # MFSS74181 shares no words -> dropped
    assert quality["weak"] is False


def test_title_search_weak_when_nothing_close(recs):
    rt, es, _ = make(recs)
    es.responses = [[make_hit(recs[2])]]
    rows, quality = rt.title_search("peak margin commodity derivatives")
    assert rows == [] and quality["weak"] is True


def test_exact_title_is_never_weak(recs):
    rt, es, _ = make(recs)
    es.responses = [[make_hit(recs[2])]]
    rows, quality = rt.title_search("API for Mutual Fund Global MIS by Payment Aggregators")
    assert rows[0]["match"] == "exact_title" and quality["weak"] is False


def test_chunk_search_overlap_guard_and_filters(recs):
    rt, es, _ = make(recs)
    es.hybrid_hits = [make_hit(recs[0], KYC_TEXT), make_hit(recs[1], "Completely unrelated dividend text")]
    out = rt.chunk_search("PAN on hold KRA trading restriction", ["NSE"], None, circular_ids=["ISC77375"])
    assert [p["circular_id"] for p in out] == ["ISC77375"]
    (args, kwargs) = es.hybrid_calls[0]
    assert args[1] == {"source": ["NSE"], "circular_id": ["ISC77375"]} and kwargs["strategy"] == "hybrid"


def test_phrase_regex_ignores_case_punctuation_and_linebreaks_but_not_spelling():
    rx = phrase_regex("KYC (Know Your Client) Registration Agency")
    assert rx.search("the KYC (Know Your\nClient) Registration Agency list")
    assert phrase_regex("on hold").search("status is On-Hold")
    assert not phrase_regex("rationalisation").search("rationalization")


def test_phrase_search_verifies_text_and_rejects_stemmed_near_misses(recs):
    rt, es, _ = make(recs)
    near_miss = make_hit(recs[1], "Traders are not permitting trade here", idx=0)   # ES stemming would match this
    real = make_hit(recs[0], KYC_TEXT, idx=1)
    es.responses = [[near_miss, real], []]                                           # chunk query, title query
    hits, matches = rt.phrase_search(["permitted to trade"])
    assert [h["circular_id"] for h in hits] == ["ISC77375"]
    m = matches["ISC77375"]
    assert m["count"] == 1 and m["snippets"][0]["match"].lower() == "permitted to trade"


def test_phrase_search_requires_all_phrases(recs):
    rt, es, _ = make(recs)
    es.responses = [[make_hit(recs[0], KYC_TEXT)], [], [], []]   # phrase 1 found, phrase 2 not found anywhere
    hits, matches = rt.phrase_search(["permitted to trade", "margin framework"])
    assert hits == [] and matches == {}


def test_phrase_in_title(recs):
    rt, es, _ = make(recs)
    es.responses = [[], [make_hit(recs[0])]]
    hits, matches = rt.phrase_search(["Guidelines in pursuance of amendment"])
    assert matches["ISC77375"]["in_title"] and "«" in matches["ISC77375"]["title_marked"]
    assert hits[0]["match"] == "phrase_title"


def test_circular_id_filter_is_a_terms_filter(recs):
    _, es, _ = make(recs)
    assert es._build_filters({"circular_id": ["ISC77375"]}) == [{"terms": {"circular_id": ["ISC77375"]}}]
    assert es._build_filters({"source": ["nse"]}) == [{"terms": {"source": ["NSE"]}}]
