from datetime import datetime
from types import SimpleNamespace

from app.notifications.matching import TopicMatcher


def _hit(circular_db_id):
    doc = SimpleNamespace(
        circular_db_id=circular_db_id,
        circular_id="SEBI/2026/1",
        full_reference="SEBI/2026/1",
        title="Surveillance measure",
        source="SEBI",
        issue_date=datetime(2026, 11, 1),
    )
    return SimpleNamespace(document=doc)


class FakeEs:
    def __init__(self, hits):
        self._hits = hits

    def search_any_keywords(self, keywords, *, sources, since, size):
        return self._hits


class FakeRepo:
    def __init__(self, indexed):
        self._indexed = indexed
        self.queries = []

    def filter_indexed_ids(self, ids):
        self.queries.append(list(ids))
        return {i for i in ids if i in self._indexed}


TOPIC = SimpleNamespace(keywords=["margin"], sources=["NSE"])


def test_unindexed_hits_are_dropped():
    repo = FakeRepo(indexed={"a", "c"})
    matcher = TopicMatcher(FakeEs([_hit("a"), _hit("b"), _hit("c")]), repo)
    kept = matcher.match_window(TOPIC, now=datetime(2026, 11, 18), window_days=7)
    assert [m.id for m in kept] == ["a", "c"]
    assert repo.queries == [["a", "b", "c"]]


def test_all_indexed_pass_through():
    repo = FakeRepo(indexed={"a"})
    matcher = TopicMatcher(FakeEs([_hit("a")]), repo)
    kept = matcher.match_window(TOPIC, now=datetime(2026, 11, 18), window_days=7)
    assert [m.id for m in kept] == ["a"]


def test_no_hits_skips_db_check():
    repo = FakeRepo(indexed=set())
    matcher = TopicMatcher(FakeEs([]), repo)
    assert matcher.match_window(TOPIC, now=datetime(2026, 11, 18), window_days=7) == []
    assert repo.queries == []


def test_hit_without_db_id_is_dropped():
    repo = FakeRepo(indexed={"a"})
    matcher = TopicMatcher(FakeEs([_hit(None), _hit("a")]), repo)
    kept = matcher.match_window(TOPIC, now=datetime(2026, 11, 18), window_days=7)
    assert [m.id for m in kept] == ["a"]
    assert repo.queries == [["a"]]
