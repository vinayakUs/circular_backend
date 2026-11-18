from ingestion.indexer.es_client import build_keywords_query


def _bool(keywords):
    return build_keywords_query(keywords, [])["bool"]


def _must_only(keywords):
    b = _bool(keywords)
    assert b["must_not"] == [] and b["filter"] == []
    return b["must"]


def test_bare_keyword_keeps_conjunctive_multi_match():
    assert _must_only(["algo trading"]) == [
        {"multi_match": {"query": "algo trading", "fields": ["title^2", "chunk_text"], "operator": "and"}}
    ]


def test_comma_groups_and():
    must = _must_only(["margin", "settlement"])
    assert [c["multi_match"]["query"] for c in must] == ["margin", "settlement"]


def test_or_group_becomes_should():
    must = _must_only(["margin OR settlement"])
    should = must[0]["bool"]["should"]
    assert [c["multi_match"]["query"] for c in should] == ["margin", "settlement"]
    assert must[0]["bool"]["minimum_should_match"] == 1


def test_quoted_phrase():
    must = _must_only(['"circuit breaker"'])
    assert must[0]["multi_match"]["type"] == "phrase"
    assert must[0]["multi_match"]["query"] == "circuit breaker"


def test_or_group_with_comma_precedence():
    b = _bool(["margin OR settlement, clearing"])
    assert [c["multi_match"]["query"] for c in b["must"][1:]] == ["clearing"]
    assert "should" in b["must"][0]["bool"]


def test_phrase_and_bare_in_one_alternative():
    inner = _must_only(['"circuit breaker" margin'])[0]["bool"]["must"]
    assert inner[0]["multi_match"]["type"] == "phrase"
    assert inner[1]["multi_match"]["query"] == "margin"


def test_exclusions_land_in_must_not():
    b = _bool(["margin OR -penalty"])
    assert b["must_not"] == [
        {"multi_match": {"query": "penalty", "fields": ["title^2", "chunk_text"], "operator": "and"}}
    ]
    assert b["must"][0]["multi_match"]["query"] == "margin"


def test_only_exclusions_match_all():
    b = _bool(["-penalty"])
    assert b["must"] == [{"match_all": {}}]
    assert b["must_not"][0]["multi_match"]["query"] == "penalty"


def test_empty_keywords_match_all():
    assert _must_only([]) == [{"match_all": {}}]
