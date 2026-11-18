from app.notifications.keyword_syntax import parse_keywords


def test_comma_split_respects_quotes():
    parsed = parse_keywords(['"a, b", c'])
    assert len(parsed.groups) == 2
    assert [(a.text, a.phrase) for a in parsed.groups[0][0]] == [("a, b", True)]
    assert [a.text for a in parsed.groups[1][0]] == ["c"]


def test_or_and_pipe_build_alternatives():
    assert parse_keywords(["margin|settlement"]) == parse_keywords(["margin OR settlement"])
    parsed = parse_keywords(["margin OR settlement"])
    assert [[a.text for a in alt] for alt in parsed.groups[0]] == [["margin"], ["settlement"]]


def test_and_is_noop_but_lowercase_or_is_literal():
    parsed = parse_keywords(["margin AND settlement"])
    assert [[a.text for a in alt] for alt in parsed.groups[0]] == [["margin", "settlement"]]
    parsed = parse_keywords(["margin or settlement"])
    assert [a.text for a in parsed.groups[0][0]] == ["margin", "or", "settlement"]


def test_exclusions_extracted_from_anywhere():
    parsed = parse_keywords(['margin OR -penalty, NOT fine, -"big fine"'])
    assert [a.text for a in parsed.exclusions] == ["penalty", "fine", "big fine"]
    assert [[a.text for a in alt] for alt in parsed.groups[0]] == [["margin"]]


def test_unbalanced_quote_falls_back_to_bare_words():
    parsed = parse_keywords(['"circuit breaker'])
    assert [a.text for a in parsed.groups[0][0]] == ["circuit", "breaker"]


def test_lone_dash_and_blank_groups_ignored():
    parsed = parse_keywords(["-, , margin"])
    assert [[a.text for a in alt] for alt in parsed.groups[0]] == [["margin"]]


def test_negated_phrase_atom():
    parsed = parse_keywords(['-"circuit breaker"'])
    assert parsed.groups == ()
    assert [(a.text, a.phrase) for a in parsed.exclusions] == [("circuit breaker", True)]