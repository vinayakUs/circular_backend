from app.routes.topics import _validate


def test_create_requires_at_least_one_source():
    fields, error = _validate(
        {"topic_name": "Margins", "keywords": ["margin"], "sources": [], "alert_level": "DAILY"},
        partial=False,
    )
    assert fields == {}
    assert error == "at least one source is required"


def test_update_with_empty_sources_rejected():
    fields, error = _validate({"sources": []}, partial=True)
    assert fields == {}
    assert error == "at least one source is required"


def test_non_empty_sources_accepted():
    fields, error = _validate(
        {"topic_name": "Margins", "keywords": ["margin"], "sources": ["NSE"], "alert_level": "DAILY"},
        partial=False,
    )
    assert error is None
    assert fields["sources"] == ["NSE"]


def test_partial_update_without_sources_still_allowed():
    fields, error = _validate({"alert_level": "WEEKLY"}, partial=True)
    assert error is None
    assert fields == {"alert_level": "WEEKLY"}
