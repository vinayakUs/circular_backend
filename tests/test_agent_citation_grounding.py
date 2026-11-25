from app.services.search_engine.grounding import collect_evidence, grounded_lines


def test_weak_hits_cannot_support_claims():
    records = {}
    collect_evidence(records, "search_circulars", {"quality": {"weak": True},
                     "results": [{"circular_id": "NSE123", "title": "Margin"}]})
    assert grounded_lines("The margin increased [NSE123].", records, lambda *_: True) == ("", True)


def test_only_supported_lines_survive():
    records = {}
    passage = "The margin remains at 15%."
    collect_evidence(records, "search_chunks", {"results": [
        {"circular_id": "NSE123", "text": passage, "page": 3}]})
    supported = "The margin remains at 15% [NSE123]."
    unsupported = "The margin increased to 15% [NSE123]."

    def verifier(line, cited):
        assert cited == {"NSE123": [{"text": passage, "page": 3}]}
        return line == supported

    assert grounded_lines(supported + "\n" + unsupported, records, verifier) == (supported, True)


def test_summary_is_not_passage_evidence():
    records = {}
    collect_evidence(records, "get_circular", {"results": [
        {"circular_id": "NSE123", "title": "Margin", "summary": "An increase"}]})
    assert records == {"NSE123": [{"title": "Margin"}]}


def test_verifier_failure_and_missing_citations():
    def unavailable(*args):
        raise RuntimeError("offline")

    assert grounded_lines("Rate changed [NSE123].\nUncited.", {"NSE123": [{}]}, unavailable) == ("", True)
