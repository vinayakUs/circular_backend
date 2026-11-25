from app.services.search_engine.coverage import document_passages


class CircularIndex:
    def chunks_for(self, ids, per):
        assert ids == ["IA169"] and per >= 5
        sections = ["Free trials", "Risk profiling and consent", "Payment channels",
                    "Complaints disclosure", "Effective date"]
        return [{"circular_id": "IA169", "text": section} for section in sections]


def test_measures_in_separate_sections_reach_the_answer_context():
    matches = [{"circular_id": "IA169", "text": "Free trials"}]
    result = document_passages(CircularIndex(), "IA169", matches)
    assert [p["text"] for p in result] == [
        "Free trials", "Risk profiling and consent", "Payment channels",
        "Complaints disclosure", "Effective date"]
    assert len(matches) == 1


def test_unavailable_extra_sections_preserve_ranked_matches():
    class Offline:
        def chunks_for(self, ids, per):
            raise OSError("offline")

    matches = [{"circular_id": "IA169", "text": "Free trials"}]
    assert document_passages(Offline(), "IA169", matches) == matches
