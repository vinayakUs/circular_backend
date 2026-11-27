"""Behavioral tests for LLM-first circular reference extraction.

Uses a fake structured-output client: tests are deterministic and make no
network requests or database writes.
"""
from __future__ import annotations

import pytest

from ingestion.processor.reference_extractor import (
    MentionClassification,
    MentionExtraction,
    _evidence_span,
    chunk_text,
    extract_reference_edges,
)


class FakeLLM:
    def __init__(self, extraction: list[list[dict]], classifications: list[list[dict]]):
        self.extraction = list(extraction)
        self.classifications = list(classifications)
        self.calls: list[tuple[int, str]] = []

    def create_completions_parallel(self, *, prompts, model, response_model, **kwargs):
        self.calls.append((len(prompts), response_model.__name__))
        if response_model is MentionExtraction:
            assert len(prompts) == len(self.extraction)
            return [MentionExtraction.model_validate({"mentions": items}) for items in self.extraction]
        assert response_model is MentionClassification
        return [MentionClassification.model_validate({"relations": self.classifications.pop(0)})]


def run(text: str, llm: FakeLLM):
    return extract_reference_edges(
        text,
        source_circular_number="NSE/FAOP/99999",
        source_full_reference="NSE/FAOP/99999",
        llm_client=llm,
        model="fake",
    )


def mention(number: str, evidence: str) -> dict:
    return {"circular_number": number, "evidence": evidence}


def decision(index: int, relation: str, actor: bool = True) -> dict:
    return {
        "mention_id": str(index),
        "relationship_type": relation,
        "current_circular_is_actor": actor,
    }


def test_amend_and_reference_from_one_clause():
    sentence = (
        "This circular amends NSE/FAOP/73791 and refers to "
        "SEBI/HO/MIRSD/CRADT/CIR/P/2020/53."
    )
    llm = FakeLLM(
        extraction=[[mention("NSE/FAOP/73791", sentence),
                     mention("SEBI/HO/MIRSD/CRADT/CIR/P/2020/53", sentence)]],
        classifications=[[decision(0, "amends"), decision(1, "references", False)]],
    )
    edges = run(sentence, llm)
    assert {(x.circular_number, x.relationship_type) for x in edges} == {
        ("NSE/FAOP/73791", "amends"),
        ("SEBI/HO/MIRSD/CRADT/CIR/P/2020/53", "references"),
    }


def test_hallucinated_identifier_is_rejected_before_classification():
    text = "There is no earlier circular referenced here."
    llm = FakeLLM(
        extraction=[[mention("NSE/FAOP/73791", "NSE/FAOP/73791 was amended")]],
        classifications=[],
    )
    assert run(text, llm) == []
    assert llm.calls == [(1, "MentionExtraction")]


def test_current_circular_is_not_a_reference():
    text = "Circular NSE/FAOP/99999 is issued today."
    llm = FakeLLM(
        extraction=[[mention("NSE/FAOP/99999", text)]],
        classifications=[],
    )
    assert run(text, llm) == []


def test_passive_voice_does_not_reverse_graph_direction():
    text = "Circular NSE/FAOP/73791 was modified by NSE/FAOP/70000 in 2024."
    llm = FakeLLM(
        extraction=[[mention("NSE/FAOP/73791", text)]],
        classifications=[[decision(0, "modifies", False)]],
    )
    edges = run(text, llm)
    assert len(edges) == 1
    assert edges[0].relationship_type == "references"


def test_duplicate_mentions_deduplicate_target_and_relation():
    sentence = "This circular modifies NSE/FAOP/73791."
    text = sentence + "\n" + sentence
    llm = FakeLLM(
        extraction=[[mention("NSE/FAOP/73791", sentence)]],
        classifications=[[decision(0, "modifies")]],
    )
    edges = run(text, llm)
    assert len(edges) == 1
    assert edges[0].relationship_type == "modifies"


def test_no_partial_graph_on_failed_model_batch():
    text = "As stated in circular NSE/FAOP/73791."
    class FailingLLM:
        def create_completions_parallel(self, **kwargs):
            return [None]
    with pytest.raises(RuntimeError, match="incomplete"):
        run(text, FailingLLM())


def test_no_partial_graph_on_missing_classification():
    text = "This circular modifies NSE/FAOP/73791."
    llm = FakeLLM(
        extraction=[[mention("NSE/FAOP/73791", text)]],
        classifications=[[]],
    )
    with pytest.raises(RuntimeError, match="omitted"):
        run(text, llm)


def test_chunk_coverage_and_whitespace_grounding():
    text = "A" * 1900 + "\n" + "Circular NSE/FAOP/73791 applies." + "\n" + "B" * 5000
    chunks = chunk_text(text, max_chars=1200, overlap=250)
    assert "NSE/FAOP/73791" in "".join(chunks)
    assert len(chunks) > 1
    assert _evidence_span("See NSE/FAOP/\n73791", "NSE/FAOP/ 73791") is not None
