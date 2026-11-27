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
    _find_identifier_span,
    chunk_text,
    extract_reference_edges,
    get_relationship_context,
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
    assert llm.calls == [(1, "MentionExtraction")] * 3


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

def test_sebi_2019_investment_policy_implicit_modification_uses_following_paragraph():
    """The 2019 SEBI circular changes the 2016 investment policy in paragraph 2."""
    source_id = "SEBI/HO/MRD2/DCAP/CIR/P/2019/145"
    old_id = "SEBI/HO/MRD/DRMNP/CIR/P/2016/54"
    reference_sentence = (
        "1. SEBI vide circular SEBI/HO/MRD/DRMNP/CIR/P/2016/54 dated "
        "May 04, 2016 permitted Clearing Corporations to invest in "
        "Fixed Deposits, Government Securities and Liquid Funds."
    )
    modification_paragraph = (
        "2. Upon a review of investment avenues available for Clearing "
        "Corporations, it has been decided to permit investments in "
        "Overnight Funds. Combined investments in Liquid Funds and Overnight "
        "Funds shall not exceed ten percent of total investible resources."
    )
    text = (
        f"CIRCULAR\\n{source_id} November 28, 2019\\n"
        "Subject: Investment Policy of Clearing Corporations\\n"
        f"{reference_sentence}\\n{modification_paragraph}"
    ).replace("\\n", "\n")

    class CapturingLLM(FakeLLM):
        def __init__(self):
            super().__init__(
                extraction=[[mention(old_id, reference_sentence)]],
                classifications=[[decision(0, "modifies")]],
            )
            self.classification_prompt = ""

        def create_completions_parallel(self, *, prompts, model, response_model, **kwargs):
            if response_model is MentionClassification:
                self.classification_prompt = prompts[0]
            return super().create_completions_parallel(
                prompts=prompts, model=model, response_model=response_model, **kwargs
            )

    llm = CapturingLLM()
    result = extract_reference_edges(
        text, source_circular_number=source_id, source_full_reference=source_id,
        llm_client=llm, model="fake",
    )
    assert len(result) == 1
    assert result[0].circular_number == old_id
    assert result[0].relationship_type == "modifies"
    # The actual purpose of this test: the relationship model sees paragraph 2,
    # although the referenced ID occurs only in paragraph 1.
    assert modification_paragraph in llm.classification_prompt
    assert reference_sentence in llm.classification_prompt


def test_long_document_relationship_context_includes_next_paragraph():
    reference = "SEBI/HO/MRD/DRMNP/CIR/P/2016/54"
    quote = "Earlier circular " + reference + " permitted only Liquid Funds."
    modification = "Now Overnight Funds are also permitted under the investment policy."
    text = "A" * 12500 + quote + "\n" + modification + "Z" * 5000

    context = get_relationship_context(text, quote)
    assert quote in context
    assert modification in context
    assert len(context) < len(text)


def test_unreliable_first_llm_response_retries_then_classifies():
    source_text = "This circular modifies NSE/FAOP/73791."
    class FirstAttemptEmpty(FakeLLM):
        def __init__(self):
            super().__init__(
                extraction=[[mention("NSE/FAOP/73791", "paraphrased quote")]],
                classifications=[[decision(0, "modifies")]],
            )
            self.extraction_count = 0

        def create_completions_parallel(self, *, prompts, model, response_model, **kwargs):
            if response_model is MentionExtraction:
                self.extraction_count += 1
                if self.extraction_count == 1:
                    self.calls.append((len(prompts), "MentionExtraction"))
                    return [MentionExtraction(mentions=[])]
            return super().create_completions_parallel(
                prompts=prompts, model=model, response_model=response_model, **kwargs
            )

    llm = FirstAttemptEmpty()
    edges = run(source_text, llm)
    assert len(edges) == 1
    assert edges[0].circular_number == "NSE/FAOP/73791"
    assert edges[0].relationship_type == "modifies"
    assert llm.calls == [
        (1, "MentionExtraction"),
        (1, "MentionExtraction"),
        (1, "MentionClassification"),
    ]


def test_inaccurate_llm_evidence_does_not_discard_correct_identifier():
    text = (
        "SEBI vide circular SEBI/HO/MRD/DRMNP/CIR/P/2016/54 dated "
        "May 04, 2016 permitted certain investments."
    )
    llm = FakeLLM(
        extraction=[[
            mention("SEBI/HO/MRD/DRMNP/CIR/P/2016/54",
                    "Incorrect quote: dated May 4, 2016")
        ]],
        classifications=[[decision(0, "modifies")]],
    )
    edges = run(text, llm)
    assert len(edges) == 1
    assert edges[0].circular_number == "SEBI/HO/MRD/DRMNP/CIR/P/2016/54"
    assert "May 04, 2016" in edges[0].evidence
    assert "Incorrect quote" not in edges[0].evidence


def test_line_wrapped_identifier_matches_original_source_without_regex():
    text = "Referenced: SEBI/HO/MRD/DRMNP/CIR/P/2016/\n54 dated May 04, 2016."
    target = "SEBI/HO/MRD/DRMNP/CIR/P/2016/54"
    span = _find_identifier_span(text, target)
    assert span is not None
    assert text[span[0]:span[1]] == "SEBI/HO/MRD/DRMNP/CIR/P/2016/\n54"

    llm = FakeLLM(
        extraction=[[{"circular_number": target}]],
        classifications=[[decision(0, "references", False)]],
    )
    edges = run(text, llm)
    assert len(edges) == 1
    assert edges[0].circular_number == target
    assert "\n54" in edges[0].evidence


def test_identifier_is_not_accepted_as_prefix_of_another_id():
    assert _find_identifier_span(
        "See NSE/FAOP/737910 for details.",
        "NSE/FAOP/73791",
    ) is None


def test_reference_free_chunk_retries_then_returns_empty():
    llm = FakeLLM(extraction=[[]], classifications=[])
    assert run("There are no circular identifiers in this text.", llm) == []
    assert llm.calls == [(1, "MentionExtraction")] * 3
