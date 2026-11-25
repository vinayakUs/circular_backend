from app.services.rag.citation_engine import (
    debracket_clause_ids,
    normalize_citation_markers,
)


def test_debracket_removes_lineage_brackets():
    # Chunker lineage headers wear [id] brackets that collide with [n] citations
    assert (
        debracket_clause_ids(
            "[11] General | [11.1] Scope | [11.1.1] Applicability | Content: x"
        )
        == "11 General | 11.1 Scope | 11.1.1 Applicability | Content: x"
    )


def test_normalize_strips_dotted_clause_ids_keeps_citations():
    # [11.1.1] can never be a citation; [3] is the RRF index and must survive
    assert (
        normalize_citation_markers("As per [11.1.1][3], the rule applies.")
        == "As per 11.1.1[3], the rule applies."
    )


def test_normalize_leaves_canonical_citations_untouched():
    assert normalize_citation_markers("See [1] and [2][3].") == "See [1] and [2][3]."
