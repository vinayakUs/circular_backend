"""Tests for amendment-context injection in RAGAnswerGenerator."""
from __future__ import annotations

from unittest.mock import MagicMock

from ingestion.graph import Neo4jClient
from services.rag.answer_generator import RAGAnswerGenerator


def _make_search_hit(circular_id: str, chunk_text: str = "x"):
    """Build a SearchHit-like mock carrying the fields the generator reads."""
    doc = MagicMock()
    doc.circular_id = circular_id
    doc.title = f"Title for {circular_id}"
    doc.source = "NSE"
    doc.issue_date = MagicMock()
    doc.issue_date.isoformat.return_value = "2022-01-07"
    doc.chunk_text = chunk_text
    hit = MagicMock()
    hit.document = doc
    return hit


class TestAmendmentBlockFormatting:
    def test_deleted_block_includes_status_and_legal_basis(self):
        row = {
            "change_type": "deleted",
            "section_status": "deleted",
            "section_label": "12",
            "circular_id": "NSE/COMP/50957",
            "changer_id": "NSE/INSP/74836",
            "changer_issue_date": "2026-06-23",
            "effective_date": "2026-06-23",
            "raw_sentence": "Point No. 3 to 12 stand deleted.",
            "new_text": None,
        }
        block = RAGAnswerGenerator._format_amendment_block(row)
        assert block is not None
        assert "[AMENDMENT]" in block
        assert "DELETED" in block
        assert "NSE/COMP/50957" in block
        assert "NSE/INSP/74836" in block
        assert "Point No. 3 to 12 stand deleted." in block

    def test_modified_block_includes_replacement_text(self):
        row = {
            "change_type": "modified",
            "section_status": "modified",
            "section_label": "1",
            "circular_id": "NSE/COMP/50957",
            "changer_id": "NSE/INSP/74836",
            "changer_issue_date": "2026-06-23",
            "effective_date": "2026-06-23",
            "raw_sentence": "Point 1 is modified as below.",
            "new_text": "Issuing Corporate Guarantees...",
        }
        block = RAGAnswerGenerator._format_amendment_block(row)
        assert block is not None
        assert "MODIFIED" in block
        assert "Issuing Corporate Guarantees..." in block

    def test_no_change_type_returns_none(self):
        row = {"change_type": None, "section_status": "active"}
        assert RAGAnswerGenerator._format_amendment_block(row) is None

    def test_inconsistent_status_returns_none(self):
        # Section says active but edge says deleted — stale state, skip.
        row = {
            "change_type": "deleted",
            "section_status": "active",
            "section_label": "X",
            "circular_id": "C",
            "changer_id": "Y",
            "changer_issue_date": "2026-01-01",
            "effective_date": "2026-01-01",
            "raw_sentence": "...",
            "new_text": None,
        }
        assert RAGAnswerGenerator._format_amendment_block(row) is None


class TestAmendmentContextBuilding:
    """Verify _build_amendment_context calls Neo4j and formats blocks."""

    def _generator_with_mock_client(self, mock_rows):
        client = MagicMock(spec=Neo4jClient)
        client.database = "neo4j"
        # Patch fetch_amendments_for_circulars via patching the import site
        # the generator uses.
        import services.rag.answer_generator as ag_module
        original = ag_module.fetch_amendments_for_circulars
        ag_module.fetch_amendments_for_circulars = MagicMock(return_value=mock_rows)
        try:
            gen = RAGAnswerGenerator(neo4j_client=client)
            return gen, ag_module
        finally:
            ag_module.fetch_amendments_for_circulars = original

    def test_returns_empty_when_no_client(self):
        # When NEO4J_ENABLED is false, passing client=None keeps it None.
        from unittest.mock import patch
        with patch("services.rag.answer_generator.Config.NEO4J_ENABLED", False):
            gen = RAGAnswerGenerator(neo4j_client=None)
        assert gen.neo4j_client is None
        blocks = gen._build_amendment_context(
            [_make_search_hit("NSE/COMP/50957")]
        )
        assert blocks == []

    def test_returns_empty_when_no_hits(self):
        client = MagicMock(spec=Neo4jClient)
        gen = RAGAnswerGenerator(neo4j_client=client)
        assert gen._build_amendment_context([]) == []

    def test_returns_empty_when_neo4j_raises(self):
        client = MagicMock(spec=Neo4jClient)
        client.database = "neo4j"
        import services.rag.answer_generator as ag_module
        original = ag_module.fetch_amendments_for_circulars
        ag_module.fetch_amendments_for_circulars = MagicMock(
            side_effect=RuntimeError("Neo4j down")
        )
        try:
            gen = RAGAnswerGenerator(neo4j_client=client)
            blocks = gen._build_amendment_context(
                [_make_search_hit("NSE/COMP/50957")]
            )
            assert blocks == []
        finally:
            ag_module.fetch_amendments_for_circulars = original

    def test_dedup_circular_ids_passed_to_query(self):
        client = MagicMock(spec=Neo4jClient)
        client.database = "neo4j"
        import services.rag.answer_generator as ag_module
        original = ag_module.fetch_amendments_for_circulars
        mock_query = MagicMock(return_value=[])
        ag_module.fetch_amendments_for_circulars = mock_query
        try:
            gen = RAGAnswerGenerator(neo4j_client=client)
            hits = [
                _make_search_hit("NSE/COMP/50957"),
                _make_search_hit("NSE/COMP/50957"),  # duplicate
                _make_search_hit("NSE/INSP/74836"),
            ]
            gen._build_amendment_context(hits)
            called_with = mock_query.call_args.args[1]
            # Sorted + deduped
            assert called_with == ["NSE/COMP/50957", "NSE/INSP/74836"]
        finally:
            ag_module.fetch_amendments_for_circulars = original

    def test_only_consistent_rows_formatted_as_blocks(self):
        client = MagicMock(spec=Neo4jClient)
        client.database = "neo4j"
        rows = [
            {  # consistent — should produce a block
                "change_type": "deleted",
                "section_status": "deleted",
                "section_label": "12",
                "circular_id": "NSE/COMP/50957",
                "changer_id": "NSE/INSP/74836",
                "changer_issue_date": "2026-06-23",
                "effective_date": "2026-06-23",
                "raw_sentence": "stand deleted",
                "new_text": None,
            },
            {  # inconsistent — skipped
                "change_type": "deleted",
                "section_status": "active",
                "section_label": "5",
                "circular_id": "NSE/COMP/50957",
                "changer_id": "NSE/INSP/74836",
                "changer_issue_date": "2026-06-23",
                "effective_date": "2026-06-23",
                "raw_sentence": "...",
                "new_text": None,
            },
            {  # no change_type — skipped
                "change_type": None,
                "section_status": "active",
            },
        ]
        import services.rag.answer_generator as ag_module
        original = ag_module.fetch_amendments_for_circulars
        ag_module.fetch_amendments_for_circulars = MagicMock(return_value=rows)
        try:
            gen = RAGAnswerGenerator(neo4j_client=client)
            blocks = gen._build_amendment_context(
                [_make_search_hit("NSE/COMP/50957")]
            )
            assert len(blocks) == 1
            assert "[AMENDMENT]" in blocks[0]
            assert "DELETED" in blocks[0]
        finally:
            ag_module.fetch_amendments_for_circulars = original


class TestPromptAmendmentInjection:
    def _make_hit(self):
        return _make_search_hit("NSE/COMP/50957", chunk_text="some chunk text")

    def test_prompt_contains_amendment_section_when_blocks_present(self):
        gen = RAGAnswerGenerator(neo4j_client=None)
        chunks = gen._build_context_chunks([self._make_hit()])
        prompt = gen._build_prompt(
            "Is digital gold allowed?",
            chunks,
            amendment_context=[
                "[AMENDMENT] Section '12' was DELETED by NSE/INSP/74836",
            ],
        )
        assert "Amendment Context" in prompt
        assert "[AMENDMENT] Section '12' was DELETED" in prompt
        # Amendment rule in guidelines
        assert "AMENDMENT RULE" in prompt

    def test_prompt_omits_amendment_section_when_no_blocks(self):
        gen = RAGAnswerGenerator(neo4j_client=None)
        chunks = gen._build_context_chunks([self._make_hit()])
        prompt = gen._build_prompt(
            "Is digital gold allowed?", chunks, amendment_context=[],
        )
        assert "Amendment Context" not in prompt
