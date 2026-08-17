"""Tests for the ingestion.graph package.

Unit tests for deterministic pieces (schemas, prompts, KG schema,
section_id generation) and mocked Cypher for loader logic. Full
integration tests against a real Neo4j live in
``test_graph_integration.py`` and are gated behind ``--integration``.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ingestion.graph.kg_schema import (
    ALLOWED_ENTITY_TYPES,
    ALLOWED_RELATION_TYPES,
    KG_SCHEMA,
    RELATION_VALIDATION_SCHEMA,
)
from ingestion.graph.loader import GraphLoader
from ingestion.graph.prompts import (
    CHANGE_EXTRACTION_PROMPT,
    MAX_INPUT_CHARS,
    SECTION_EXTRACTION_PROMPT,
)
from ingestion.graph.schemas import (
    ChangeList,
    ChangeRelation,
    Section,
    SectionList,
)


# ======================================================================
# Schema tests
# ======================================================================


class TestSectionSchema:
    def test_minimal_section_defaults(self):
        s = Section(label="1", text="hello")
        assert s.label == "1"
        assert s.text == "hello"
        assert s.unit_type == "point"
        assert s.status == "active"
        assert s.superseded_text is None

    def test_all_unit_types_accepted(self):
        for unit_type in (
            "point", "question", "answer", "proviso",
            "annexure", "sub_rule", "other",
        ):
            s = Section(label="1", text="x", unit_type=unit_type)
            assert s.unit_type == unit_type

    def test_invalid_unit_type_rejected(self):
        with pytest.raises(ValueError):
            Section(label="1", text="x", unit_type="made_up")

    def test_invalid_status_rejected(self):
        with pytest.raises(ValueError):
            Section(label="1", text="x", status="archived")

    def test_section_list_round_trip(self):
        sl = SectionList(sections=[
            Section(label="1", text="a"),
            Section(label="2", text="b"),
        ])
        assert len(sl.sections) == 2
        dumped = sl.model_dump()
        assert dumped["sections"][0]["label"] == "1"


class TestChangeRelationSchema:
    def test_minimal_change_deleted(self):
        c = ChangeRelation(
            target_circular_id="NSE/COMP/50957",
            target_label="12",
            change_type="deleted",
            raw_sentence="Point 12 stands deleted.",
        )
        assert c.change_type == "deleted"
        assert c.new_text is None
        assert c.effective_date is None

    def test_change_modified_requires_new_text_field_but_nullable(self):
        c = ChangeRelation(
            target_circular_id="NSE/COMP/50957",
            target_label="1",
            change_type="modified",
            raw_sentence="Point 1 is modified.",
        )
        assert c.new_text is None  # allowed but should be set in practice

    def test_invalid_change_type_rejected(self):
        with pytest.raises(ValueError):
            ChangeRelation(
                target_circular_id="X",
                target_label="1",
                change_type="supersedes",  # not in v1 enum
                raw_sentence="...",
            )


# ======================================================================
# KG schema tests
# ======================================================================


class TestKGSchema:
    def test_entity_types_present(self):
        assert "Circular" in ALLOWED_ENTITY_TYPES
        assert "Section" in ALLOWED_ENTITY_TYPES

    def test_relation_types_include_changed_by(self):
        assert "CHANGED_BY" in ALLOWED_RELATION_TYPES

    def test_circular_has_required_properties(self):
        for prop in ("circular_id", "title", "issue_date"):
            assert prop in KG_SCHEMA["Circular"]

    def test_changed_by_validation_schema_has_required_properties(self):
        rel_props = RELATION_VALIDATION_SCHEMA["CHANGED_BY"]
        for prop in ("change_type", "new_text", "effective_date", "raw_sentence"):
            assert prop in rel_props


# ======================================================================
# Prompt tests
# ======================================================================


class TestPrompts:
    def test_section_prompt_has_input_placeholder(self):
        assert "{circular_text}" in SECTION_EXTRACTION_PROMPT

    def test_change_prompt_has_input_placeholder(self):
        assert "{circular_text}" in CHANGE_EXTRACTION_PROMPT

    def test_change_prompt_instructs_range_expansion(self):
        # Range expansion is critical — must be in the prompt.
        assert "range" in CHANGE_EXTRACTION_PROMPT.lower()
        assert "Point No. 3 to 12" in CHANGE_EXTRACTION_PROMPT

    def test_max_input_chars_is_positive(self):
        assert MAX_INPUT_CHARS > 0
        assert MAX_INPUT_CHARS >= 1000


# ======================================================================
# Loader — section_id generation + Cypher substitution
# ======================================================================


class TestGraphLoaderSectionId:
    def test_section_id_format(self):
        assert GraphLoader._section_id("NSE/COMP/50957", "12") == \
            "NSE/COMP/50957::12"

    def test_section_id_handles_colons_in_label(self):
        # Q&A labels contain colons — still valid for our composite key.
        assert GraphLoader._section_id("NSE/X", "Question No.6") == \
            "NSE/X::Question No.6"


class TestGraphLoaderCypherExecution:
    """Verify the loader passes the right parameters to Cypher.
    Uses a mock driver so tests don't need a running Neo4j."""

    def _client(self):
        client = MagicMock()
        client.user = "neo4j"
        client.password = "pw"
        client.uri = "bolt://localhost"
        client.database = "neo4j"
        # Mock the session context manager
        session = MagicMock()
        client.driver.session.return_value.__enter__.return_value = session
        client.driver.session.return_value.__exit__.return_value = False
        return client, session

    def test_upsert_circular_calls_cypher_once(self):
        client, session = self._client()
        loader = GraphLoader(client)
        loader._upsert_circular(
            circular_id="NSE/COMP/50957",
            title="Title",
            issue_date="2022-01-07",
            source="NSE",
            full_reference="ref",
            effective_date=None,
        )
        session.run.assert_called_once()
        cypher, params = session.run.call_args.args[0], session.run.call_args.kwargs
        assert "MERGE (c:Circular" in cypher
        assert params["circular_id"] == "NSE/COMP/50957"
        assert params["issue_date"] == "2022-01-07"

    def test_upsert_section_passes_status(self):
        client, session = self._client()
        loader = GraphLoader(client)
        loader._upsert_section(
            section_id="NSE/X::1",
            circular_id="NSE/X",
            label="1",
            unit_type="point",
            text="hello",
            status="active",
            superseded_text=None,
        )
        cypher, params = session.run.call_args.args[0], session.run.call_args.kwargs
        assert "MERGE (s:Section" in cypher
        assert params["section_id"] == "NSE/X::1"
        assert params["status"] == "active"

    def test_write_changed_by_edge_uses_merge_on_change_type(self):
        client, session = self._client()
        loader = GraphLoader(client)
        loader._write_changed_by_edge(
            target_section_id="NSE/X::1",
            source_circular_id="NSE/Y",
            change_type="deleted",
            new_text=None,
            effective_date="2026-06-23",
            raw_sentence="Point 1 stands deleted.",
        )
        cypher, params = session.run.call_args.args[0], session.run.call_args.kwargs
        # MERGE on (target, source, change_type) — multiple amendments stay distinct
        assert "MERGE (s)-[r:CHANGED_BY" in cypher
        assert "change_type: $change_type" in cypher
        assert params["change_type"] == "deleted"
        assert params["effective_date"] == "2026-06-23"

    def test_apply_amendment_status_handles_null_effective_date(self):
        client, session = self._client()
        loader = GraphLoader(client)
        loader._apply_amendment_status(
            section_id="NSE/X::1",
            change_type="deleted",
            new_text=None,
            effective_date=None,
        )
        cypher, params = session.run.call_args.args[0], session.run.call_args.kwargs
        assert params["effective_date"] is None
        assert "SET s.status" in cypher


# ======================================================================
# Module import smoke test
# ======================================================================


class TestModuleImports:
    def test_public_api_exports(self):
        from ingestion.graph import (
            Neo4jClient,
            CircularGraphExtractor,
            CircularGraphIndexer,
            GraphLoader,
            Section,
            SectionList,
            ChangeRelation,
            ChangeList,
            KG_SCHEMA,
            ALLOWED_ENTITY_TYPES,
            ALLOWED_RELATION_TYPES,
            RELATION_VALIDATION_SCHEMA,
            fetch_amendments_for_sections,
        )
        assert Neo4jClient is not None
        assert CircularGraphExtractor is not None
        assert CircularGraphIndexer is not None
        assert GraphLoader is not None
        assert fetch_amendments_for_sections is not None
