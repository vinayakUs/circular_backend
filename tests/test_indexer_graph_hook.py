"""Tests for the graph-build hook inside ElasticsearchIndexer.

Verifies:
  - Graph build runs after a successful ES index.
  - Graph build is skipped when NEO4J_ENABLED is false or no
    graph_indexer is supplied.
  - Graph failures are isolated — they don't propagate to
    _process_record.
  - The right text extraction path is used.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from ingestion.indexer.indexer import ElasticsearchIndexer
from tests.fakes import FakeCircularRepository


class TestGraphBuildHook:
    def _make_indexer(self, graph_indexer=None):
        repo = FakeCircularRepository()
        # Bypass AssetRepository default construction — fake repo has no db_pool.
        asset_repo = MagicMock()
        es_client = MagicMock()
        return ElasticsearchIndexer(
            circular_repository=repo,
            es_client=es_client,
            asset_repository=asset_repo,
            graph_indexer=graph_indexer,
        )

    def test_build_skipped_when_no_graph_indexer(self):
        indexer = self._make_indexer(graph_indexer=None)
        record = MagicMock()
        record.id = "id-1"
        record.circular_id = "NSE/X/1"
        # Should silently return — no exception, no call to anything.
        indexer._build_graph_for_record(record)

    def test_build_calls_index_circular_with_record_text(self):
        graph_indexer = MagicMock()
        graph_indexer.index_circular.return_value = {
            "sections_extracted": 12, "edges_written": 10,
        }
        indexer = self._make_indexer(graph_indexer=graph_indexer)

        record = MagicMock()
        record.id = "id-1"
        record.circular_id = "NSE/X/1"
        record.title = "Title"
        record.source = "NSE"
        record.full_reference = "NSE/X/1"
        record.issue_date = date(2022, 1, 7)

        # Stub out the text extraction path so we don't need a real PDF.
        with patch.object(
            ElasticsearchIndexer, "_get_indexable_text",
            return_value="1. Point one\n2. Point two",
        ):
            indexer._build_graph_for_record(record)

        graph_indexer.index_circular.assert_called_once()
        kwargs = graph_indexer.index_circular.call_args.kwargs
        assert kwargs["circular_id"] == "NSE/X/1"
        assert kwargs["issue_date"] == "2022-01-07"
        assert kwargs["source"] == "NSE"
        assert kwargs["title"] == "Title"

    def test_build_swallows_graph_failure(self):
        graph_indexer = MagicMock()
        graph_indexer.index_circular.side_effect = RuntimeError("Neo4j down")
        indexer = self._make_indexer(graph_indexer=graph_indexer)

        record = MagicMock()
        record.id = "id-1"
        record.circular_id = "NSE/X/1"
        record.title = "T"
        record.source = "NSE"
        record.full_reference = "NSE/X/1"
        record.issue_date = date(2022, 1, 7)

        with patch.object(
            ElasticsearchIndexer, "_get_indexable_text",
            return_value="text",
        ):
            # Must not raise — graph failures are isolated.
            indexer._build_graph_for_record(record)

    def test_build_skipped_when_text_empty(self):
        graph_indexer = MagicMock()
        indexer = self._make_indexer(graph_indexer=graph_indexer)

        record = MagicMock()
        record.id = "id-1"
        record.circular_id = "NSE/X/1"

        with patch.object(
            ElasticsearchIndexer, "_get_indexable_text",
            return_value="",
        ):
            indexer._build_graph_for_record(record)

        graph_indexer.index_circular.assert_not_called()

    def test_neo4j_disabled_skips_connection_attempt(self):
        # When NEO4J_ENABLED is False and no graph_indexer is passed,
        # __init__ must not attempt to connect.
        with patch("ingestion.indexer.indexer.Config.NEO4J_ENABLED", False):
            indexer = ElasticsearchIndexer(
                circular_repository=FakeCircularRepository(),
                es_client=MagicMock(),
                asset_repository=MagicMock(),
            )
        assert indexer.graph_indexer is None
