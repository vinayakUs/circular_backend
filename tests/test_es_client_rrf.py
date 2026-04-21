"""
Tests for ElasticsearchClient._combine_with_rrf

Run with: python -m pytest tests/test_es_client_rrf.py -v
"""
from __future__ import annotations

import unittest
from datetime import datetime
from typing import Any
from unittest.mock import Mock

from ingestion.indexer.dto import IndexDocument, SearchHit
from ingestion.indexer.es_client import ElasticsearchClient
from ingestion.indexer.embedding_provider import NoOpEmbeddingProvider


def _make_doc(chunk_id: str, text: str = "sample text") -> IndexDocument:
    """Minimal IndexDocument for testing."""
    return IndexDocument(
        chunk_id=chunk_id,
        circular_db_id="db-1",
        circular_id="circular-1",
        asset_id="asset-1",
        asset_role="original_pdf",
        source="SEBI",
        title="Test Circular",
        department="Legal",
        issue_date=datetime(2024, 1, 1).date(),
        effective_date=None,
        full_reference="REF-001",
        url="https://example.com/1",
        pdf_url="https://example.com/1.pdf",
        file_path="/tmp/1.pdf",
        archive_member_path=None,
        content_hash=None,
        chunk_index=0,
        chunk_text=text,
        chunk_text_contextual=None,
        embedding=None,
        indexed_at=datetime(2024, 1, 1),
    )


def _make_hit(es_id: str, score: float | None, doc: IndexDocument) -> SearchHit:
    return SearchHit(es_id=es_id, score=score, document=doc)


class CombineWithRRFTestCase(unittest.TestCase):
    """Tests for the RRF fusion logic in _combine_with_rrf."""

    def _client(self) -> ElasticsearchClient:
        """Build a client with a mock ES client (not actually used by _combine_with_rrf)."""
        raw_client = Mock()
        raw_client.search.return_value = {"hits": {"hits": []}}
        return ElasticsearchClient(
            url="http://localhost:9200",
            index_name="test_circulars_chunks",
            embedding_provider=NoOpEmbeddingProvider(),
            client=raw_client,
        )

    # ------------------------------------------------------------------
    # Helper to run _combine_with_rrf
    # ------------------------------------------------------------------
    def _run_rrf(
        self,
        bm25_hits: list[SearchHit],
        knn_hits: list[SearchHit],
        k: int = 60,
    ) -> list[SearchHit]:
        return self._client()._combine_with_rrf(bm25_hits, knn_hits, k=k)

    # ------------------------------------------------------------------
    # Test cases
    # ------------------------------------------------------------------

    def test_doc_in_both_retrievers_ranked_first(self) -> None:
        """A doc appearing in both BM25 and KNN should outrank docs in only one."""
        doc_a = _make_doc("doc_a", "content A")
        doc_b = _make_doc("doc_b", "content B")
        doc_c = _make_doc("doc_c", "content C")

        bm25_hits = [
            _make_hit("doc_a", 1.0, doc_a),   # rank 1 in BM25
            _make_hit("doc_b", 0.9, doc_b),   # rank 2 in BM25
            _make_hit("doc_c", 0.8, doc_c),   # rank 3 in BM25
        ]
        knn_hits = [
            _make_hit("doc_a", 0.9, doc_a),   # rank 1 in KNN
            _make_hit("doc_c", 0.85, doc_c),  # rank 2 in KNN
        ]

        results = self._run_rrf(bm25_hits, knn_hits)

        self.assertEqual(results[0].es_id, "doc_a")  # in both retrievers
        # doc_c is in both (rank 3 BM25 + rank 2 KNN), doc_b is BM25-only (rank 2)
        ids = [r.es_id for r in results]
        self.assertIn(ids[0], {"doc_a", "doc_c"})  # top spots should be dual-source
        self.assertIn("doc_b", ids)

    def test_returns_rrf_score_not_original_score(self) -> None:
        """Returned SearchHit.score should be the RRF fused score, not BM25/KNN score."""
        doc_a = _make_doc("doc_a")

        bm25_hits = [_make_hit("doc_a", 99.9, doc_a)]
        knn_hits = [_make_hit("doc_a", 0.01, doc_a)]

        results = self._run_rrf(bm25_hits, knn_hits)

        self.assertEqual(len(results), 1)
        # RRF score should be approximately 1/(60+1) + 1/(60+1) = 0.0328
        self.assertAlmostEqual(results[0].score, 0.0328, places=4)
        # Must NOT be the original BM25 score of 99.9
        self.assertNotEqual(results[0].score, 99.9)
        self.assertNotEqual(results[0].score, 0.01)

    def test_bm25_only_doc_included(self) -> None:
        """Docs only in BM25 (not KNN) should still appear in results."""
        doc_a = _make_doc("doc_a")
        doc_b = _make_doc("doc_b")

        bm25_hits = [
            _make_hit("doc_a", 9.0, doc_a),
            _make_hit("doc_b", 8.5, doc_b),
        ]
        knn_hits = [_make_hit("doc_a", 0.9, doc_a)]  # doc_b not in KNN

        results = self._run_rrf(bm25_hits, knn_hits)
        result_ids = [r.es_id for r in results]

        self.assertIn("doc_b", result_ids)
        self.assertIn("doc_a", result_ids)

    def test_knn_only_doc_included(self) -> None:
        """Docs only in KNN (not BM25) should still appear in results."""
        doc_a = _make_doc("doc_a")
        doc_c = _make_doc("doc_c")

        bm25_hits = [_make_hit("doc_a", 9.0, doc_a)]  # doc_c not in BM25
        knn_hits = [
            _make_hit("doc_a", 0.9, doc_a),
            _make_hit("doc_c", 0.85, doc_c),
        ]

        results = self._run_rrf(bm25_hits, knn_hits)
        result_ids = [r.es_id for r in results]

        self.assertIn("doc_c", result_ids)
        self.assertIn("doc_a", result_ids)

    def test_empty_bm25_hits(self) -> None:
        """BM25 returning nothing — KNN results should still be returned."""
        doc_a = _make_doc("doc_a")

        bm25_hits: list[SearchHit] = []
        knn_hits = [_make_hit("doc_a", 0.9, doc_a)]

        results = self._run_rrf(bm25_hits, knn_hits)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].es_id, "doc_a")

    def test_empty_knn_hits(self) -> None:
        """KNN returning nothing — BM25 results should still be returned."""
        doc_a = _make_doc("doc_a")

        bm25_hits = [_make_hit("doc_a", 9.0, doc_a)]
        knn_hits: list[SearchHit] = []

        results = self._run_rrf(bm25_hits, knn_hits)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].es_id, "doc_a")

    def test_empty_both(self) -> None:
        """Both retrievers empty — returns empty list (no crash)."""
        results = self._run_rrf([], [])
        self.assertEqual(results, [])

    def test_k_parameter_affects_rrf_score(self) -> None:
        """RRF score = 1/(k + rank). Different k values change the score."""
        doc_a = _make_doc("doc_a")

        bm25_hits = [_make_hit("doc_a", 9.0, doc_a)]
        knn_hits = [_make_hit("doc_a", 0.9, doc_a)]

        results_k10 = self._run_rrf(bm25_hits, knn_hits, k=10)
        results_k60 = self._run_rrf(bm25_hits, knn_hits, k=60)

        # k=10: 1/(10+1) + 1/(10+1) = 2/11 ≈ 0.1818
        # k=60: 1/(60+1) + 1/(60+1) = 2/61 ≈ 0.0328
        self.assertAlmostEqual(results_k10[0].score, 0.1818, places=4)
        self.assertAlmostEqual(results_k60[0].score, 0.0328, places=4)

    def test_bm25_wins_document_on_collision(self) -> None:
        """When a doc appears in both, BM25's SearchHit.document is used."""
        doc_a = _make_doc("doc_a", "from bm25")
        doc_a_knn = _make_doc("doc_a", "from knn")  # different text

        bm25_hits = [_make_hit("doc_a", 9.0, doc_a)]
        knn_hits = [_make_hit("doc_a", 0.9, doc_a_knn)]

        results = self._run_rrf(bm25_hits, knn_hits)

        # doc_map builds with KNN first, then BM25 overwrites — BM25 wins
        # So the returned document should have "from bm25" text
        self.assertEqual(results[0].document.chunk_text, "from bm25")

    def test_order_preserved(self) -> None:
        """Results should be sorted by RRF score descending (best first)."""
        doc_a = _make_doc("doc_a")
        doc_b = _make_doc("doc_b")
        doc_c = _make_doc("doc_c")
        doc_d = _make_doc("doc_d")

        bm25_hits = [
            _make_hit("doc_a", 9.0, doc_a),  # rank 1
            _make_hit("doc_b", 8.0, doc_b),  # rank 2
            _make_hit("doc_c", 7.0, doc_c),  # rank 3
            _make_hit("doc_d", 6.0, doc_d),  # rank 4
        ]
        knn_hits = [
            _make_hit("doc_a", 0.9, doc_a),  # rank 1
            _make_hit("doc_b", 0.8, doc_b),  # rank 2
            _make_hit("doc_c", 0.7, doc_c),  # rank 3
            _make_hit("doc_d", 0.6, doc_d),  # rank 4
        ]

        results = self._run_rrf(bm25_hits, knn_hits)
        scores = [r.score for r in results]

        # Scores should be strictly descending (no equal scores with 4 unique docs)
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_same_rank_different_retrievers_equal_scores(self) -> None:
        """A doc at rank X in BM25 and rank Y in KNN gets 1/(k+X) + 1/(k+Y)."""
        doc_a = _make_doc("doc_a")
        doc_b = _make_doc("doc_b")

        # doc_a: rank 1 in both → score = 1/(60+1) + 1/(60+1)
        # doc_b: rank 1 BM25, rank 2 KNN → score = 1/(60+1) + 1/(60+2)
        bm25_hits = [
            _make_hit("doc_a", 9.0, doc_a),
            _make_hit("doc_b", 8.0, doc_b),
        ]
        knn_hits = [
            _make_hit("doc_a", 0.9, doc_a),
            _make_hit("doc_b", 0.8, doc_b),
        ]

        results = self._run_rrf(bm25_hits, knn_hits)
        result_map = {r.es_id: r for r in results}

        # doc_a has higher score than doc_b (higher combined rank)
        self.assertGreater(result_map["doc_a"].score, result_map["doc_b"].score)


if __name__ == "__main__":
    unittest.main()