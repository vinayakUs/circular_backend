import logging
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

from ingestion.processor.reference_extractor import (
    CandidateMatch,
    ReferenceExtractor,
    normalize_relationship,
)


class ReferenceExtractorRelationshipTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.extractor = ReferenceExtractor.__new__(ReferenceExtractor)
        self.extractor.logger = logging.getLogger("test.reference_extractor")

    @staticmethod
    def candidate(relationship: str = "REFERENCES") -> CandidateMatch:
        circular_id = "SEBI/HO/MRD/MRD-PoD-1/P/CIR/2023/136"
        return CandidateMatch(
            matched_text=circular_id,
            start=0,
            end=len(circular_id),
            source="SEBI",
            left_context="",
            right_context="",
            regex_relationship=relationship,
            regex_trigger="Master Circular",
        )

    def test_merge_normalizes_consolidates_to_references(self) -> None:
        result = self.extractor._merge_results(
            [self.candidate("CONSOLIDATES")],
            [],
        )

        self.assertEqual(result[0]["relationship_type"], "references")

    def test_merge_normalizes_partial_amendment_to_amends(self) -> None:
        candidate = self.candidate()
        result = self.extractor._merge_results(
            [candidate],
            [{
                "circular_id": candidate.matched_text,
                "relationship_nature": "Partial_Amendment",
            }],
        )

        self.assertEqual(result[0]["relationship_type"], "amends")
        self.assertEqual(result[0]["source"], "merged")

    def test_merge_falls_back_for_unknown_llm_relationship(self) -> None:
        result = self.extractor._merge_results(
            [],
            [{
                "circular_id": "SEBI/HO/CFD/CMD1/CIR/P/2020/106",
                "relationship_nature": "supersedes_in_part",
            }],
        )

        self.assertEqual(result[0]["relationship_type"], "references")

    def test_normalize_relationship_preserves_supported_value(self) -> None:
        self.assertEqual(normalize_relationship(" Clarifies "), "clarifies")


class ReferenceExtractorPersistenceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.extractor = ReferenceExtractor.__new__(ReferenceExtractor)
        self.extractor.logger = logging.getLogger("test.reference_extractor")
        self.extractor.reference_repo = MagicMock()
        self.source_id = uuid4()
        self.references = [{
            "circular_id": "SEBI/HO/MRD/MRD-PoD-1/P/CIR/2023/136",
            "relationship_type": "references",
        }]

    def test_persist_references_propagates_repository_error(self) -> None:
        self.extractor.reference_repo.replace_references.side_effect = RuntimeError(
            "insert failed",
        )

        with self.assertRaisesRegex(RuntimeError, "insert failed"):
            self.extractor._persist_references(self.source_id, self.references)

    def test_run_marks_task_failed_when_persistence_fails(self) -> None:
        self.extractor.processor_repo = MagicMock()
        self.extractor.reference_repo.replace_references.side_effect = RuntimeError(
            "insert failed",
        )
        self.extractor.process = lambda record: self.extractor._persist_references(
            record.id,
            self.references,
        )
        record = SimpleNamespace(id=self.source_id)

        success = self.extractor.run(record)

        self.assertFalse(success)
        self.extractor.processor_repo.mark_task_completed.assert_not_called()
        self.extractor.processor_repo.mark_task_failed.assert_called_once_with(
            self.source_id,
            self.extractor.name,
            "insert failed",
        )


if __name__ == "__main__":
    unittest.main()
