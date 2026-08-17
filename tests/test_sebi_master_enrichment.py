from __future__ import annotations

import io
from datetime import date
import unittest
from unittest.mock import patch

from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas

from ingestion.scrapper.base import IScraper, ScrapeDetectionResult
from ingestion.scrapper.dto import Circular
from ingestion.scrapper.orchestrator import ScraperOrchestrator
from ingestion.scrapper.sources.nse import NSEScraper
from ingestion.scrapper.sources.sebi import SEBIScraper
from ingestion.scrapper.sources.sebi_master import SEBIMasterCircularScraper
from tests.fakes import FakeCircularRepository


def _make_pdf(text: str) -> bytes:
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=LETTER)
    c.setFont("Helvetica", 12)
    c.drawString(72, 720, text)
    c.showPage()
    c.save()
    return buffer.getvalue()


EXTRACTED_REF = "HO/49/14/14(7)2025-CFD-POD2/I/3762/2026"
PDF_TEXT = (
    "MASTER CIRCULAR"
    "HO/49/14/14(7)2025-CFD-POD2/I/3762/2026"
    "Issued on: July 11, 2023"
)
PDF_BYTES = _make_pdf(PDF_TEXT)


def _build_circular(source: str, short_id: str = "101473") -> Circular:
    return Circular(
        source=source,
        circular_id=short_id,
        full_reference=short_id,
        department="ISD",
        title="Master Circular on Surveillance",
        issue_date=date(2026, 5, 15),
        applicable_to_nse=False,
        url="https://www.sebi.gov.in/legal/master-circulars/may-2026/x_101473.html",
        pdf_url="https://example.com/x.pdf",
        source_item_key="https://www.sebi.gov.in/legal/master-circulars/may-2026/x_101473.html",
    )


class _RecordingSEBIMaster(SEBIMasterCircularScraper):
    """SEBIMasterCircularScraper variant whose detect_new returns a fixed
    Circular so we can drive the orchestrator without network mocks."""

    def __init__(self, circular: Circular) -> None:
        super().__init__()
        self._circular = circular

    def detect_new(self, from_date, to_date):  # type: ignore[override]
        return ScrapeDetectionResult(circulars=[self._circular])


class SebiMasterEnrichmentTestCase(unittest.TestCase):
    def _build_orchestrator(
        self,
        source: IScraper,
        repository: FakeCircularRepository,
    ) -> ScraperOrchestrator:
        return ScraperOrchestrator(
            circular_repository=repository,
            checkpoint_repository=repository,
            asset_repository=repository,
            s3_client=None,
            default_lookback_days=7,
            from_date=date(2026, 5, 1),
            to_date=date(2026, 5, 30),
            enabled_sources=(source.source_name,),
        )

    def test_sebi_master_record_id_is_overwritten_with_extracted_reference(self) -> None:
        repository = FakeCircularRepository()
        source = _RecordingSEBIMaster(_build_circular("SEBI_MASTER", "101473"))

        orchestrator = self._build_orchestrator(source, repository)

        # Stub _download_assets and _fetch_pdf_bytes to avoid real network I/O.
        with (
            patch.object(
                orchestrator,
                "_download_assets",
                return_value=("local/path.pdf", "hash", []),
            ),
            patch.object(
                orchestrator,
                "_fetch_pdf_bytes",
                return_value=PDF_BYTES,
            ),
        ):
            orchestrator.run()

        records = repository.list_records()
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record.source, "SEBI_MASTER")
        self.assertEqual(record.circular_id, EXTRACTED_REF)
        self.assertEqual(record.full_reference, EXTRACTED_REF)
        self.assertEqual(record.status, "FETCHED")

    def test_sebi_master_marked_failed_when_extraction_returns_none(self) -> None:
        repository = FakeCircularRepository()
        source = _RecordingSEBIMaster(_build_circular("SEBI_MASTER", "101473"))

        orchestrator = self._build_orchestrator(source, repository)

        empty_pdf = _make_pdf("This PDF has no master circular reference.")

        with (
            patch.object(
                orchestrator,
                "_download_assets",
                return_value=("local/path.pdf", "hash", []),
            ),
            patch.object(
                orchestrator,
                "_fetch_pdf_bytes",
                return_value=empty_pdf,
            ),
        ):
            orchestrator.run()

        records = repository.list_records()
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record.status, "FAILED")
        self.assertIn(
            "Could not extract master circular reference",
            record.error_message or "",
        )

    def test_sebi_master_marked_failed_when_extractor_raises(self) -> None:
        repository = FakeCircularRepository()
        source = _RecordingSEBIMaster(_build_circular("SEBI_MASTER", "101473"))

        orchestrator = self._build_orchestrator(source, repository)

        with (
            patch.object(
                orchestrator,
                "_download_assets",
                return_value=("local/path.pdf", "hash", []),
            ),
            patch.object(
                orchestrator,
                "_fetch_pdf_bytes",
                side_effect=ConnectionError("refused"),
            ),
        ):
            orchestrator.run()

        records = repository.list_records()
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record.status, "FAILED")
        self.assertIn(
            "PDF re-fetch for reference extraction failed",
            record.error_message or "",
        )

    def test_other_scrapers_are_not_enriched(self) -> None:
        # SEBIScraper and NSEScraper must not declare enrich_reference_from_pdf.
        self.assertFalse(getattr(SEBIScraper, "enrich_reference_from_pdf", False))
        self.assertFalse(getattr(NSEScraper, "enrich_reference_from_pdf", False))


if __name__ == "__main__":
    unittest.main()