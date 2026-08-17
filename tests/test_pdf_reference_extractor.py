from __future__ import annotations

import io
import unittest

from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas

from ingestion.scrapper.pdf_reference_extractor import (
    _looks_like_reference,
    extract_master_circular_reference,
)


def _make_pdf_with_text(text: str) -> bytes:
    """Generate a one-page PDF whose extracted text matches `text` exactly.

    ReportLab + pypdf extraction is reliable enough that the round-trip
    preserves ASCII text for testing purposes.
    """
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=LETTER)
    c.setFont("Helvetica", 12)
    c.drawString(72, 720, text)
    c.showPage()
    c.save()
    return buffer.getvalue()


class LooksLikeReferenceTestCase(unittest.TestCase):
    def test_accepts_real_reference(self) -> None:
        self.assertTrue(
            _looks_like_reference("HO/49/14/14(7)2025-CFD-POD2/I/3762/2026")
        )

    def test_accepts_longer_reference(self) -> None:
        self.assertTrue(
            _looks_like_reference("SEBI/HO/MIRSD/MIRSD-SEC-FATF/P/2026/12345")
        )

    def test_rejects_lowercase_start(self) -> None:
        self.assertFalse(_looks_like_reference("ho/49/14/14(7)2025/1234/2026"))

    def test_rejects_too_few_slashes(self) -> None:
        # Only one slash — doesn't look like a SEBI master circular reference
        self.assertFalse(_looks_like_reference("HO/49"))

    def test_rejects_too_short(self) -> None:
        self.assertFalse(_looks_like_reference("AB/CD"))

    def test_rejects_too_long(self) -> None:
        self.assertFalse(_looks_like_reference("A" + "B" * 250))

    def test_rejects_invalid_chars(self) -> None:
        self.assertFalse(
            _looks_like_reference("HO/49/MIRSD,COMPLIANCE/2026/1234")
        )


class ExtractMasterCircularReferenceTestCase(unittest.TestCase):
    def test_extracts_reference_from_real_sample_layout(self) -> None:
        # Real pypdf extraction produces single-line output with no whitespace
        # between the header and the reference.
        text = (
            "MASTER CIRCULARHO/49/14/14(7)2025-CFD-POD2/I/3762/2026Issued on: "
            "July 11, 2023Last updated on: January 30, 2026"
        )
        pdf_bytes = _make_pdf_with_text(text)

        result = extract_master_circular_reference(pdf_bytes)

        self.assertEqual(result, "HO/49/14/14(7)2025-CFD-POD2/I/3762/2026")

    def test_extracts_reference_with_space_after_header(self) -> None:
        text = (
            "MASTER CIRCULAR HO/49/14/14(7)2025-CFD-POD2/I/3762/2026 "
            "Issued on: July 11, 2023"
        )
        pdf_bytes = _make_pdf_with_text(text)

        result = extract_master_circular_reference(pdf_bytes)

        self.assertEqual(result, "HO/49/14/14(7)2025-CFD-POD2/I/3762/2026")

    def test_extracts_reference_with_multiline_layout(self) -> None:
        text = (
            "MASTER CIRCULAR\n"
            "HO/49/14/14(7)2025-CFD-POD2/I/3762/2026\n"
            "Issued on: July 11, 2023\n"
            "Sub: Master Circular for compliance with the provisions..."
        )
        pdf_bytes = _make_pdf_with_text(text)

        result = extract_master_circular_reference(pdf_bytes)

        self.assertEqual(result, "HO/49/14/14(7)2025-CFD-POD2/I/3762/2026")

    def test_extracts_lowercase_header(self) -> None:
        text = (
            "Master Circular SEBI/HO/MIRSD/P/2026/12345 "
            "Issued on: January 1, 2026"
        )
        pdf_bytes = _make_pdf_with_text(text)

        result = extract_master_circular_reference(pdf_bytes)

        self.assertEqual(result, "SEBI/HO/MIRSD/P/2026/12345")

    def test_ignores_body_master_circular_mentions(self) -> None:
        # The "Sub: Master Circular for compliance with..." body text must NOT
        # be picked up — only the header reference counts.
        text = (
            "MASTER CIRCULARHO/49/14/14(7)2025-CFD-POD2/I/3762/2026Issued on: "
            "July 11, 2023Sub: Master Circular for compliance with the "
            "provisions of the Securities and Exchange Board of India"
        )
        pdf_bytes = _make_pdf_with_text(text)

        result = extract_master_circular_reference(pdf_bytes)

        self.assertEqual(result, "HO/49/14/14(7)2025-CFD-POD2/I/3762/2026")

    def test_returns_none_when_no_master_circular_header(self) -> None:
        text = "This is some unrelated PDF content about regulations."
        pdf_bytes = _make_pdf_with_text(text)

        self.assertIsNone(extract_master_circular_reference(pdf_bytes))

    def test_returns_none_on_garbage_bytes(self) -> None:
        self.assertIsNone(extract_master_circular_reference(b"not a pdf"))

    def test_returns_none_on_empty_bytes(self) -> None:
        self.assertIsNone(extract_master_circular_reference(b""))


if __name__ == "__main__":
    unittest.main()