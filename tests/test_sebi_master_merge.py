"""Unit tests for ingestion.scrapper.sebi_master_merge.

Builds small PDFs on the fly with reportlab so the suite is self-contained —
no external fixture files required.
"""
from __future__ import annotations

import unittest
from io import BytesIO

import fitz
from pypdf import PdfReader
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from ingestion.scrapper.sebi_master_merge import (
    ChapterFetcher,
    _is_self_reference,
    _locate_index_pages,
    _looks_like_pdf_url,
    extract_chapter_urls,
    merge_pdfs,
    merge_sebi_master_chapters,
)


def _make_pdf(pages: list[str]) -> bytes:
    """Build an in-memory PDF where each entry in *pages* becomes one page."""
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=letter)
    for text in pages:
        for i, line in enumerate(text.split("\n")):
            c.drawString(72, 750 - 20 * i, line)
        c.showPage()
    c.save()
    return buf.getvalue()


def _add_link_annotation(pdf_bytes: bytes, page_idx: int, uri: str) -> bytes:
    """Add a URI link annotation on *page_idx* covering most of the page."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        page = doc.load_page(page_idx)
        rect = fitz.Rect(50, 50, page.rect.width - 50, page.rect.height - 50)
        page.insert_link({"kind": fitz.LINK_URI, "uri": uri, "from": rect})
        out = BytesIO()
        doc.save(out)
        return out.getvalue()
    finally:
        doc.close()


class IsSelfReferenceTests(unittest.TestCase):

    def test_exact_match_is_self_ref(self):
        self.assertTrue(_is_self_reference("https://x.com/a.pdf", "https://x.com/a.pdf"))

    def test_fragment_only_difference_is_self_ref(self):
        self.assertTrue(_is_self_reference(
            "https://x.com/a.pdf#page=5", "https://x.com/a.pdf"
        ))

    def test_query_only_difference_is_self_ref(self):
        self.assertTrue(_is_self_reference(
            "https://x.com/a.pdf?download=1", "https://x.com/a.pdf"
        ))

    def test_different_path_is_not_self_ref(self):
        self.assertFalse(_is_self_reference(
            "https://x.com/b.pdf", "https://x.com/a.pdf"
        ))

    def test_empty_main_url_is_not_self_ref(self):
        self.assertFalse(_is_self_reference("https://x.com/a.pdf", ""))


class LooksLikePdfUrlTests(unittest.TestCase):

    def test_pdf_suffix(self):
        self.assertTrue(_looks_like_pdf_url("https://x.com/a.pdf"))

    def test_pdf_with_query(self):
        self.assertTrue(_looks_like_pdf_url("https://x.com/a.pdf?x=1"))

    def test_sebi_legal_no_suffix(self):
        self.assertTrue(_looks_like_pdf_url("https://www.sebi.gov.in/legal/foo/bar"))

    def test_sebi_non_legal_no_suffix(self):
        self.assertFalse(_looks_like_pdf_url("https://www.sebi.gov.in/about/"))

    def test_non_sebi_html(self):
        self.assertFalse(_looks_like_pdf_url("https://example.com/page"))


class LocateIndexPagesTests(unittest.TestCase):

    def test_finds_table_of_contents_heading(self):
        pdf = _make_pdf([
            "body", "body", "body",
            "TABLE OF CONTENTS\nitem1\nitem 2",
            "more tocs", "more tocs", "more tocs",
        ])
        doc = fitz.open(stream=pdf, filetype="pdf")
        try:
            pages = _locate_index_pages(doc)
            self.assertEqual(pages, [3, 4, 5])
        finally:
            doc.close()

    def test_finds_enclosures_heading(self):
        pdf = _make_pdf(["body", "body", "body", "Enclosures:\nchapter1\nchapter 2"])
        doc = fitz.open(stream=pdf, filetype="pdf")
        try:
            self.assertEqual(_locate_index_pages(doc), [3])
        finally:
            doc.close()

    def test_no_heading_returns_empty_list(self):
        pdf = _make_pdf(["body"] * 20)
        doc = fitz.open(stream=pdf, filetype="pdf")
        try:
            self.assertEqual(_locate_index_pages(doc), [])
        finally:
            doc.close()

    def test_search_range_clamped_to_page_count(self):
        pdf = _make_pdf(["body", "body", "TABLE OF CONTENTS"])
        doc = fitz.open(stream=pdf, filetype="pdf")
        try:
            pages = _locate_index_pages(doc)
            self.assertEqual(pages, [2])
        finally:
            doc.close()


class ExtractChapterUrlsTests(unittest.TestCase):

    def test_extracts_from_text(self):
        pdf = _make_pdf([
            "body", "body", "body",
            "Enclosures:\nhttps://x.com/c1.pdf\nhttps://x.com/c2.pdf",
        ])
        urls = extract_chapter_urls(pdf, "https://main.com/main.pdf")
        self.assertEqual(
            urls, ["https://x.com/c1.pdf", "https://x.com/c2.pdf"]
        )

    def test_extracts_from_link_annotations(self):
        pdf = _make_pdf(["body", "body", "body", "Enclosures:"])
        pdf = _add_link_annotation(pdf, 3, "https://x.com/c1.pdf")
        pdf = _add_link_annotation(pdf, 3, "https://x.com/c2.pdf")
        urls = extract_chapter_urls(pdf, "https://main.com/main.pdf")
        self.assertEqual(len(urls), 2)
        self.assertIn("https://x.com/c1.pdf", urls)
        self.assertIn("https://x.com/c2.pdf", urls)

    def test_dedups(self):
        pdf = _make_pdf([
            "body", "body", "body",
            "Enclosures:\nhttps://x.com/c1.pdf\nhttps://x.com/c1.pdf",
        ])
        urls = extract_chapter_urls(pdf, "https://main.com/main.pdf")
        self.assertEqual(urls, ["https://x.com/c1.pdf"])

    def test_skips_self_references(self):
        pdf = _make_pdf([
            "body", "body", "body",
            "Enclosures:\nhttps://main.com/main.pdf\nhttps://x.com/c1.pdf",
        ])
        urls = extract_chapter_urls(pdf, "https://main.com/main.pdf")
        self.assertEqual(urls, ["https://x.com/c1.pdf"])

    def test_caps_at_max_chapters(self):
        urls_text = "\n".join(f"https://x.com/c{i}.pdf" for i in range(50))
        pdf = _make_pdf(["body", "body", "body", f"Enclosures:\n{urls_text}"])
        urls = extract_chapter_urls(
            pdf, "https://main.com/main.pdf", max_chapters=5
        )
        self.assertEqual(len(urls), 5)

    def test_no_heading_returns_empty(self):
        pdf = _make_pdf([
            "body", "body", "body",
            "just body text with https://x.com/c1.pdf",
        ])
        urls = extract_chapter_urls(pdf, "https://main.com/main.pdf")
        self.assertEqual(urls, [])


class MergePdfsTests(unittest.TestCase):

    def test_concatenates_in_order(self):
        a = _make_pdf(["page A"])
        b = _make_pdf(["page B"])
        c = _make_pdf(["page C"])
        merged = merge_pdfs(a, [b, c])
        self.assertEqual(len(PdfReader(BytesIO(merged)).pages), 3)

    def test_handles_empty_chapter_list(self):
        a = _make_pdf(["page A", "page A2"])
        merged = merge_pdfs(a, [])
        self.assertEqual(len(PdfReader(BytesIO(merged)).pages), 2)


class MergeSebiMasterChaptersTests(unittest.TestCase):

    def test_returns_main_unchanged_when_no_chapters(self):
        main_pdf = _make_pdf(["body only, no heading"])
        calls: list[str] = []
        fetcher: ChapterFetcher = lambda url: calls.append(url) or b""
        result = merge_sebi_master_chapters(
            main_pdf, "https://m/main.pdf", fetcher
        )
        self.assertEqual(result, main_pdf)
        self.assertEqual(calls, [])

    def test_fetches_and_merges_chapters(self):
        main_pdf = _make_pdf([
            "body", "body", "body",
            "Enclosures:\nhttps://x.com/c1.pdf",
        ])
        chapter_pdf = _make_pdf(["chapter 1"])
        calls: list[str] = []

        def fetcher(url: str) -> bytes:
            calls.append(url)
            return chapter_pdf

        merged = merge_sebi_master_chapters(
            main_pdf, "https://m/main.pdf", fetcher
        )
        self.assertEqual(calls, ["https://x.com/c1.pdf"])
        self.assertEqual(len(PdfReader(BytesIO(merged)).pages), 5)

    def test_fetch_failure_propagates(self):
        main_pdf = _make_pdf([
            "body", "body", "body",
            "Enclosures:\nhttps://x.com/c1.pdf",
        ])

        def failing_fetcher(url: str) -> bytes:
            raise RuntimeError(f"network down for {url}")

        with self.assertRaises(RuntimeError) as ctx:
            merge_sebi_master_chapters(
                main_pdf, "https://m/main.pdf", failing_fetcher
            )
        self.assertIn("network down", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()