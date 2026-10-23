import pymupdf

from ingestion.indexer.pdf_extractor import PDFTextExtractor


def _make_pdf(path, pages):
    doc = pymupdf.open()
    for n in range(1, pages + 1):
        doc.new_page().insert_text((72, 72), f"PAGE {n}")
    doc.save(str(path))
    doc.close()


def test_extract_caps_pages(tmp_path):
    pdf = tmp_path / "d.pdf"
    _make_pdf(pdf, 25)

    text = PDFTextExtractor().extract(pdf, max_pages=10)

    assert "PAGE 10" in text
    assert "PAGE 11" not in text


def test_extract_unbounded_by_default(tmp_path):
    pdf = tmp_path / "d.pdf"
    _make_pdf(pdf, 25)

    assert "PAGE 25" in PDFTextExtractor().extract(pdf)


def test_extract_cap_above_page_count(tmp_path):
    pdf = tmp_path / "d.pdf"
    _make_pdf(pdf, 3)

    text = PDFTextExtractor().extract(pdf, max_pages=10)

    assert "PAGE 3" in text