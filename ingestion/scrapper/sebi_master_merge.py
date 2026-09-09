"""Detect and merge chapter PDFs referenced from a SEBI master circular."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable
from io import BytesIO

import fitz
from pypdf import PdfReader, PdfWriter

logger = logging.getLogger(__name__)

INDEX_SEARCH_RANGE: tuple[int, int] = (0, 15)
INDEX_FOLLOW_PAGES: int = 2
MAX_CHAPTERS: int = 20

_CHAPTER_URL_RE = re.compile(
    r'https?://[^\s)>\]]+?\.pdf(?:\?[^\s)>\]]*)?',
    re.IGNORECASE,
)

# Each anchored on its own line so e.g. "index" inside body text doesn't trigger.
_INDEX_TITLE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^\s*table\s+of\s+contents:?\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*contents:?\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*index:?\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*annexures?:?\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*schedules?:?\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*enclosures?:?\s*$", re.IGNORECASE | re.MULTILINE),
)

ChapterFetcher = Callable[[str], bytes]


def extract_chapter_urls(
    main_pdf_bytes: bytes,
    main_pdf_url: str,
    *,
    max_chapters: int = MAX_CHAPTERS,
) -> list[str]:
    """Return URLs to OTHER PDFs found on the index pages of *main_pdf_bytes*."""
    doc = fitz.open(stream=main_pdf_bytes, filetype="pdf")
    try:
        urls: list[str] = []
        seen: set[str] = set()

        for page_idx in _locate_index_pages(doc):
            if len(urls) >= max_chapters:
                break
            page = doc.load_page(page_idx)

            for match in _CHAPTER_URL_RE.finditer(page.get_text("text")):
                _maybe_add(match.group(0), main_pdf_url, urls, seen, max_chapters)
                if len(urls) >= max_chapters:
                    break

            if len(urls) >= max_chapters:
                break
            for link in page.get_links():
                uri = link.get("uri")
                if uri and _looks_like_pdf_url(uri):
                    _maybe_add(uri, main_pdf_url, urls, seen, max_chapters)
                if len(urls) >= max_chapters:
                    break

        return urls
    finally:
        doc.close()


def merge_pdfs(main_bytes: bytes, chapter_bytes_list: Iterable[bytes]) -> bytes:
    """Append each chapter PDF to *main_bytes*. Returns the merged PDF bytes."""
    writer = PdfWriter()
    for page in PdfReader(BytesIO(main_bytes)).pages:
        writer.add_page(page)
    for chapter_bytes in chapter_bytes_list:
        for page in PdfReader(BytesIO(chapter_bytes)).pages:
            writer.add_page(page)
    out = BytesIO()
    writer.write(out)
    return out.getvalue()


def merge_sebi_master_chapters(
    main_pdf_bytes: bytes,
    main_pdf_url: str,
    chapter_fetcher: ChapterFetcher,
    *,
    max_chapters: int = MAX_CHAPTERS,
) -> bytes:
    """Return *main_pdf_bytes* with any chapter PDFs appended; unchanged if none."""
    chapter_urls = extract_chapter_urls(
        main_pdf_bytes, main_pdf_url, max_chapters=max_chapters
    )
    if not chapter_urls:
        logger.info(
            "No chapter PDF links found in index main_pdf_url=%s", main_pdf_url
        )
        return main_pdf_bytes

    logger.info(
        "Merging %d chapter PDF(s) into main main_pdf_url=%s chapters=%s",
        len(chapter_urls), main_pdf_url, chapter_urls,
    )
    chapter_bytes_list = [chapter_fetcher(url) for url in chapter_urls]
    merged = merge_pdfs(main_pdf_bytes, chapter_bytes_list)
    logger.info(
        "Merged main PDF with %d chapter(s) main_pdf_url=%s merged_bytes=%d",
        len(chapter_bytes_list), main_pdf_url, len(merged),
    )
    return merged


def _locate_index_pages(doc: fitz.Document) -> list[int]:
    """Return page indices to scan: [heading, heading+1, ...+INDEX_FOLLOW_PAGES], or []."""
    start, end = INDEX_SEARCH_RANGE
    end = min(end, doc.page_count)
    for page_idx in range(start, end):
        text = doc.load_page(page_idx).get_text("text")
        if any(pat.search(text) for pat in _INDEX_TITLE_PATTERNS):
            last = min(page_idx + INDEX_FOLLOW_PAGES, doc.page_count - 1)
            return list(range(page_idx, last + 1))

    logger.info("No index heading found in pages %d-%d; skipping scan", start, end - 1)
    return []


def _maybe_add(
    url: str,
    main_pdf_url: str,
    urls: list[str],
    seen: set[str],
    max_chapters: int,
) -> None:
    if len(urls) >= max_chapters or url in seen or _is_self_reference(url, main_pdf_url):
        return
    seen.add(url)
    urls.append(url)


def _looks_like_pdf_url(url: str) -> bool:
    url_lower = url.lower()
    if url_lower.endswith(".pdf") or ".pdf?" in url_lower:
        return True
    # SEBI sometimes serves PDFs without an explicit .pdf suffix.
    return "sebi.gov.in" in url_lower and "/legal/" in url_lower


def _is_self_reference(url: str, main_pdf_url: str) -> bool:
    if not main_pdf_url:
        return False
    url_base = url.split("#", 1)[0].split("?", 1)[0]
    main_base = main_pdf_url.split("#", 1)[0].split("?", 1)[0]
    return bool(url_base) and url_base == main_base