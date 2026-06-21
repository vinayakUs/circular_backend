from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import NamedTuple

from config import Config
from storage.s3_client import S3StorageClient


class PDFTextExtractor:
    """Extracts text from PDF files using pymupdf."""

    def extract(self, path: str | Path) -> str:
        """Extracts text from PDF files using PyMuPDF."""
        target = Path(path)

        if not target.exists() and not (
            Config.AWS_S3_BUCKET and str(path).startswith("s3://")
        ):
            raise FileNotFoundError(f"PDF file not found: {path}")

        try:
            import pymupdf
        except ImportError as exc:
            raise RuntimeError(
                "PyMuPDF is not installed. Install dependencies before running the indexer."
            ) from exc

        if str(path).startswith("s3://"):
            s3_client = S3StorageClient()
            pdf_bytes = s3_client.download_bytes(str(path))
            if not pdf_bytes.startswith(b"%PDF"):
                raise ValueError(
                    f"Downloaded content from {path} is not a valid PDF "
                    f"(starts with: {pdf_bytes[:50]!r}). "
                    f"Check that the S3 URL is correct and the object exists."
                )
            with TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir) / "doc.pdf"
                tmp_path.write_bytes(pdf_bytes)
                doc = pymupdf.open(str(tmp_path))
        else:
            doc = pymupdf.open(str(target))

        text_parts: list[str] = []
        for page in doc:
            text_parts.append(page.get_text() or "")
        doc.close()
        return "\n".join(text_parts).strip()


class Block(NamedTuple):
    """A block of content from a PDF page."""
    kind: str          # "text" | "table"
    content: str       # plain text or markdown table string
    page: int
    top: float         # y-position on page (for reading-order sort)


class PDFPlumberExtractor:
    """Extracts structured blocks (text + tables) from PDFs using pdfplumber.

    Handles S3 paths same as PDFTextExtractor.
    """

    def extract_blocks(self, path: str | Path) -> list[Block]:
        """Extract blocks from a PDF file, returning text and table blocks.

        Handles S3 paths by downloading to a temp file first.
        """
        target = Path(path)

        if not target.exists() and not (
            Config.AWS_S3_BUCKET and str(path).startswith("s3://")
        ):
            raise FileNotFoundError(f"PDF file not found: {path}")

        try:
            import pdfplumber
        except ImportError as exc:
            raise RuntimeError(
                "pdfplumber is not installed. Install dependencies before running the indexer."
            ) from exc

        if str(path).startswith("s3://"):
            s3_client = S3StorageClient()
            pdf_bytes = s3_client.download_bytes(str(path))
            if not pdf_bytes.startswith(b"%PDF"):
                raise ValueError(
                    f"Downloaded content from {path} is not a valid PDF "
                    f"(starts with: {pdf_bytes[:50]!r}). "
                    f"Check that the S3 URL is correct and the object exists."
                )
            with TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir) / "doc.pdf"
                tmp_path.write_bytes(pdf_bytes)
                return self._extract_blocks_from_path(tmp_path)
        else:
            return self._extract_blocks_from_path(target)

    def _extract_blocks_from_path(self, path: Path) -> list[Block]:
        import pdfplumber

        all_blocks: list[Block] = []

        with pdfplumber.open(str(path)) as pdf:
            print(path)
            for page_num, page in enumerate(pdf.pages):
                # print(page_num , page.extract_text())
                page_blocks = self._extract_page_blocks(page, page_num)
                all_blocks.extend(page_blocks)

        return all_blocks

    def _extract_page_blocks(self, page, page_num: int) -> list[Block]:
        blocks: list[Block] = []

        detected_tables = page.find_tables()
        table_bboxes = [t.bbox for t in detected_tables]

        # Table blocks
        for table_obj in detected_tables:
            rows = table_obj.extract()
            text = self._table_to_text(rows)
            blocks.append(Block("table", text, page_num, table_obj.bbox[1]))

        # Text blocks (words outside any table)
        words = page.extract_words(keep_blank_chars=False)
        outside_words = [
            w for w in words
            if not self._word_in_any_table(w, table_bboxes)
        ]

        if outside_words:
            text = self._words_to_paragraphs(outside_words)
            if text.strip():
                blocks.append(Block("text", text, page_num, outside_words[0]["top"]))

        blocks.sort(key=lambda b: (b.page, b.top))
        return blocks

    def _word_in_any_table(self, word: dict, bboxes: list[tuple]) -> bool:
        wx0, wtop = word["x0"], word["top"]
        wx1, wbot = word["x1"], word["bottom"]
        for (tx0, ttop, tx1, tbot) in bboxes:
            if (wx0 >= tx0 - 2 and wx1 <= tx1 + 2
                    and wtop >= ttop - 2 and wbot <= tbot + 2):
                return True
        return False

    def _words_to_paragraphs(self, words: list[dict]) -> str:
        if not words:
            return ""

        lines: list[list[dict]] = []
        current_line: list[dict] = [words[0]]

        for w in words[1:]:
            if abs(w["top"] - current_line[-1]["top"]) < 3:
                current_line.append(w)
            else:
                lines.append(current_line)
                current_line = [w]
        lines.append(current_line)

        line_texts: list[tuple[float, str]] = []
        for line in lines:
            line_str = " ".join(w["text"] for w in line)
            line_texts.append((line[0]["top"], line_str))

        if len(line_texts) < 2:
            return line_texts[0][1] if line_texts else ""

        line_heights = [
            line_texts[i + 1][0] - line_texts[i][0]
            for i in range(len(line_texts) - 1)
        ]
        median_gap = sorted(line_heights)[len(line_heights) // 2]
        para_threshold = median_gap * 1.6

        paragraphs: list[list[str]] = []
        current_para: list[str] = [line_texts[0][1]]

        for i in range(1, len(line_texts)):
            gap = line_texts[i][0] - line_texts[i - 1][0]
            if gap > para_threshold:
                paragraphs.append(current_para)
                current_para = [line_texts[i][1]]
            else:
                current_para.append(line_texts[i][1])
        paragraphs.append(current_para)

        return "\n\n".join(" ".join(p) for p in paragraphs)

    def _table_to_text(self, rows: list[list]) -> str:
        """Serialize a pdfplumber table to plain text (no markdown characters)."""
        if not rows:
            return ""
        text_rows = []
        for row in rows:
            cells = [(c or "").replace("\n", " ").strip() for c in row]
            text_rows.append(" ".join(cells))
        return "\n".join(text_rows)