from __future__ import annotations

from pathlib import Path


class PDFTextExtractor:
    """Extracts text from PDF files using pypdf."""

    def extract(self, path: str | Path) -> str:
        target = Path(path)
        if not target.exists():
            raise FileNotFoundError(f"PDF file not found: {target}")

        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise RuntimeError(
                "pypdf is not installed. Install dependencies before running the indexer."
            ) from exc

        reader = PdfReader(str(target))
        text_parts: list[str] = []
        for page in reader.pages:
            text_parts.append(page.extract_text() or "")
        return "\n".join(text_parts).strip()
