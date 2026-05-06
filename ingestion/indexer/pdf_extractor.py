from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from config import Config
from storage.s3_client import S3StorageClient


class PDFTextExtractor:
    """Extracts text from PDF files using pypdf."""

    def extract(self, path: str | Path) -> str:
        target = Path(path)

        if not target.exists() and not (
            Config.AWS_S3_BUCKET and str(path).startswith("s3://")
        ):
            raise FileNotFoundError(f"PDF file not found: {path}")

        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise RuntimeError(
                "pypdf is not installed. Install dependencies before running the indexer."
            ) from exc

        if str(path).startswith("s3://"):
            s3_client = S3StorageClient()
            pdf_bytes = s3_client.download_bytes(str(path))
            with TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir) / "doc.pdf"
                tmp_path.write_bytes(pdf_bytes)
                reader = PdfReader(str(tmp_path))
        else:
            reader = PdfReader(str(target))

        text_parts: list[str] = []
        for page in reader.pages:
            text_parts.append(page.extract_text() or "")
        return "\n".join(text_parts).strip()
