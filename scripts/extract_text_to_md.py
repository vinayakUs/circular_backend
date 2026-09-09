"""Extract text from a SEBI Master Circular PDF (from S3) and write to a markdown file.

Usage:
    python scripts/extract_text_to_md.py                           # latest PDF
    python scripts/extract_text_to_md.py --s3-key SEBI_MASTER/2026/07/102815_5671c4034d57/original/source.pdf
    python scripts/extract_text_to_md.py --output /tmp/my_output.md
"""

from __future__ import annotations

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from ingestion.indexer.pdf_extractor import PDFPlumberExtractor, PDFTextExtractor
from storage.s3_client import get_s3_client


def get_latest_sebi_master_key(s3_client) -> str:
    paginator = s3_client.client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=Config.AWS_S3_BUCKET, Prefix="SEBI_MASTER/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/source.pdf"):
                keys.append(obj["Key"])
    if not keys:
        raise RuntimeError("No SEBI Master circulars found in S3")
    keys.sort()
    return keys[-1]


def blocks_to_markdown(blocks) -> str:
    lines = []
    for b in blocks:
        if b.kind == "table":
            lines.append(f"\n---\n**[Table - Page {b.page + 1}]**\n")
            lines.append(b.content)
            lines.append("")
        else:
            lines.append(b.content)
    return "\n\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Extract SEBI Master Circular text to markdown")
    parser.add_argument("--s3-key", help="S3 key of the PDF (default: latest)")
    parser.add_argument("--output", default="/tmp/sebi_master_extracted.md", help="Output markdown file path")
    parser.add_argument("--method", choices=["plumber", "pymupdf"], default="plumber",
                        help="Extraction method: plumber (structured blocks) or pymupdf (flat text)")
    args = parser.parse_args()

    s3_client = get_s3_client()

    if args.s3_key:
        s3_key = args.s3_key
    else:
        s3_key = get_latest_sebi_master_key(s3_client)
        print(f"Latest PDF: {s3_key}")

    s3_path = f"s3://{Config.AWS_S3_BUCKET}/{s3_key}"

    if args.method == "plumber":
        extractor = PDFPlumberExtractor()
        blocks = extractor.extract_blocks(s3_path)
        print(f"Extracted {len(blocks)} blocks")
        md_content = blocks_to_markdown(blocks)
    else:
        extractor = PDFTextExtractor()
        text = extractor.extract(s3_path)
        print(f"Extracted {len(text)} characters")
        md_content = text

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(md_content)

    print(f"Written to {args.output} ({len(md_content)} chars)")


if __name__ == "__main__":
    main()
