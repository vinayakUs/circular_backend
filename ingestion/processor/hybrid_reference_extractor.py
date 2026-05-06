import argparse
import logging
import re
import sys
from typing import Any

from pydantic import BaseModel, Field

from config import Config
from db.client import get_db_client
from ingestion.indexer.pdf_extractor import PDFTextExtractor
from ingestion.processor.base import BaseProcessor
from ingestion.repository.circular_reference_repository import CircularReferenceRepository
from ingestion.repository.circular_repository import CircularRepository, CircularRecord
from utils.llm_providers import get_llm_provider

import pymupdf
from pathlib import Path
from tempfile import TemporaryDirectory
from storage.s3_client import S3StorageClient


class PDFTextExtractorPyMuPDF:
    """Extracts text from PDF files using PyMuPDF (for testing)."""

    def extract(self, path: str | Path) -> str:
        target = Path(path)

        if not target.exists() and not (
            Config.AWS_S3_BUCKET and str(path).startswith("s3://")
        ):
            raise FileNotFoundError(f"PDF file not found: {path}")

        if str(path).startswith("s3://"):
            s3_client = S3StorageClient()
            pdf_bytes = s3_client.download_bytes(str(path))
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


class ExtractedReference(BaseModel):
    """LLM extracts normalized circular_id and relationship nature."""
    circular_id: str = Field(..., description="Normalized circular ID (e.g., SEBI/HO/MIRSD/CRADT/CIR/P/2020/53)")
    relationship_nature: str = Field(..., description="implements, enforces, supersedes, references, clarifies, amends")


class ReferenceListResponse(BaseModel):
    """LLM response: list of validated references."""
    references: list[ExtractedReference]


EXTRACTION_PROMPT = """
You are a regulatory compliance assistant. Given the circular reference candidates extracted from a regulatory document, normalize each circular ID and classify the relationship nature.

## Your Task
For EACH candidate below:
1. Return the normalized circular ID as a single clean string
2. Classify the relationship nature (only if genuinely specified, otherwise "references")

## Relationship Types
- implements: implements a directive/framework from a parent regulatory circular
- enforces: enforces compliance with another circular's provisions
- supersedes: supersedes/replaces a previous circular
- references: merely references another circular without specific enforcement intent
- clarifies: clarifies interpretation of another circular
- amends: amends/modifies provisions of another circular

## Output Format
Return JSON: {{"references": [{{"circular_id": "...", "relationship_nature": "..."}}]}}

## Candidates with Context
{candidates}

## Full Text Excerpt (first 2000 chars for additional context)
{text_excerpt}
"""


class CandidateMatch:
    """A regex-matched candidate with position and context."""
    def __init__(self, matched_text: str, start: int, end: int, source: str, left_context: str, right_context: str):
        self.matched_text = matched_text
        self.start = start
        self.end = end
        self.source = source
        self.left_context = left_context
        self.right_context = right_context


class HybridReferenceExtractor(BaseProcessor):
    """Extracts circular references using regex for candidate extraction + LLM for classification."""

    # Pass 1 (strict): no space, no \n — capture clean IDs
    PATTERNS_STRICT = [
        re.compile(r'SEBI/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'HO/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'NSE/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'NCL/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'AFD/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'CIR/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'IMD/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'sebi-[\w/().\-]+', re.IGNORECASE),
        re.compile(r'ho-[\w/().\-]+', re.IGNORECASE),
        re.compile(r'CFD/POD[\w/().\-]+', re.IGNORECASE),
        re.compile(r'\b[CML][A-Z]{2,3}\d{5,6}\b', re.IGNORECASE),
        re.compile(r'HO_[\w/().\-]+', re.IGNORECASE),
    ]

    # Pass 2 (loose): with space and \n — fill gaps where strict didn't match
    PATTERNS_LOOSE = [
        re.compile(r'SEBI/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'HO/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'NSE/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'NCL/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'AFD/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'CIR/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'IMD/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'sebi-[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'ho-[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'CFD/POD[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'\b[CML][A-Z]{2,3}\d{5,6}\b', re.IGNORECASE),
        re.compile(r'HO_[\w/().\- \n]+', re.IGNORECASE),
    ]

    SOURCE_PATTERNS = [
        (re.compile(r'^SEBI/'), 'SEBI'),
        (re.compile(r'^HO/'), 'SEBI'),
        (re.compile(r'^NSE/'), 'NSE'),
        (re.compile(r'^NCL/'), 'NCL'),
        (re.compile(r'^AFD/'), 'AFD'),
        (re.compile(r'^CIR/'), 'CIR'),
        (re.compile(r'^IMD/'), 'IMD'),
        (re.compile(r'^sebi-'), 'SEBI'),
        (re.compile(r'^ho-'), 'SEBI'),
        (re.compile(r'^CFD/POD'), 'SEBI'),
        (re.compile(r'^[CML][A-Z]{2,3}\d'), 'NSE'),
        (re.compile(r'^HO_'), 'SEBI'),
    ]

    def __init__(self, db_pool: Any):
        super().__init__(db_pool)
        self.ref_repo = CircularReferenceRepository(db_pool)
        self.circular_repo = CircularRepository(db_pool)

    @property
    def name(self) -> str:
        return "hybrid_reference_extractor"

    def process(self, record: CircularRecord) -> None:
        file_path = self._get_pdf_path(record)
        extractor = PDFTextExtractorPyMuPDF()
        text = extractor.extract(file_path)

        print('xx')
        print(text)
        print('xx')


        if not text.strip():
            self.logger.warning("Empty text extracted for circular_id=%s", record.circular_id)
            return

        candidates = self._extract_candidates(text)

        for x in candidates:
            print("xxx")
            print(f"[{x.start}] [{x.matched_text}]")
            print("xxx")


        print(f"\n=== REGEX CANDIDATES ({len(candidates)}) ===")
        for i, c in enumerate(candidates, 1):
            print(f"  {i}. [{c.source}] {c.matched_text}")

        if not candidates:
            self.logger.info("No candidates found in circular_id=%s", record.circular_id)
            return

        references = self._classify_references_llm(candidates, text)

        if not references:
            self.logger.info("No references classified for circular_id=%s", record.circular_id)
            return

        seen = set()
        unique_refs = []
        for ref in references:
            key = ref["reference_circular_no"].upper()
            if key not in seen:
                seen.add(key)
                unique_refs.append(ref)

        # Filter out self-references (source circular's own ID)
        current_refs = {record.circular_id.upper(), record.full_reference.upper()}
        unique_refs = [r for r in unique_refs if r["reference_circular_no"].upper() not in current_refs]

        for ref in unique_refs:
            ref_circ = self._resolve_reference(ref["reference_circular_no"])
            ref["reference_circular_id"] = ref_circ.id if ref_circ else None

        self.ref_repo.delete_references_for_circular(record.id)
        self.ref_repo.insert_reference_batch(record.id, unique_refs)
        self.logger.info("Inserted %d references for circular_id=%s", len(unique_refs), record.circular_id)

    def _get_pdf_path(self, record: CircularRecord) -> str:
        assets = self.circular_repo.list_assets(record.id)
        for role in ['extracted_pdf', 'original_pdf']:
            for asset in assets:
                if asset.asset_role == role and asset.file_path and asset.file_path.lower().endswith('.pdf'):
                    return asset.file_path
        if record.file_path and record.file_path.lower().endswith('.pdf'):
            return record.file_path
        raise ValueError(f"No PDF file path found for circular: {record.circular_id}")

    def _extract_candidates(self, text: str) -> list[CandidateMatch]:
        """Extract candidates using two-pass approach.

        Pass 1 (STRICT): capture clean IDs (no space, no \n)
        Pass 2 (LOOSE): only fill gaps where STRICT found nothing at that position
        """
        # Pass 1: strict — capture clean IDs
        strict_matches = self._extract_with_patterns(text, self.PATTERNS_STRICT)

        # Pass 2: loose — only fill gaps not covered by strict
        strict_positions = {(m.start, m.end) for m in strict_matches}
        loose_matches = self._extract_with_patterns(text, self.PATTERNS_LOOSE)
        for m in loose_matches:
            if (m.start, m.end) not in strict_positions:
                strict_matches.append(m)

        return self._deduplicate_by_position(strict_matches)

    def _extract_with_patterns(self, text: str, patterns: list[re.Pattern]) -> list[CandidateMatch]:
        """Run a set of patterns against text and return CandidateMatch list."""
        matches = []
        for pattern in patterns:
            for match in pattern.finditer(text):
                matched_str = match.group().strip()
                # Normalize newlines and multiple spaces within matched text
                matched_str = re.sub(r'[\n ]+', ' ', matched_str).strip()
                if not matched_str:
                    continue
                start = match.start()
                end = match.end()

                source = None
                for src_pattern, src in self.SOURCE_PATTERNS:
                    if src_pattern.match(matched_str):
                        source = src
                        break

                if not source:
                    continue

                left_start = max(0, start - 50)
                right_end = min(len(text), end + 50)
                left_context = text[left_start:start].replace('\n', ' ').strip()
                right_context = text[end:right_end].replace('\n', ' ').strip()

                matches.append(CandidateMatch(
                    matched_text=matched_str,
                    start=start,
                    end=end,
                    source=source,
                    left_context=left_context,
                    right_context=right_context,
                ))

        return matches

    def _deduplicate_by_position(self, matches: list[CandidateMatch]) -> list[CandidateMatch]:
        """When matches overlap, keep earliest-starting. If same start, prefer longer match.

        Also deduplicate by normalized text — if two matches have identical normalized text
        (after whitespace normalization), keep only the longer one to avoid duplicates from
        loose pattern capturing same ID with trailing text.
        """
        if not matches:
            return []

        # First pass: position deduplication
        sorted_matches = sorted(matches, key=lambda m: (m.start, -(m.end - m.start)))
        result = []
        last_end = -1

        for m in sorted_matches:
            if m.start >= last_end:
                result.append(m)
                last_end = m.end

        # Second pass: deduplicate by normalized text
        normalized_map: dict[str, CandidateMatch] = {}
        for m in result:
            norm = re.sub(r'[\n ]+', ' ', m.matched_text).strip()
            existing = normalized_map.get(norm)
            if existing is None:
                normalized_map[norm] = m
            else:
                # Keep the longer match
                if (m.end - m.start) > (existing.end - existing.start):
                    normalized_map[norm] = m

        return list(normalized_map.values())

    def _classify_references_llm(self, candidates: list[CandidateMatch], text: str) -> list[dict]:
        """Call LLM to normalize IDs and classify relationships for all candidates."""
        if not candidates:
            return []

        candidate_lines = []
        for i, c in enumerate(candidates, 1):
            candidate_lines.append(
                f"{i}. [{c.source}] {c.matched_text}\n"
                f"   Context: \"{c.left_context} {c.matched_text} {c.right_context}\""
            )

        prompt = EXTRACTION_PROMPT.format(
            candidates="\n".join(candidate_lines),
            text_excerpt=text[:2000],
        )

        print(f"\n=== EXTRACTED TEXT (first 500 chars) ===\n{text[:500]}")
        print(f"\n=== LLM PROMPT ===\n{prompt}")

        try:
            llm_client = get_llm_provider(Config.LLM_PROVIDER)
            response = llm_client.create_completions_parallel(
                prompts=[prompt],
                model=Config.ACTION_ITEM_MODEL,
                response_model=ReferenceListResponse,
            )[0]

            if response is None:
                raise RuntimeError("LLM returned None")

            print(f"\n=== LLM RESPONSE ===\n{response.model_dump_json(indent=2)}")

            return [
                {
                    "reference_circular_no": r.circular_id.upper(),
                    "relationship_nature": r.relationship_nature.lower(),
                }
                for r in response.references
            ]
        except Exception as e:
            self.logger.warning("LLM classification failed: %s - falling back to regex candidates", e)
            print(f"\n=== LLM ERROR ===\n{e}")
            return self._fallback_classification(candidates)

    def _fallback_classification(self, candidates: list[CandidateMatch]) -> list[dict]:
        """When LLM fails, return regex candidates with default relationship."""
        return [
            {
                "reference_circular_no": c.matched_text.upper(),
                "relationship_nature": "references",
            }
            for c in candidates
        ]

    def _resolve_reference(self, full_reference_circular: str) -> CircularRecord | None:
        return self.circular_repo.get_record_by_full_reference(full_reference_circular)


def main():
    parser = argparse.ArgumentParser(description="Extract circular references using hybrid regex+LLM.")
    parser.add_argument("--circular_id", type=str, required=True, help="The circular ID to process (e.g. NSE/FAOP/73791)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    print(f"Extracting references for: {args.circular_id}")
    try:
        db_client = get_db_client()
        pool = db_client.get_pool()
        repo = CircularRepository(pool)

        record = repo.get_record_by_circular_id(args.circular_id)
        if not record:
            print(f"No circular found with ID: {args.circular_id}", file=sys.stderr)
            sys.exit(1)

        processor = HybridReferenceExtractor(pool)
        success = processor.run(record)

        if success:
            print("Successfully processed and saved references.")
            ref_repo = CircularReferenceRepository(pool)
            refs, total = ref_repo.get_references(circular_id=record.id, limit=100)
            print(f"\n=== Saved References ({total}) ===")
            for r in refs:
                print(f"- {r.reference_circular_no}: {r.relationship_nature}")
        else:
            print("Failed to process circular.", file=sys.stderr)
            sys.exit(1)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
