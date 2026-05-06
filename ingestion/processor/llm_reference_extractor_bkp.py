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


class SimpleReference(BaseModel):
    """LLM extracts only circular_id and relationship_nature."""
    circular_id: str = Field(..., description="The circular reference ID as it appears")
    relationship_nature: str = Field(..., description="amends, refers, supersedes, implements, enforces, clarifies")


class SimpleReferenceList(BaseModel):
    """LLM response model for batched reference extraction."""
    references: list[SimpleReference]


EXTRACTION_PROMPT = """
You are a regulatory compliance assistant. Extract all circular references from the text.

For each reference output ONLY:
- circular_id: The circular reference ID as it appears (e.g., SEBI/HO/MIRSD/DOP1/CIR/P/2018/54, NSE/CML/73791, or NCL/CMPT/61816)
- relationship_nature: Whether this circular amends, refers, supersedes, implements, enforces, or clarifies another circular

Valid circular_id patterns (MUST match at least one of these):
- SEBI formats: SEBI/HO/..., SEBI/CFD/POD..., SEBI/HO/MIRSD/..., HO/MIRSD/...
- NSE/NCL formats: NSE/..., NCL/...
- Must contain at least one letter prefix before any slash (e.g., "SEBI", "NSE", "NCL", "CFD","HO")

DO NOT extract these (invalid - skip them entirely):
- Purely numeric codes like "0095/2024", "123/2023", "2024/095"
- Codes with only numbers on both sides of the slash
- Standalone numbers without regulatory body prefix
- Any ID that is just numbers and slashes without a letter-based prefix

Output JSON: {{"references": [{{"circular_id": "...", "relationship_nature": "..."}}]}}

Text to extract:
{text}
"""


class LLMCircularReferenceExtractor(BaseProcessor):
    """Extracts circular references using LLM-only approach (no regex)."""

    def __init__(self, db_pool: Any):
        super().__init__(db_pool)
        self.ref_repo = CircularReferenceRepository(db_pool)
        self.circular_repo = CircularRepository(db_pool)

    @property
    def name(self) -> str:
        return "llm_reference_extractor"

    def process(self, record: CircularRecord) -> None:
        file_path = self._get_pdf_path(record)
        extractor = PDFTextExtractor()
        text = extractor.extract(file_path)

        if not text.strip():
            self.logger.warning("Empty text extracted for circular_id=%s", record.circular_id)
            return

        # LLM-based extraction (with chunking for large texts)
        refs = self._extract_references_llm(text)

        if not refs:
            self.logger.info("No references found in circular_id=%s", record.circular_id)
            return

        # Deduplicate by circular_no
        seen = set()
        unique_refs = []
        for ref in refs:
            if ref["reference_circular_no"] not in seen:
                seen.add(ref["reference_circular_no"])
                unique_refs.append(ref)

        # Resolve reference_circular_id for each reference
        for ref in unique_refs:
            ref_circ = self._resolve_reference(ref["reference_circular_no"])
            ref["reference_circular_id"] = ref_circ.id if ref_circ else None

        # Persist idempotently
        self.ref_repo.delete_references_for_circular(record.id)
        self.ref_repo.insert_reference_batch(record.id, unique_refs)
        self.logger.info("Inserted %d references for circular_id=%s", len(unique_refs), record.circular_id)

    def _get_pdf_path(self, record: CircularRecord) -> str:
        """Get PDF path using same logic as ActionItemProcessor."""
        assets = self.circular_repo.list_assets(record.id)
        for role in ['extracted_pdf', 'original_pdf']:
            for asset in assets:
                if asset.asset_role == role and asset.file_path and asset.file_path.lower().endswith('.pdf'):
                    return asset.file_path
        if record.file_path and record.file_path.lower().endswith('.pdf'):
            return record.file_path
        raise ValueError(f"No PDF file path found for circular: {record.circular_id}")

    def _chunk_text(self, text: str, chunk_size: int = 850) -> list[str]:
        """Split text into sentence-aware chunks.

        Splits on sentence boundaries (full stop .) to avoid splitting
        circular references mid-sentence. Each chunk is up to chunk_size chars.
        """
        sentence_pattern = re.compile(r'(?<=[.!?])\s+')
        sentences = sentence_pattern.split(text)

        chunks = []
        current_chunk = ""

        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue

            if len(sentence) > chunk_size:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                    current_chunk = ""

                words = sentence.split()
                sub_chunk = ""
                for word in words:
                    if len(sub_chunk) + len(word) + 1 <= chunk_size:
                        sub_chunk = f"{sub_chunk} {word}".strip()
                    else:
                        if sub_chunk:
                            chunks.append(sub_chunk)
                        sub_chunk = word
                if sub_chunk:
                    current_chunk = sub_chunk
            elif len(current_chunk) + len(sentence) + 1 <= chunk_size:
                current_chunk = f"{current_chunk} {sentence}".strip()
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = sentence

        if current_chunk:
            chunks.append(current_chunk.strip())

        return chunks

    def _resolve_reference(self, full_reference_circular: str) -> CircularRecord | None:
        """Look up a reference circular by its reference (full_reference ).

        Returns CircularRecord if found, None if not found.
        """
        return self.circular_repo.get_record_by_full_reference(full_reference_circular)

    def _extract_references_llm(self, text: str) -> list[dict]:
        """Extract references by running LLM on all text chunks in parallel.

        Returns list of dicts with 'reference_circular_no' and 'relationship_nature'.
        Fails entire operation if any chunk fails (no partial results).
        """
        chunks = self._chunk_text(text)
        if not chunks:
            return []

        prompts = [EXTRACTION_PROMPT.format(text=chunk) for chunk in chunks]

        try:
            llm_client = get_llm_provider(Config.LLM_PROVIDER)
            responses = llm_client.create_completions_parallel(
                prompts=prompts,
                model=Config.ACTION_ITEM_MODEL,
                response_model=SimpleReferenceList,
                max_workers=min(len(prompts), 8),
            )
        except Exception as e:
            self.logger.error("LLM extraction failed: %s - aborting entire operation", e)
            raise

        result = []
        for response in responses:
            if response is None:
                raise RuntimeError("LLM extraction returned None for a chunk")
            for simple_ref in response.references:
                result.append({
                    "reference_circular_no": simple_ref.circular_id.upper(),
                    "relationship_nature": simple_ref.relationship_nature.lower(),
                })

        self.logger.info("LLM extracted %s references", len(result))
        return result


def main():
    parser = argparse.ArgumentParser(description="Extract circular references using LLM.")
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

        processor = LLMCircularReferenceExtractor(pool)
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