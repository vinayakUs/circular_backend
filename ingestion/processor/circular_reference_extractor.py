import logging
import re
from typing import Any

from config import Config
from db.client import get_db_client
from ingestion.indexer.pdf_extractor import PDFTextExtractor
from ingestion.processor.base import BaseProcessor
from ingestion.repository.circular_reference_repository import CircularReferenceRepository
from ingestion.repository.circular_repository import CircularRepository, CircularRecord
from utils.llm_providers import get_llm_provider

from pydantic import BaseModel, Field


class ExtractedReference(BaseModel):
    """A reference detected in circular text."""
    referenced_id: str
    referenced_source: str  # 'NSE' or 'SEBI'
    referenced_full_ref: str
    matched_text: str
    extraction_method: str = "regex"


class ReferenceRelationship(BaseModel):
    """LLM classification of reference relationship."""
    relationship_nature: str = Field(
        description="Relationship: implements, enforces, supersedes, references, clarifies, amends"
    )
    confidence: float = Field(description="Confidence 0.0-1.0")


class ReferenceWithNature(BaseModel):
    """A reference paired with its LLM-classified relationship."""
    reference: ExtractedReference
    relationship: ReferenceRelationship | None = None


class ReferenceListResponse(BaseModel):
    """LLM response model for batched reference classification."""
    references: list[ReferenceWithNature]


class CircularReferenceProcessor(BaseProcessor):
    """Extracts cross-references from circulars and classifies relationship nature."""

    # Regex patterns for detecting circular references
    PATTERNS = [
        # NSE structured: NSE/CML/73791, NSE/FAOP/73629
        re.compile(r'NSE/[A-Z]+/\d+', re.IGNORECASE),
        # SEBI patterns: SEBI/HO/CFD/..., CFD/POD1/I/9380/2026
        re.compile(r'SEBI/HO/\S+', re.IGNORECASE),
        re.compile(r'CFD/POD\d?/\S+', re.IGNORECASE),
        # Standalone NSE IDs: CML73791, SURV73764, NMF73709
        re.compile(r'\b[CML][A-Z]{2,3}\d{5,6}\b', re.IGNORECASE),
        # SEBI raw format: HO_49_14_(10)2026-CFD-POD1_I_9380_2026
        re.compile(r'HO_\d+_\d+_\S+', re.IGNORECASE),
    ]

    SOURCE_PATTERNS = [
        (re.compile(r'^NSE/', re.IGNORECASE), 'NSE'),
        (re.compile(r'^SEBI/', re.IGNORECASE), 'SEBI'),
        (re.compile(r'^[CML][A-Z]{2,3}\d', re.IGNORECASE), 'NSE'),
        (re.compile(r'^HO_\d+_\d+', re.IGNORECASE), 'SEBI'),
        (re.compile(r'^CFD/POD', re.IGNORECASE), 'SEBI'),
    ]

    def __init__(self, db_pool: Any):
        super().__init__(db_pool)
        self.ref_repo = CircularReferenceRepository(db_pool)
        self.circular_repo = CircularRepository(db_pool)

    @property
    def name(self) -> str:
        return "circular_reference_extractor"

    def process(self, record: CircularRecord) -> None:
        file_path = self._get_pdf_path(record)
        extractor = PDFTextExtractor()
        text = extractor.extract(file_path)

        if not text.strip():
            self.logger.warning("Empty text extracted for circular_id=%s", record.circular_id)
            return

        # Regex-based extraction
        raw_references = self._extract_references_regex(text)

        if not raw_references:
            self.logger.info("No references found in circular_id=%s", record.circular_id)
            return

        # Deduplicate
        seen = set()
        unique_refs = []
        for ref in raw_references:
            key = (ref.referenced_source, ref.referenced_id.upper())
            if key not in seen:
                seen.add(key)
                unique_refs.append(ref)

        # LLM classification
        classified = self._classify_references_llm(unique_refs, text)

        # Persist idempotently
        self.ref_repo.delete_references_for_circular(record.id)
        self.ref_repo.insert_reference_batch(record.id, classified)
        self.logger.info(
            "Inserted %d references for circular_id=%s",
            len(classified),
            record.circular_id,
        )

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

    def _extract_references_regex(self, text: str) -> list[ExtractedReference]:
        """Extract references using regex patterns."""
        references = []

        for pattern in self.PATTERNS:
            for match in pattern.finditer(text):
                matched_str = match.group().strip()

                # Normalize source
                source = None
                for src_pattern, src in self.SOURCE_PATTERNS:
                    if src_pattern.match(matched_str):
                        source = src
                        break

                if not source:
                    continue

                # Build full reference
                if source == 'NSE' and '/' not in matched_str:
                    # Just an ID like CML73791 -> NSE/CML/73791
                    dept = matched_str[:3]
                    num = matched_str[3:]
                    full_ref = f"NSE/{dept}/{num}"
                elif source == 'SEBI' and matched_str.startswith('CFD/'):
                    full_ref = f"SEBI/{matched_str}"
                else:
                    full_ref = matched_str

                # Get surrounding context for matched_text
                start = max(0, match.start() - 50)
                end = min(len(text), match.end() + 50)
                context = text[start:end].replace('\n', ' ').strip()

                references.append(ExtractedReference(
                    referenced_id=matched_str.upper(),
                    referenced_source=source,
                    referenced_full_ref=full_ref.upper(),
                    matched_text=context,
                    extraction_method='regex',
                ))

        return references

    def _classify_references_llm(
        self,
        references: list[ExtractedReference],
        text: str,
    ) -> list[ReferenceWithNature]:
        """Use LLM to classify relationship nature for each reference."""
        if not references:
            return []

        ref_list = "\n".join([
            f"- [{r.referenced_source}] {r.referenced_id}: \"{r.matched_text[:80]}...\""
            for r in references
        ])

        prompt = f"""You are a regulatory compliance assistant. For each circular reference below, classify the relationship nature.

Relationship types (choose one per reference):
- implements: implements a directive/framework from a parent regulatory circular
- enforces: enforces compliance with another circular's provisions
- supersedes: supersedes/replaces a previous circular
- references: merely references another circular without specific enforcement
- clarifies: clarifies interpretation of another circular
- amends: amends/modifies provisions of another circular

Output a JSON object with a 'references' array containing each reference with its relationship_nature and confidence (0.0-1.0).

References found in this circular:
{ref_list}

Text excerpt for context (first 1500 chars):
{text[:1500]}
"""

        try:
            llm_client = get_llm_provider(Config.LLM_PROVIDER)
            response = llm_client.create_completions_parallel(
                prompts=[prompt],
                model=Config.ACTION_ITEM_MODEL,
                response_model=ReferenceListResponse,
            )[0]
            # Merge LLM results with original references
            result = []
            for classified in response.references:
                result.append(ReferenceWithNature(
                    reference=classified.reference,
                    relationship=classified.relationship,
                ))
            return result
        except Exception as e:
            self.logger.warning("LLM classification failed: %s - storing references without nature", e)
            # Return references with None relationship (will be stored with NULL)
            return [ReferenceWithNature(reference=r, relationship=None) for r in references]