"""
Reference Extractor — SEBI Circular Reference Chain (regex + LLM, stdout-only)
================================================================================

Extracts references from a single SEBI circular:
  1. Regex pass: 11 SEBI patterns (1992-2026) + keyword-based relationship classification
  2. LLM pass: ONLY for candidates the regex couldn't classify confidently
  3. Merge: regex result wins unless LLM upgraded a generic "references" to a specific one

Output (stdout only, no DB writes, no metadata extraction):
  - source circular_id
  - list of references: circular_id, relationship_type, trigger_phrase, source tag

Usage:
  python -m ingestion.processor.reference_extractor --circular_id "SEBI/HO/CFD/..."
  python -m ingestion.processor.reference_extractor --limit 100
"""

import argparse
import logging
import re
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Optional
from uuid import UUID

from config import Config
from db.postgres_client import get_postgres_client
from ingestion.processor.base import BaseProcessor
from ingestion.repository.asset_repository import AssetRepository
from ingestion.repository.circular_reference_repository import (
    CircularReference,
    CircularReferenceRepository,
)
from ingestion.repository.circular_repository import CircularRecord, CircularRepository
from pydantic import BaseModel, Field
from storage.s3_client import get_s3_client
from utils.llm_providers import get_llm_provider


# ─────────────────────────────────────────────────────────────────────────────
# PYDANTIC MODELS — LLM response schema
# ─────────────────────────────────────────────────────────────────────────────


class ExtractedReference(BaseModel):
    """LLM extracts normalized circular_id and relationship nature."""

    circular_id: str = Field(
        ...,
        description="Normalized circular ID (e.g., SEBI/HO/MIRSD/CRADT/CIR/P/2020/53)",
    )
    relationship_nature: str = Field(
        ...,
        description=(
            "One of: implements, enforces, supersedes, references, "
            "clarifies, amends, modifies, rescinds"
        ),
    )


class ReferenceListResponse(BaseModel):
    """LLM response: list of validated references."""

    references: list[ExtractedReference]


# ─────────────────────────────────────────────────────────────────────────────
# LLM EXTRACTION PROMPT
# ─────────────────────────────────────────────────────────────────────────────

EXTRACTION_PROMPT = """\
You are a regulatory compliance assistant. Given the circular reference \
candidates extracted from a regulatory document, normalize each circular ID \
and classify the relationship nature.

## Your Task
For EACH candidate below:
1. Return the normalized circular ID as a single clean string
2. Classify the relationship nature (only if genuinely specified, otherwise "references")

## Relationship Types
- references: merely references another circular without specific enforcement intent
- amends: amends/modifies provisions of another circular
- supersedes: supersedes/replaces a previous circular
- clarifies: clarifies interpretation of another circular
- implements: implements a directive/framework from a parent regulatory circular
- modifies: modifies specific clauses/provisions of another circular
- rescinds: rescinds/withdraws another circular
- enforces: enforces compliance with another circular's provisions

## Output Format
Return JSON: {{"references": [{{"circular_id": "...", "relationship_nature": "..."}}]}}

## Candidates with Context
{candidates}

## Full Text Excerpt (first 1000 chars for additional context)
{text_excerpt}
"""


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

VALID_TOP_PREFIXES: set[str] = {
    "SEBI", "CIR", "HO", "CFD", "MF", "MFD", "SMD", "TSMD", "SMDRP",
    "FITTC", "D&CC", "IIMARP", "SE", "DEB", "IMD", "MRD", "MIRSD", "DNPD",
    "NSE", "NCL", "AFD",
}

BAD_WORDS: set[str] = {
    "CHEMICALS", "PETROCHEMICALS", "PLASTIC", "RUBBER", "ARRANGEMENT",
    "AMALGAMATION", "MERGER", "RECONSTRUCTION", "REDUCTION", "DIRECTORATE",
    "DEPARTMENT", "GOVERNMENT", "MINISTRY", "ISIN", "INE", "SECTION",
    "REGULATION", "SCHEDULE", "ANNEXURE", "APPENDIX", "ARTICLE",
}

BAD_PREFIXES: set[str] = {
    "AS", "RS", "NO", "IN", "ON", "AT", "TO", "BY", "OF", "OR", "AN",
    "IS", "IT", "IF", "BE", "DO", "GO", "HE", "ME", "MY", "US", "WE",
}

SUPPORTED_RELATIONSHIPS: set[str] = {
    "references", "amends", "supersedes", "clarifies",
    "implements", "modifies", "rescinds", "enforces",
}

RELATIONSHIP_ALIASES: dict[str, str] = {
    "consolidates": "references",
    "partial_amendment": "amends",
}

_CONTROL_CHARS_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]+')


# ─────────────────────────────────────────────────────────────────────────────
# SANITIZATION
# ─────────────────────────────────────────────────────────────────────────────


def sanitize_text(s: Optional[str]) -> Optional[str]:
    """Strip carriage returns and other control characters from a string.

    Used for display values only — preserves meaningful spacing.
    """
    if not s:
        return s
    s = s.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
    s = _CONTROL_CHARS_RE.sub(' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def normalize_relationship(value: Optional[str]) -> str:
    """Map an extracted relationship to the persisted canonical vocabulary."""
    relationship = (sanitize_text(value) or "references").lower()
    relationship = RELATIONSHIP_ALIASES.get(relationship, relationship)
    if relationship not in SUPPORTED_RELATIONSHIPS:
        return "references"
    return relationship


# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZATION HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def normalise_circ_num(s: str) -> str:
    """Normalise a raw circular number string into a MATCH-KEY (internal use only).

    Strips spaces around slashes/dashes and between alphanumeric tokens,
    collapses whitespace runs, uppercases, strips trailing punctuation.
    """
    if not s:
        return ""
    s = sanitize_text(s) or ""
    s = re.sub(r'\s+/', '/', s)
    s = re.sub(r'/\s+', '/', s)
    s = re.sub(r'\s*-\s*', '-', s)
    s = re.sub(r'(?<=[A-Za-z0-9])\s+(?=[A-Za-z0-9])', '', s)
    s = re.sub(r'\s+', ' ', s)
    s = s.strip().upper()
    s = s.rstrip('.,;: ')
    return s


def canonicalize_circular_id(circular_number: str) -> Optional[str]:
    """Convert a raw circular number to a stable, URL-safe MATCH-KEY.

    Strips ALL whitespace; replaces non-alphanum (except slash) with dash.
    """
    if not circular_number:
        return None
    c = sanitize_text(circular_number) or ""
    c = c.upper()
    c = re.sub(r'\s+', '', c)
    c = re.sub(r'[^A-Z0-9/]', '-', c)
    c = re.sub(r'/+', '-', c)
    c = re.sub(r'-+', '-', c)
    return c.strip('-')


def normalize_text_light(text: str) -> str:
    """Flatten multi-line PDF text into a single line WITHOUT touching
    spacing around slashes or dashes. Used for regex matching.
    """
    text = re.sub(r'[\r\n\t]+', ' ', text)
    text = re.sub(r' {2,}', ' ', text)
    text = re.sub(r'/+', '/', text)
    text = re.sub(r'\bMaster\s+Cir\b', 'Master-Cir', text, flags=re.IGNORECASE)
    return text.strip()


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION + IDENTITY COMPARISON
# ─────────────────────────────────────────────────────────────────────────────


def is_valid_circular_number(circ: str) -> bool:
    """Validate a candidate SEBI circular number."""
    if not circ or len(circ) < 8:
        return False
    if len(circ) > 90:
        return False

    upper = circ.upper()

    if circ.count('/') < 2:
        return False

    has_4y = bool(re.search(r'(?:19|20)\d{2}', circ))
    # Trailing numeric ID of 2+ digits after a separator.
    # Covers: .../2020/53 (2-digit) and .../SURV/75174 (5-digit NSE serial).
    has_2y = bool(re.search(r'[/_-]\d{2,}$', circ))
    if not has_4y and not has_2y:
        return False

    has_cir   = 'CIR' in upper
    is_ho     = upper.startswith('HO/')
    is_fittc  = upper.startswith('FITTC') or upper.startswith('D&CC')
    is_iimarp = upper.startswith('IIMARP')
    is_smd    = upper.startswith('SMD') or upper.startswith('TSMD')
    is_cfd    = upper.startswith('CFD')
    is_nse    = upper.startswith('NSE/')
    is_ncl    = upper.startswith('NCL/')
    is_afd    = upper.startswith('AFD/')
    if not (has_cir or is_ho or is_fittc or is_iimarp or is_smd or is_cfd
            or is_nse or is_ncl or is_afd):
        return False

    if any(w in upper for w in BAD_WORDS):
        return False

    first = upper.split('/')[0].strip().rstrip('-')
    if first in BAD_PREFIXES:
        return False

    first_seg = first.split('-')[0]
    if first_seg not in VALID_TOP_PREFIXES and first not in VALID_TOP_PREFIXES:
        return False

    return True


def is_same_circular(a: str, b: str) -> bool:
    """Fuzzy-compare two circular numbers (any spacing/prefix variant)."""
    if not a or not b:
        return False
    a = canonicalize_circular_id(a) or a
    b = canonicalize_circular_id(b) or b
    if a == b:
        return True
    a_core = a[5:] if a.startswith("SEBI-") else a
    b_core = b[5:] if b.startswith("SEBI-") else b
    if a_core == b_core:
        return True
    if len(a_core) >= 10 and len(b_core) >= 10:
        if (a_core.endswith(b_core) or b_core.endswith(a_core)
                or a_core.startswith(b_core) or b_core.startswith(a_core)):
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# RELATIONSHIP CLASSIFICATION (regex keyword-based)
# ─────────────────────────────────────────────────────────────────────────────

RELATIONSHIP_KEYWORDS: dict[str, list[str]] = {
    "SUPERSEDES": [
        "in supersession of", "supersedes", "stands cancelled", "hereby cancelled",
        "in place of", "stands withdrawn", "is hereby rescinded", "stands rescinded",
    ],
    "RESCINDS": [
        "rescinded", "withdrawn", "hereby rescinded", "stands rescinded",
    ],
    "AMENDS": [
        "amends", "amended vide", "modif",
        "substituting", "replacing clause", "partially modif",
        "addendum to", "corrigendum to",
        "in partial modification",
    ],
    "MODIFIES": [
        "modifies clause", "modifies para", "modifies provision",
        "modification to clause", "modification to para",
    ],
    "IMPLEMENTS": [
        "in implementation of", "pursuant to the directions",
        "in terms of", "as directed by", "as mandated by",
    ],
    "ENFORCES": [
        "enforce", "compliance with", "mandatory compliance",
        "shall comply with", "required to comply",
    ],
    "CONSOLIDATES": [
        "consolidat", "master circular", "compiling all",
        "combined circular", "compilation of",
    ],
    "CLARIFIES": [
        "clarif", "further to", "with reference to", "in continuation",
        "in this regard", "pursuant to", "with a view to",
        "in order to", "it is clarified",
    ],
}

_REL_ORDER: list[str] = [
    "SUPERSEDES", "RESCINDS", "AMENDS", "MODIFIES",
    "IMPLEMENTS", "ENFORCES", "CONSOLIDATES", "CLARIFIES",
]


def classify_relationship(context: str) -> tuple[str, str]:
    """Classify the relationship between two circulars from surrounding text.

    Returns (relationship_type, trigger_phrase).
    """
    ctx_lower = context.lower()
    for rel_type in _REL_ORDER:
        for kw in RELATIONSHIP_KEYWORDS[rel_type]:
            if kw in ctx_lower:
                idx = ctx_lower.find(kw)
                start = max(0, idx - 10)
                trigger = context[start: idx + len(kw) + 20].strip()
                return rel_type, trigger
    return "REFERENCES", "referred to"


def get_context(text: str, start: int, end: int, window: int = 150) -> str:
    """Extract surrounding context around a match position."""
    return text[max(0, start - window): min(len(text), end + window)]


# ─────────────────────────────────────────────────────────────────────────────
# SEBI CIRCULAR REGEX PATTERNS — 11 patterns
# ─────────────────────────────────────────────────────────────────────────────

_S    = r'\s*/\s*'
_SEG  = r'[A-Za-z0-9][A-Za-z0-9\.\-_]*'

# _YEAR widened from \d{2} to \d{1,4} so 3+ digit trailing serials (e.g. .../2021/577)
# are captured in full rather than silently truncated.
_YEAR = r'(?:(?:19|20)\d{2}|\d{1,4})'
_4Y   = r'(?:19|20)\d{2}'
_NUM  = r'\d{1,6}'
_LNUM = r'\d{1,9}'

CIRCULAR_PATTERNS: list[re.Pattern] = [

    # P1: HO/ FORMAT (2025-2026)
    re.compile(
        r'\bHO'
        + r'(?:' + _S + r'(?:\d+|\(\d+\))' + r'){0,4}'
        + _S
        + r'(?:\d+)?\(\d+\)'
        + r'(?:20\d{2}|19\d{2})?'
        + r'[-\w]+'
        + r'(?:'
        +   r'[\s/]+[A-Za-z0-9][-A-Za-z0-9]*'
        +   r'(?:' + _S + r'[A-Za-z0-9][-A-Za-z0-9]*' + r'){0,3}'
        +   r'(?:' + _S + r'(?:20\d{2}|19\d{2})' + r')?'
        + r')?',
        re.IGNORECASE,
    ),

    # P2: SEBI/... FORMAT (2000-2026)
    re.compile(
        r'\bSEBI(?:' + _S + _SEG + r'){1,9}' + _S + _YEAR,
        re.IGNORECASE,
    ),

    # P3: SEBI/.../CIR No. FORMAT (2000-2012)
    re.compile(
        r'\bSEBI(?:' + _S + _SEG + r'){1,5}'
        + r'\s*/??\s*CIR\.?\s+No\.?\s*'
        + _NUM
        + r'(?:' + _S + _LNUM + r')?'
        + _S + _YEAR,
        re.IGNORECASE,
    ),

    # P4: CIR/ PREFIX FORMAT (2005-2020)
    re.compile(
        r'\bCIR(?:' + _S + _SEG + r'){1,8}' + _S + _NUM + _S + _YEAR,
        re.IGNORECASE,
    ),

    # P5: CFD/... WITHOUT CIR PREFIX (2010-2020)
    re.compile(
        r'\bCFD'
        + r'(?:' + _S + _SEG + r'){1,5}'
        + _S + _YEAR,
        re.IGNORECASE,
    ),

    # P6: FITTC/D&CC FORMAT (1998-2004)
    re.compile(
        r'\b(?:D&CC' + _S + r')?FITTC'
        + r'(?:' + _S + _SEG + r'){1,4}'
        + r'(?:' + _S + _NUM + r')?'
        + _S + _YEAR,
        re.IGNORECASE,
    ),

    # P7: IIMARP FORMAT (1993-2003)
    re.compile(
        r'\bIIMARP'
        + r'(?:' + _S + _SEG + r'){0,4}'
        + r'(?:' + _S + _NUM + r'){0,2}'
        + _S + _YEAR,
        re.IGNORECASE,
    ),

    # P8: SMD/TSMD FORMATS (1992-2003)
    re.compile(
        r'\b(?:T?SMD)'
        + r'(?:' + _S + r'[A-Za-z][A-Za-z0-9]{1,15}' + r'){1,3}'
        + r'(?:'
        +   _S + r'(?:CIR(?:CULARS?)?[-A-Za-z0-9]*|IECG)'
        + r')?'
        + r'(?:' + _S + _NUM + r'){0,2}'
        + r'(?:' + _S + _YEAR + r'|[-]\d{2})',
        re.IGNORECASE,
    ),

    # P9: MF/MFD SHORT FORMATS (1997-2003)
    re.compile(
        r'\bMF(?:D)?'
        + _S + r'CIR'
        + r'(?:' + _S + _NUM + r'){1,3}'
        + _S + _YEAR,
        re.IGNORECASE,
    ),

    # P10: LEGACY DEPT-FIRST FORMAT (1992-2010)
    re.compile(
        r'\b[A-Z]{2,12}'
        + r'(?:' + _S + r'[A-Za-z][A-Za-z0-9\.]{1,15}' + r'){0,3}'
        + _S + r'CIR[A-Za-z0-9\-]*'
        + r'(?:' + _S + _SEG + r'){0,3}'
        + r'(?:' + _S + _NUM + r')?' + _S + _YEAR,
        re.IGNORECASE,
    ),

    # P11: MASTER CIRCULAR FORMAT (2000-2020)
    re.compile(
        r'\b(?:SEBI' + _S + r')?'
        + r'(?:HO' + _S + r')?'
        + r'[A-Z]{2,12}'
        + r'(?:' + _S + r'[A-Za-z][A-Za-z0-9\.]{1,15}' + r'){0,3}'
        + _S + r'Master[\s\-]Cir[A-Za-z0-9\-]*'
        + r'(?:' + _S + _SEG + r'){0,3}'
        + r'(?:' + _S + _NUM + r')?' + _S + _YEAR,
        re.IGNORECASE,
    ),

    # P12: NSE/... FORMAT (NSE Surveillance, FAOP, CIR, etc.)
    # Covers: NSE/SURV/75174, NSE/FAOP/2024/73791, NSE/CIR/P/2019/053
    # 1-4 letter-starting segments, then either YEAR/NUM or NUM alone.
    re.compile(
        r'\bNSE'
        + r'(?:' + _S + r'[A-Za-z][A-Za-z0-9]{1,15}' + r'){1,4}'
        + _S + r'(?:\d{4}' + _S + r'\d+|\d+)',
        re.IGNORECASE,
    ),

    # P13: NCL/... FORMAT (National Commodity / Clearing Ltd circulars)
    re.compile(
        r'\bNCL'
        + r'(?:' + _S + r'[A-Za-z][A-Za-z0-9]{1,15}' + r'){1,4}'
        + _S + r'(?:\d{4}' + _S + r'\d+|\d+)',
        re.IGNORECASE,
    ),

    # P14: AFD/... FORMAT (Alternative Investment Funds Dept circulars)
    re.compile(
        r'\bAFD'
        + r'(?:' + _S + r'[A-Za-z][A-Za-z0-9]{1,15}' + r'){1,4}'
        + _S + r'(?:\d{4}' + _S + r'\d+|\d+)',
        re.IGNORECASE,
    ),
]


# ─────────────────────────────────────────────────────────────────────────────
# CANDIDATE MATCH
# ─────────────────────────────────────────────────────────────────────────────


class CandidateMatch:
    """A regex-matched candidate with position, context, and regex classification."""

    def __init__(
        self,
        matched_text: str,
        start: int,
        end: int,
        source: str,
        left_context: str,
        right_context: str,
        regex_relationship: str = "REFERENCES",
        regex_trigger: str = "referred to",
    ):
        self.matched_text = matched_text
        self.start = start
        self.end = end
        self.source = source
        self.left_context = left_context
        self.right_context = right_context
        self.regex_relationship = regex_relationship
        self.regex_trigger = regex_trigger

    def __repr__(self) -> str:
        return (
            f"CandidateMatch(text={self.matched_text!r}, "
            f"pos=[{self.start}:{self.end}], source={self.source!r}, "
            f"regex_rel={self.regex_relationship!r})"
        )


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE PATTERNS — to attribute each candidate to its issuing authority
# ─────────────────────────────────────────────────────────────────────────────

SOURCE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r'^SEBI/'), 'SEBI'),
    (re.compile(r'^HO/'), 'SEBI'),
    (re.compile(r'^NSE/'), 'NSE'),
    (re.compile(r'^NCL/'), 'NCL'),
    (re.compile(r'^AFD/'), 'AFD'),
    (re.compile(r'^CIR/'), 'CIR'),
    (re.compile(r'^IMD/'), 'IMD'),
    (re.compile(r'^MIRSD/'), 'SEBI'),
    (re.compile(r'^MRD/'), 'SEBI'),
    (re.compile(r'^sebi-'), 'SEBI'),
    (re.compile(r'^ho-'), 'SEBI'),
    (re.compile(r'^CFD/POD'), 'SEBI'),
    (re.compile(r'^[CML][A-Z]{2,3}\d'), 'NSE'),
    (re.compile(r'^HO_'), 'SEBI'),
]


# ─────────────────────────────────────────────────────────────────────────────
# REFERENCE EXTRACTOR — Main Processor Class
# ─────────────────────────────────────────────────────────────────────────────


class ReferenceExtractor(BaseProcessor):
    """Reference extractor (regex + LLM) — stdout-only, no DB writes."""

    name = "reference_extractor"

    def __init__(self, db_pool: Any):
        super().__init__(db_pool)
        self.circular_repo = CircularRepository(db_pool)
        self.asset_repo = AssetRepository(db_pool)
        self.reference_repo = CircularReferenceRepository(db_pool)

    # ─────────────────────────────────────────────────────────────────────────
    # MAIN PIPELINE
    # ─────────────────────────────────────────────────────────────────────────

    def process(self, record: CircularRecord) -> None:
        self.logger.info(
            "Starting reference extraction for circular_id=%s",
            sanitize_text(record.circular_id),
        )

        # Step 1: Locate PDF
        try:
            pdf_path = self._get_pdf_path(record)
        except (ValueError, FileNotFoundError) as e:
            self.logger.error("PDF location failed: %s", e)
            print(f"\nERROR: {e}")
            raise

        self.logger.info("PDF located: %s", pdf_path)

        # Step 2: Extract text
        raw_text = self._extract_text(pdf_path)
        if not raw_text.strip():
            self.logger.warning(
                "Empty text extracted for circular_id=%s", record.circular_id,
            )
            print("\n(no text extracted from PDF)")
            self._print_references(sanitize_text(record.circular_id), [])
            return

        # Step 3: Regex extraction + classification
        candidates = self._extract_references_regex(raw_text, record)
        self.logger.info(
            "Regex found %d reference candidates", len(candidates),
        )

        # Step 4: Identify ambiguous candidates (regex returned generic "references")
        ambiguous = self._select_ambiguous_candidates(candidates)
        self.logger.info(
            "%d candidates need LLM classification", len(ambiguous),
        )

        # Step 5: LLM classification (only if needed)
        llm_results: list[dict] = []
        if ambiguous:
            llm_results = self._classify_with_llm(ambiguous, raw_text)
            self.logger.info(
                "LLM returned %d classifications", len(llm_results),
            )

        # Step 6: Merge and print
        final = self._merge_results(candidates, llm_results)
        self._print_references(sanitize_text(record.circular_id), final)

        # Step 7: Resolve target FKs (best-effort) and persist
        self._resolve_target_fks(final)
        self._persist_references(record.id, final)
        self.logger.info(
            "Reference extraction complete: %d references for circular_id=%s",
            len(final), record.circular_id,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 1: LOCATE PDF
    # ─────────────────────────────────────────────────────────────────────────

    def _get_pdf_path(self, record: CircularRecord) -> str:
        assets = self.asset_repo.list_assets(record.id)
        for role in ('extracted_pdf', 'original_pdf'):
            for asset in assets:
                if (
                    asset.asset_role == role
                    and asset.file_path
                    and asset.file_path.lower().endswith('.pdf')
                ):
                    return asset.file_path

        # Fallback: record.file_path is not declared on the slotted CircularRecord,
        # so use getattr for backward-compat with any older code paths.
        fallback = getattr(record, "file_path", None)
        if fallback and fallback.lower().endswith('.pdf'):
            return fallback

        raise ValueError(f"No PDF file path found for circular: {record.circular_id}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 2: EXTRACT TEXT
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_text(self, path: str) -> str:
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise RuntimeError(
                "pypdf is not installed. Install dependencies before running the processor."
            ) from exc

        target = Path(path)

        if str(path).startswith("s3://"):
            s3_client = get_s3_client()
            pdf_bytes = s3_client.download_bytes(str(path))
            with TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir) / "doc.pdf"
                tmp_path.write_bytes(pdf_bytes)
                reader = PdfReader(str(tmp_path))
                return self._read_all_pages(reader)
        else:
            if not target.exists():
                raise FileNotFoundError(f"PDF file not found: {path}")
            reader = PdfReader(str(target))
            return self._read_all_pages(reader)

    def _read_all_pages(self, reader) -> str:
        """Concatenate text from all pages of an open PdfReader."""
        parts: list[str] = []
        for page in reader.pages:
            text = page.extract_text() or ""
            if text:
                parts.append(text)
        return "\n".join(parts)

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 3: REGEX EXTRACTION + KEYWORD CLASSIFICATION
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_references_regex(
        self, text: str, record: CircularRecord,
    ) -> list[CandidateMatch]:
        """Run all 11 SEBI regex patterns against flattened PDF text.

        Returns a list of CandidateMatch (one per surviving, non-self, unique
        reference) with the regex-keyword relationship already computed.
        """
        flat_text = normalize_text_light(text)

        # Build the set of "self" ids to filter out
        self_ids: list[str] = []
        if record.circular_id:
            self_ids.append(record.circular_id)
        full_reference = getattr(record, "full_reference", None)
        if full_reference:
            self_ids.append(full_reference)

        candidates: list[CandidateMatch] = []
        seen: set[str] = set()  # canonical ids already emitted

        for pattern in CIRCULAR_PATTERNS:
            for m in pattern.finditer(flat_text):
                raw = m.group(0).strip()
                raw = re.sub(r'[\r\n ]+', ' ', raw).strip()
                if not raw:
                    continue

                # Resolve source attribution
                source: Optional[str] = None
                for src_pattern, src in SOURCE_PATTERNS:
                    if src_pattern.match(raw):
                        source = src
                        break
                if not source:
                    continue

                # Build match-key for validation and dedup
                match_key = normalise_circ_num(raw)
                if not is_valid_circular_number(match_key):
                    continue

                # Filter self-references
                is_self = any(
                    is_same_circular(match_key, sid) for sid in self_ids if sid
                )
                if is_self:
                    continue

                # Dedup
                canon = canonicalize_circular_id(match_key) or match_key.upper()
                if canon in seen:
                    continue
                seen.add(canon)

                # Context + regex-keyword classification
                start, end = m.start(), m.end()
                left_ctx = flat_text[max(0, start - 50):start].replace('\n', ' ').strip()
                right_ctx = flat_text[end:end + 50].replace('\n', ' ').strip()
                full_ctx = get_context(flat_text, start, end)
                rel_type, trigger = classify_relationship(full_ctx)

                candidates.append(CandidateMatch(
                    matched_text=sanitize_text(raw) or raw,
                    start=start,
                    end=end,
                    source=source,
                    left_context=left_ctx,
                    right_context=right_ctx,
                    regex_relationship=rel_type,
                    regex_trigger=trigger,
                ))

        return candidates

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4: SELECT AMBIGUOUS CANDIDATES (for LLM)
    # ─────────────────────────────────────────────────────────────────────────

    def _select_ambiguous_candidates(
        self, candidates: list[CandidateMatch],
    ) -> list[CandidateMatch]:
        """Return candidates whose regex classification is the generic default."""
        return [
            c for c in candidates
            if c.regex_relationship == "REFERENCES" and c.regex_trigger == "referred to"
        ]

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 5: LLM CLASSIFICATION
    # ─────────────────────────────────────────────────────────────────────────

    def _classify_with_llm(
        self, candidates: list[CandidateMatch], text: str,
    ) -> list[dict]:
        """Call the LLM to classify ambiguous candidates. Returns [] on failure."""
        if not candidates:
            return []

        # Build the prompt
        candidate_lines: list[str] = []
        for i, c in enumerate(candidates, 1):
            candidate_lines.append(
                f"{i}. [{c.source}] {c.matched_text}\n"
                f"   Context: \"{c.left_context} {c.matched_text} {c.right_context}\""
            )

        prompt = EXTRACTION_PROMPT.format(
            candidates="\n".join(candidate_lines),
            text_excerpt=text[:1000],
        )

        try:
            llm_client = get_llm_provider(Config.LLM_PROVIDER)
            response = llm_client.create_completions_parallel(
                prompts=[prompt],
                model=Config.ACTION_ITEM_MODEL,
                response_model=ReferenceListResponse,
            )[0]

            if response is None:
                raise RuntimeError("LLM returned None")

            results: list[dict] = []
            for r in response.references:
                clean_id = sanitize_text(r.circular_id)
                if not clean_id:
                    continue
                if '/' not in clean_id:
                    self.logger.debug("Skipping LLM result (no slash): %s", clean_id)
                    continue
                _PROSE_WORDS = {"DATED", "CIRCULAR", "MASTER", "VIDE", "THE", "AND", "FOR"}
                id_upper = clean_id.upper()
                if any(w in id_upper for w in _PROSE_WORDS) and id_upper.count('/') < 2:
                    self.logger.debug("Skipping LLM result (looks like prose): %s", clean_id)
                    continue
                results.append({
                    "circular_id": clean_id.upper(),
                    "relationship_nature": (
                        sanitize_text(r.relationship_nature) or "references"
                    ).lower(),
                })
            return results

        except Exception as e:
            self.logger.warning(
                "LLM classification failed: %s — falling back to regex-only results", e,
            )
            return []

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 6: MERGE REGEX + LLM RESULTS
    # ─────────────────────────────────────────────────────────────────────────

    def _merge_results(
        self,
        candidates: list[CandidateMatch],
        llm_results: list[dict],
    ) -> list[dict]:
        """Combine regex and LLM results. LLM only upgrades generic 'references'.

        Returns sorted list of dicts:
          {circular_id, relationship_type, trigger_phrase, source}
        """
        merged: dict[str, dict] = {}  # keyed by canonical id

        # Layer 1: regex results
        for c in candidates:
            canon = canonicalize_circular_id(c.matched_text) or c.matched_text.upper()
            merged[canon] = {
                "circular_id":       c.matched_text,
                "relationship_type": normalize_relationship(c.regex_relationship),
                "trigger_phrase":    sanitize_text(c.regex_trigger),
                "source":            "regex",
            }

        # Layer 2: LLM results — upgrade generic refs and add LLM-only entries
        for r in llm_results:
            circ_no = r["circular_id"]
            relationship = normalize_relationship(r.get("relationship_nature"))
            canon = canonicalize_circular_id(circ_no) or circ_no.upper()

            # Find any existing regex entry that fuzzy-matches
            existing_key = None
            if canon in merged:
                existing_key = canon
            else:
                for k in merged:
                    if is_same_circular(canon, k):
                        existing_key = k
                        break

            if existing_key is not None:
                existing = merged[existing_key]
                # Only upgrade if regex said "references" (default) and LLM is more specific
                if (existing["relationship_type"] == "references"
                        and relationship != "references"):
                    existing["relationship_type"] = relationship
                    existing["trigger_phrase"] = "(LLM classification — no trigger phrase)"
                    existing["source"] = "merged"
                # Otherwise regex wins
            else:
                # LLM found something regex didn't — surface it
                merged[canon] = {
                    "circular_id":       circ_no,
                    "relationship_type": relationship,
                    "trigger_phrase":    "(LLM classification — no trigger phrase)",
                    "source":            "llm",
                }

        # Sort by canonical_id for stable output
        result = list(merged.values())
        result.sort(key=lambda x: canonicalize_circular_id(x["circular_id"]) or x["circular_id"].upper())
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 7: PRINT TO STDOUT
    # ─────────────────────────────────────────────────────────────────────────

    def _print_references(self, source_id: str, references: list[dict]) -> None:
        """Print the extraction result to stdout and the log file."""
        sep = "-" * 60
        self.logger.info("")
        self.logger.info(sep)
        self.logger.info("Source: %s", source_id or 'N/A')
        self.logger.info("References (%d):", len(references))
        self.logger.info(sep)
        if not references:
            self.logger.info("  (no references found)")
            self.logger.info(sep)
            return

        for i, r in enumerate(references, 1):
            self.logger.info("  %d. %s", i, r.get('circular_id') or 'N/A')
            self.logger.info(
                "     relationship : %s      [source: %s]",
                r.get('relationship_type', 'references'),
                r.get('source', 'unknown'),
            )
            self.logger.info("     trigger      : \"%s\"", r.get('trigger_phrase', ''))
        self.logger.info(sep)

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 8: RESOLVE TARGET FKs (best-effort)
    # ─────────────────────────────────────────────────────────────────────────

    def _resolve_target_fks(self, references: list[dict]) -> None:
        """For each reference, try to resolve target_circular_id via circulars.full_reference.

        Mutates `references` in-place, setting `target_circular_id` to the
        matching circular's UUID, or leaving it None if no match. Failures
        are logged but never abort the pipeline — best-effort.
        """
        for ref in references:
            circ_no = ref.get("circular_id")
            if not circ_no:
                ref["target_circular_id"] = None
                continue
            try:
                resolved = self.circular_repo.get_record_by_full_reference(circ_no)
                ref["target_circular_id"] = resolved.id if resolved else None
            except Exception as e:
                self.logger.warning(
                    "FK lookup failed for %s: %s", circ_no, e,
                )
                ref["target_circular_id"] = None

        resolved_count = sum(1 for r in references if r.get("target_circular_id"))
        self.logger.info(
            "Resolved %d/%d target FKs",
            resolved_count, len(references),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 9: PERSIST TO circular_references
    # ─────────────────────────────────────────────────────────────────────────

    def _persist_references(
        self, source_circular_id: UUID, references: list[dict],
    ) -> int:
        """Persist references via CircularReferenceRepository.replace_references().

        Idempotent: re-runs for the same source overwrite prior edges.
        Returns the count written.
        """
        payload = [
            CircularReference(
                relationship_type=r.get("relationship_type", "references"),
                target_circular_number=r["circular_id"],
                target_circular_id=r.get("target_circular_id"),
            )
            for r in references
            if r.get("circular_id")
        ]
        count = self.reference_repo.replace_references(source_circular_id, payload)
        self.logger.info(
            "Persisted %d references for source_circular_id=%s",
            count, source_circular_id,
        )
        return count


# ─────────────────────────────────────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Extract references from a SEBI circular (regex + LLM, stdout only).",
    )
    parser.add_argument(
        "--circular_id",
        type=str,
        help="The circular ID to process (e.g. SEBI/HO/CFD/...)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of pending circulars to process when --circular_id is omitted",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    db_client = get_postgres_client()
    pool = db_client.get_pool()
    processor = ReferenceExtractor(pool)

    if args.circular_id:
        logger.info("Extracting references for: %s", args.circular_id)
        repo = CircularRepository(pool)
        record = repo.get_record_by_circular_id(args.circular_id)
        if not record:
            print(f"No circular found with ID: {args.circular_id}", file=sys.stderr)
            sys.exit(1)

        success = processor.run(record)
        if not success:
            print("Failed to extract references.", file=sys.stderr)
            sys.exit(1)
    else:
        from ingestion.repository.processor_repository import ProcessorRepository
        processor_repo = ProcessorRepository(pool)
        pending = processor_repo.get_pending_circulars_for_processor(
            processor.name, limit=args.limit,
        )
        logger.info(
            "Found %d pending circulars for '%s'", len(pending), processor.name,
        )
        for record in pending:
            logger.info("Processing: %s", record.circular_id)
            processor.run(record)
        logger.info("Completed %d circulars.", len(pending))


if __name__ == "__main__":
    main()
