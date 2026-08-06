"""
Schema Reference Extractor — SEBI Reference Chain Graph Processor
=================================================================

R&D processor that builds a Reference Chain Graph from a single SEBI circular.
Combines:
  1. Architecture from processor framework (BaseProcessor lifecycle)
  2. Business logic from SEBI Updated Project (10 regex patterns, metadata, cleaning)
  3. Advanced extraction from hybrid_reference_extractor (two-pass regex, LLM classification)
  4. Graph generation (Nodes + Edges)

Usage:
  python -m ingestion.processor.schema_reference_extractor --circular_id "SEBI/HO/CFD/CMD/CIR/P/2023/001"
  python -m ingestion.processor.schema_reference_extractor --circular_id "NSE/FAOP/73791"

This is an R&D processor:
  - No database writes (PostgreSQL, Elasticsearch, Neo4j)
  - Everything in-memory
  - All output printed to stdout
  - Graph-ready JSON suitable for future frontend visualization

PATCH NOTE 1 (restored): _YEAR regex constant fixed below. The old
`\d{2}` alternative silently truncated 3+ digit trailing serial numbers
(e.g. .../2021/577 -> was captured as .../2021/57). Fixed to `\d{1,4}`
so it always captures the FULL trailing number. See the comment at the
_YEAR definition for the full explanation.

PATCH NOTE 2: Console output was showing garbled/duplicated lines
and a missing circular_id in the REFERENCES section. Root cause: stray
control characters (chiefly \r, "carriage return") were leaking into
circular_id strings — either from raw PDF text via the candidate regex
match, or echoed back verbatim inside an LLM completion — and were never
stripped before being stored or printed. Fixed via sanitize_text() at
every stage that touches an ID (candidate extraction, LLM output, final
output/print).

PATCH NOTE 3 (this version): Two families of bugs around circular numbers
that contain spaces the PDF genuinely has (e.g. "POD1 I/10421" or
"POD1/ I/146"):

  PROBLEM A — inconsistent/incorrect space handling.
    The old normalise_circ_num() only stripped spaces immediately before
    or after a slash, with a special exception meant to "preserve"
    slash-adjacent spaces in a "/ X/" pattern. That exception was based on
    a wrong assumption (that the space was structurally meaningful) and
    it didn't even cover mid-segment spaces like "POD1 I" (not adjacent
    to any slash), so the same circular could come out formatted two
    different, inconsistent ways depending on where PyMuPDF happened to
    insert a stray space during block extraction.

  PROBLEM B — duplicate references.
    _deduplicate_references() merged results into a dict keyed by an
    EXACT canonical-id string. Two mentions of the very same circular,
    one with a leading "SEBI/" prefix and one without (extremely common:
    one comes from the LLM's own normalization, the other from the raw
    regex match), produced two different canonical keys and were kept as
    two separate, duplicate output entries.

  PROBLEM C — wrong department extraction.
    extract_department() could return a mangled numeric/date blob like
    "122025-DDHS-POD1" instead of "DDHS" when the department code was
    packed into a dash-joined segment alongside date/serial numbers.

THE FIX — two versions of every ID, used for two different purposes:

  1. A **match-key** (fully space-stripped, via normalise_circ_num /
     canonicalize_circular_id) used ONLY internally: validity checks,
     self-reference filtering, and — critically — DEDUPLICATION. Because
     canonicalize_circular_id() already strips ALL whitespace, this key
     is identical regardless of how a given mention of the circular was
     spaced, which is exactly what fixes Problem B: two spellings of the
     same circular now collapse into ONE match-key and therefore ONE
     output entry.

  2. A **display value** (only sanitize_text()'d — control characters and
     multi-space runs cleaned up, but NO space stripping) used for
     EVERYTHING the user actually sees: source.circular_id and every
     references[].circular_id. This is produced by a new light-touch
     flattening function, normalize_text_light(), which — unlike the old
     normalize_text_for_regex() — deliberately does NOT strip spacing
     around slashes/dashes, so genuine PDF spacing (including a space
     right after a slash, e.g. "POD1/ I/146") survives all the way to
     the final printed/returned output, exactly as it appears in the PDF.

  extract_department() was also fixed to correctly pull a department
  code like "DDHS" out of a dash-joined segment that also contains
  numeric date/serial tokens, instead of returning the whole blob.
"""

import argparse
import json
import logging
import re
import sys
from abc import ABC, abstractmethod
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Optional

from pydantic import BaseModel, Field

from config import Config
from db.postgres_client import get_postgres_client
from ingestion.processor.base import BaseProcessor
from ingestion.repository.circular_repository import CircularRepository, CircularRecord
from ingestion.repository.asset_repository import AssetRepository
from ingestion.repository.circular_reference_repository import (
    CircularReference,
    CircularReferenceRepository,
)
from storage.s3_client import get_s3_client
from utils.llm_providers import get_llm_provider


# ─────────────────────────────────────────────────────────────────────────────
# PYDANTIC MODELS — reused from hybrid_reference_extractor
# ─────────────────────────────────────────────────────────────────────────────

class ExtractedReference(BaseModel):
    """LLM extracts normalized circular_id and relationship nature.

    Reused from hybrid_reference_extractor with expanded relationship types
    to support the full Reference Chain Graph schema.
    """
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
# LLM EXTRACTION PROMPT — enhanced from hybrid_reference_extractor
#
# Extended with modifies, rescinds, enforces, implements to match the
# full Reference Chain Graph relationship set.
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
# CANDIDATE MATCH — reused from hybrid_reference_extractor
# ─────────────────────────────────────────────────────────────────────────────

class CandidateMatch:
    """A regex-matched candidate with position and surrounding context.

    Reused from hybrid_reference_extractor. Captures the matched text,
    its byte-position in the source document, the issuing authority source,
    and surrounding text for LLM context.
    """

    def __init__(
        self,
        matched_text: str,
        start: int,
        end: int,
        source: str,
        left_context: str,
        right_context: str,
    ):
        self.matched_text = matched_text
        self.start = start
        self.end = end
        self.source = source
        self.left_context = left_context
        self.right_context = right_context

    def __repr__(self) -> str:
        return (
            f"CandidateMatch(text={self.matched_text!r}, "
            f"pos=[{self.start}:{self.end}], source={self.source!r})"
        )


# ─────────────────────────────────────────────────────────────────────────────
# VALID TOP-LEVEL PREFIXES — from utils.py
# Every valid SEBI circular number must START with one of these codes.
# Kills tail-fragment matches like BOND/CIR-1/2010 or VCF/CIR-1/2010.
# ─────────────────────────────────────────────────────────────────────────────

VALID_TOP_PREFIXES: set[str] = {
    "SEBI", "CIR", "HO", "CFD", "MF", "MFD", "SMD", "TSMD", "SMDRP",
    "FITTC", "D&CC", "IIMARP", "SE", "DEB", "IMD", "MRD", "MIRSD", "DNPD",
}

# Blocklisted words — if present in a candidate, it is NOT a circular number
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

# Relationship priority for deduplication (higher = wins)
REL_PRIORITY: dict[str, int] = {
    "SUPERSEDES":   8,
    "RESCINDS":     7,
    "AMENDS":       6,
    "MODIFIES":     5,
    "CONSOLIDATES": 4,
    "IMPLEMENTS":   3,
    "CLARIFIES":    2,
    "ENFORCES":     1,
    "REFERENCES":   0,
}

# Supported relationship types for the graph
SUPPORTED_RELATIONSHIPS: set[str] = {
    "references", "amends", "supersedes", "clarifies",
    "implements", "modifies", "rescinds", "enforces",
}

# Month name → number mapping for date parsing
MONTH_MAP: dict[str, str] = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}

# Department code → full name mapping
DEPT_MAP: dict[str, str] = {
    "HO": "Head Office",
    "CFD": "Corporation Finance",
    "IMD": "Investment Management",
    "MRD": "Market Regulation",
    "MIRSD": "Market Intermediaries",
    "DNPD": "Derivatives & New Products",
    "AFD": "Alternative Investment Funds",
    "OIAE": "Investor Education & Assistance",
    "ISD": "Integrated Surveillance",
    "MFD": "Mutual Funds Dept",
    "MF": "Mutual Funds",
    "SMDRP": "Secondary Market",
    "SMD": "Secondary Market Dept",
    "SE": "Stock Exchange",
    "FITTC": "FII & Custodian",
    "IIMARP": "Investment Management (Old)",
    "DPS": "Depository Participants",
    "TSMD": "Trading & Settlement",
    "DDHS": "Debt and Hybrid Securities",
}


# ─────────────────────────────────────────────────────────────────────────────
# SANITIZATION — strip control characters that corrupt console output
# and can silently pollute downstream JSON consumers (e.g. \r, \x00, etc.)
# ─────────────────────────────────────────────────────────────────────────────

# Matches any C0 control character except the ones we handle explicitly
# (\r, \n) which are converted to spaces first. \x7f (DEL) is also stripped.
_CONTROL_CHARS_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]+')


def sanitize_text(s: Optional[str]) -> Optional[str]:
    """Strip carriage returns and other control characters from a string.

    This is the ONLY cleanup applied to values that get displayed to the
    user (circular IDs, etc.) — it removes junk that would corrupt
    console/JSON output, but it deliberately does NOT strip meaningful
    spaces, so a circular ID's original PDF spacing survives.
    """
    if not s:
        return s
    s = s.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
    s = _CONTROL_CHARS_RE.sub(' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


# ─────────────────────────────────────────────────────────────────────────────
# NORMALISATION FUNCTIONS — from utils.py
#
# IMPORTANT: normalise_circ_num() and canonicalize_circular_id() below are
# MATCH-KEY normalizers only. They fully strip whitespace so that two
# differently-spaced mentions of the same circular collapse to the same
# key for internal comparisons (validity checks, self-reference filtering,
# deduplication). Their output must NEVER be shown to the user — for
# display, use sanitize_text() (light cleanup) or normalize_text_light()
# (line flattening that preserves genuine spacing) instead.
# ─────────────────────────────────────────────────────────────────────────────

def normalise_circ_num(s: str) -> str:
    """Normalise a raw circular number string into a MATCH-KEY (internal use only).

    Rules:
    - Strip spaces before every slash (e.g. "SEBI /HO" -> "SEBI/HO")
    - Strip ALL spaces after a slash — no exceptions.
    - Strip ANY whitespace sitting directly between two alphanumeric
      characters (e.g. "POD1 I/10421" -> "POD1I/10421"). This handles PDF
      text-extraction artifacts where a stray space is inserted
      mid-segment due to font/kerning quirks — it's noise, not signal,
      so it's always removed for matching purposes.
    - Collapse spaces around dashes
    - Uppercase
    - Strip trailing punctuation

    NOTE: this is a MATCH KEY, not a display value. Never print/return
    the output of this function to the user — use sanitize_text() or
    normalize_text_light() for anything user-facing.
    """
    if not s:
        return ""
    s = sanitize_text(s) or ""
    s = re.sub(r'\s+/', '/', s)                     # strip spaces BEFORE slash
    s = re.sub(r'/\s+', '/', s)                      # strip spaces AFTER slash (no exceptions)
    s = re.sub(r'\s*-\s*', '-', s)                   # collapse spaces around dashes
    s = re.sub(r'(?<=[A-Za-z0-9])\s+(?=[A-Za-z0-9])', '', s)  # kill mid-segment spaces
    s = re.sub(r'\s+', ' ', s)                        # collapse any remaining space runs
    s = s.strip().upper()
    s = s.rstrip('.,;: ')
    return s


def canonicalize_circular_id(circular_number: str) -> Optional[str]:
    """Convert a raw circular number to a stable, URL-safe MATCH-KEY.

    Example:
        SEBI/HO/CFD/CMD/CIR/P/2023/001 → SEBI-HO-CFD-CMD-CIR-P-2023-001

    Strips ALL whitespace, so this is spacing-invariant — the same
    circular always produces the same canonical id regardless of how it
    was spaced in the source text. Used purely for internal matching /
    deduplication; never shown to the user.
    """
    if not circular_number:
        return None
    c = sanitize_text(circular_number) or ""
    c = c.upper()
    c = re.sub(r'\s+', '', c)           # remove all spaces
    c = re.sub(r'[^A-Z0-9/]', '-', c)  # non-alphanum/slash → dash
    c = re.sub(r'/+', '-', c)           # slashes → dash
    c = re.sub(r'-+', '-', c)           # collapse consecutive dashes
    return c.strip('-')


def normalize_text_light(text: str) -> str:
    """Flatten multi-line PDF text into a single line WITHOUT touching
    spacing around slashes or dashes.

    This is the DISPLAY-SAFE flattening function: it only collapses
    newlines/tabs/CR and runs of 2+ plain spaces, so genuine PDF spacing
    (e.g. "POD1/ I/146", "POD1 I/10421") survives untouched and can be
    shown to the user exactly as it appears in the source document.

    The CIRCULAR_PATTERNS regexes tolerate optional whitespace around
    slashes internally (via the `_S = r'\\s*/\\s*'` building block), so
    they still match correctly against this lightly-flattened text.
    """
    text = re.sub(r'[\r\n\t]+', ' ', text)
    text = re.sub(r' {2,}', ' ', text)
    text = re.sub(r'/+', '/', text)
    # Collapse "Master Cir" -> "Master-Cir" so P11 matches — a specific
    # keyword fix-up, not general space stripping.
    text = re.sub(r'\bMaster\s+Cir\b', 'Master-Cir', text, flags=re.IGNORECASE)
    return text.strip()


def normalize_text_for_regex(text: str) -> str:
    """Flatten multi-line PDF text into a single line for MATCH-KEY regex use.

    Normalises slashes and dashes that were split across lines. This
    version DOES strip spacing around slashes/dashes, which makes it
    suitable for internal matching (self-reference detection) but NOT
    for anything shown to the user — use normalize_text_light() for that.
    """
    text = re.sub(r'[\r\n\t]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\s*/\s*', '/', text)
    text = re.sub(r'\s*-\s*', '-', text)
    text = re.sub(r'/+', '/', text)
    text = re.sub(r'\bMaster\s+Cir\b', 'Master-Cir', text, flags=re.IGNORECASE)
    return text.strip()


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION — from utils.py
# ─────────────────────────────────────────────────────────────────────────────

def is_valid_circular_number(circ: str) -> bool:
    """Validate a candidate SEBI circular number.

    Rules (ordered from cheapest to most expensive check):
      1. Length 8–90 characters
      2. At least 2 slashes
      3. Contains a year (4-digit 19xx/20xx) OR ends in 2-digit year
      4. Contains CIR keyword OR starts with a known special prefix
      5. No blocklisted words
      6. First segment not a common English 2-letter word
      7. First segment in VALID_TOP_PREFIXES
    """
    if not circ or len(circ) < 8:
        return False
    if len(circ) > 90:
        return False

    upper = circ.upper()

    if circ.count('/') < 2:
        return False

    has_4y = bool(re.search(r'(?:19|20)\d{2}', circ))
    has_2y = bool(re.search(r'[/_-]\d{2}$', circ))
    if not has_4y and not has_2y:
        return False

    has_cir   = 'CIR' in upper
    is_ho     = upper.startswith('HO/')
    is_fittc  = upper.startswith('FITTC') or upper.startswith('D&CC')
    is_iimarp = upper.startswith('IIMARP')
    is_smd    = upper.startswith('SMD') or upper.startswith('TSMD')
    is_cfd    = upper.startswith('CFD')
    if not (has_cir or is_ho or is_fittc or is_iimarp or is_smd or is_cfd):
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
    """Fuzzy-compare two circular numbers (any spacing/prefix variant) to
    catch near-duplicates — e.g. "SEBI/HO/17/..." vs "HO/17/..." (same
    circular, one with a leading "SEBI/" prefix), or two mentions with
    different internal spacing.

    Both inputs are first pushed through canonicalize_circular_id(), which
    strips ALL whitespace, so spacing differences never prevent a match.
    """
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
# TEXT CLEANING — from extractor.py
# ─────────────────────────────────────────────────────────────────────────────

def clean_text(text: str) -> str:
    """Clean extracted PDF text to fix common issues.

    - Replace non-breaking spaces (\\xa0) with regular spaces
    - Remove garbled/non-printable characters from font encoding issues
    - Normalize internal spaces in SEBI circular numbers
    - Collapse multiple blank lines into one
    - Preserve paragraph structure
    """
    # Step 1: Replace non-breaking spaces
    text = text.replace('\xa0', ' ')
    text = text.replace('\u200b', '')   # zero-width space
    text = text.replace('\ufeff', '')   # byte order mark

    # Step 2: Remove garbled font-encoding characters (common in older SEBI PDFs)
    text = re.sub(
        r'[^\x09\x0A\x0D\x20-\x7E\u00A1-\u024F\u2013\u2014\u2018-\u201F\u2022\u2026]',
        ' ', text,
    )

    # Step 3: Normalize spaces inside SEBI-style circular numbers
    def fix_circular_number(match: re.Match) -> str:
        return re.sub(r'\s*/\s*', '/', match.group(0))

    text = re.sub(
        r'[A-Z]{2,}(?:\s*/\s*[A-Za-z0-9\-]+){2,}',
        fix_circular_number,
        text,
    )

    # Step 4: Normalize lines — strip trailing whitespace per line
    lines = text.split('\n')
    cleaned_lines = [line.rstrip() for line in lines]

    # Step 5: Collapse runs of 3+ blank lines into 2 blank lines
    result_lines: list[str] = []
    blank_count = 0
    for line in cleaned_lines:
        if line.strip() == '':
            blank_count += 1
            if blank_count <= 2:
                result_lines.append('')
        else:
            blank_count = 0
            result_lines.append(line)

    cleaned = '\n'.join(result_lines).strip()
    return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# SEBI CIRCULAR REGEX PATTERNS — 11 patterns from llm_extractor.py
# Covers ALL SEBI circular formats from 1992–2026+
# (P11 added for Master Circular format: SEBI/DEPT/Master-Cir-NN/YYYY)
# ─────────────────────────────────────────────────────────────────────────────

_S    = r'\s*/\s*'                          # slash with optional spaces
_SEG  = r'[A-Za-z0-9][A-Za-z0-9\.\-_]*'   # one path segment

# ── PATCHED (restored) ──────────────────────────────────────────────────
# WAS: _YEAR = r'(?:(?:19|20)\d{2}|\d{2})'
#
# BUG: the bare `\d{2}` alternative is fixed-width and NOT right-bounded.
# Real SEBI numbers often end in .../YYYY/NNN where NNN is a trailing
# serial that can be 1-4 digits (e.g. .../2020/577, .../2023/061). Since
# `\d{2}` only ever consumes exactly 2 digits, on a 3-digit serial like
# "577" it matched only "57" and silently dropped the "7" — truncating
# real circular numbers.
#
# FIX: widen the short alternative to a greedy `\d{1,4}` so it always
# captures the FULL trailing number, whatever its length (1-4 digits),
# while the 4-digit `(19|20)\d{2}` branch still matches real years first.
_YEAR = r'(?:(?:19|20)\d{2}|\d{1,4})'        # 4-digit year, or full 1-4 digit trailing serial
# ── END PATCH ────────────────────────────────────────────────────────────

_4Y   = r'(?:19|20)\d{2}'                  # strict 4-digit year only
_NUM  = r'\d{1,6}'                          # short number
_LNUM = r'\d{1,9}'                          # long number

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
]


# ─────────────────────────────────────────────────────────────────────────────
# DATE EXTRACTION — from llm_extractor.py
# ─────────────────────────────────────────────────────────────────────────────

DATE_RE = re.compile(
    r'(?:'
    r'(\d{1,2})(?:st|nd|rd|th)?\s+'
    r'(January|February|March|April|May|June|July|August|'
    r'September|October|November|December)'
    r'[,\s]+(\d{4})'
    r'|'
    r'(January|February|March|April|May|June|July|August|'
    r'September|October|November|December)'
    r'\s+(\d{1,2})[,\s]+(\d{4})'
    r'|'
    r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})'
    r')',
    re.IGNORECASE,
)

EFFECTIVE_DATE_RE = re.compile(
    r'(?:with\s+effect\s+from|effective\s+(?:from|date)|w\.e\.f\.?)\s+'
    r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})',
    re.IGNORECASE,
)

SUBJECT_RE = re.compile(
    r'(?:Subject\s*[:\-]|Re\s*[:\-]|Sub\s*[:\-]|SUBJECT\s*[:\-])\s*(.+?)(?:\n|$)',
    re.IGNORECASE,
)

ORDINAL_MAP: dict[str, int] = {
    "first": 1, "second": 2, "third": 3, "fourth": 4, "fifth": 5,
    "sixth": 6, "seventh": 7, "eighth": 8, "ninth": 9, "tenth": 10,
}

AMENDMENT_SERIAL_RE = re.compile(
    r'(?:amendment\s+no\.?\s*(\d+)|(\w+)\s+amendment)',
    re.IGNORECASE,
)


def parse_date(text: str) -> Optional[str]:
    """Extract the first parseable date from text, returns YYYY-MM-DD."""
    m = DATE_RE.search(text)
    if not m:
        return None
    try:
        if m.group(1):
            day, month_name, year = m.group(1), m.group(2), m.group(3)
            month = MONTH_MAP.get(month_name.lower(), "01")
        elif m.group(4):
            month_name, day, year = m.group(4), m.group(5), m.group(6)
            month = MONTH_MAP.get(month_name.lower(), "01")
        else:
            day, month, year = m.group(7), m.group(8), m.group(9)
        return f"{year}-{int(month):02d}-{int(day):02d}"
    except Exception:
        return None


def extract_effective_date(text: str) -> Optional[str]:
    """Extract 'with effect from' date separately from issue date."""
    m = EFFECTIVE_DATE_RE.search(text[:5000])
    if not m:
        return None
    try:
        day, month, year = m.group(1), m.group(2), m.group(3)
        if len(year) == 2:
            year = "20" + year if int(year) < 50 else "19" + year
        return f"{year}-{int(month):02d}-{int(day):02d}"
    except Exception:
        return None


def extract_title(text: str) -> Optional[str]:
    """Extract the Subject/Re line from the circular."""
    m = SUBJECT_RE.search(text[:3000])
    if not m:
        return None
    title = m.group(1).strip()
    title = re.sub(r'\s+', ' ', title)
    title = title.rstrip('.,;:-')
    return title[:300] if title else None


def extract_amendment_serial(text: str) -> Optional[int]:
    """Extract amendment sequence number from title.

    'Second Amendment' → 2; 'Amendment No. 7' → 7
    """
    for m in AMENDMENT_SERIAL_RE.finditer(text[:3000]):
        if m.group(1):
            return int(m.group(1))
        word = m.group(2).lower()
        if word in ORDINAL_MAP:
            return ORDINAL_MAP[word]
    return None


# ─────────────────────────────────────────────────────────────────────────────
# DEPARTMENT EXTRACTION — from llm_extractor.py
# ─────────────────────────────────────────────────────────────────────────────

def extract_department(circular_number: str) -> Optional[str]:
    """Extract department code from a SEBI circular number.

    PATCHED: a single path segment can be dash-joined and contain a mix
    of numeric date/serial tokens PLUS the real department code, e.g.
    "12(3)2025-DDHS-POD1" packs a date and "DDHS" together. The old code
    just stripped the "(3)" bracket and returned the whole leftover blob
    ("122025-DDHS-POD1"). Now we split on dashes and prefer the first
    purely-alphabetic token (>=2 chars) as the department code, since
    date/serial tokens never look like that. Purely numeric path
    segments (e.g. "17", "11") are also now skipped when scanning.
    """
    if not circular_number:
        return None
    parts = [p.strip() for p in circular_number.replace('&', '/').split('/') if p.strip()]
    if not parts:
        return None
    first = parts[0].upper()
    if first == "SEBI":
        for p in parts[1:]:
            pu = p.upper()
            if pu in ("HO", "SEBI", "P") or pu.startswith("CIR") or pu.isdigit():
                continue
            cleaned_seg = re.sub(r'\(.*?\)', '', pu)
            sub_tokens = re.split(r'[-_]', cleaned_seg)
            alpha_tok = next(
                (t for t in sub_tokens if t and t.isalpha() and len(t) >= 2),
                None,
            )
            if alpha_tok:
                return alpha_tok
            clean = cleaned_seg.strip('-').strip()
            if clean and not clean.isdigit():
                return clean
        return parts[1].upper() if len(parts) > 1 else None
    if first == "CIR" and len(parts) > 1:
        return parts[1].upper()
    if first == "HO":
        for p in parts[3:]:
            m = re.search(r'(?:\d+)?\(\d+\)\d*[-\s]*([A-Z]{2,12})', p, re.IGNORECASE)
            if m:
                return m.group(1).upper()
            m = re.search(r'\(\d+\)\d*[-\s]*([A-Z]{2,12})', p, re.IGNORECASE)
            if m:
                return m.group(1).upper()
        for p in parts[1:]:
            m = re.search(r'(?:\d+)?\(\d+\)\d*[-\s]*([A-Z]{2,12})', p, re.IGNORECASE)
            if m:
                return m.group(1).upper()
        return "HO"
    if first == "CFD" and len(parts) > 1:
        return "CFD"
    if first in ("FITTC", "D&CC"):
        return "FITTC"
    if first == "IIMARP":
        return "IIMARP"
    if first in ("SMD", "TSMD", "SMDRP"):
        return first
    if first in ("MF", "MFD"):
        return "MFD"
    if first == "SE" and len(parts) > 1:
        return "SE"
    return first


def classify_circular_type(text: str) -> str:
    """Classify a circular as MASTER, AMENDMENT, or REGULAR."""
    tl = text.lower()
    if any(kw in tl for kw in [
        "master circular", "consolidated circular", "consolidation of circulars",
        "compilation of circulars", "compilation of all circulars",
        "combined circular", "compiling all circulars",
    ]):
        return "MASTER"
    if any(kw in tl for kw in [
        "in partial modification", "amend", "substitut", "replac",
        "in supersession", "modif", "addendum", "corrigendum",
    ]):
        return "AMENDMENT"
    return "REGULAR"


# ─────────────────────────────────────────────────────────────────────────────
# CIRCULAR NUMBER EXTRACTION — from llm_extractor.py
# ─────────────────────────────────────────────────────────────────────────────

def extract_all_circular_numbers_with_raw(text: str) -> list[tuple[str, str]]:
    """Find all valid SEBI circular numbers, returning (raw_display, match_key) pairs.

    raw_display preserves the number's original spacing exactly as it
    appears in the (lightly-flattened, spacing-preserving) source text —
    this is what should be SHOWN to the user.
    match_key is the fully space-stripped form, used purely internally
    for validity checks, self-reference filtering, and dedup — never
    shown to the user.
    """
    found: list[tuple[str, str]] = []
    seen: set[str] = set()
    for pattern in CIRCULAR_PATTERNS:
        for match in pattern.finditer(text):
            raw = sanitize_text(match.group(0)) or match.group(0).strip()
            norm = normalise_circ_num(raw)
            if norm in seen:
                continue
            if not is_valid_circular_number(norm):
                continue
            seen.add(norm)
            found.append((raw, norm))
    return found


def extract_all_circular_numbers(text: str) -> list[str]:
    """Find all valid SEBI circular numbers in text. Deduplicates.

    Backward-compatible wrapper returning only the match-key (fully
    space-stripped) form, for callers that only need it for internal
    matching (e.g. self-reference filtering) rather than display.
    """
    return [norm for _raw, norm in extract_all_circular_numbers_with_raw(text)]


# ─────────────────────────────────────────────────────────────────────────────
# METADATA EXTRACTION — from llm_extractor.py
# ─────────────────────────────────────────────────────────────────────────────

def extract_circular_metadata(text: str) -> dict:
    """Extract all metadata fields from a SEBI circular's text.

    Returns dict with: circular_number, canonical_id, issue_date,
    effective_date, department, circular_type, title, amendment_serial.

    circular_number is the DISPLAY value (original PDF spacing preserved
    via normalize_text_light()); canonical_id is the spacing-invariant
    match-key derived from it.
    """
    header_light = normalize_text_light(text[:2000])
    all_matches = extract_all_circular_numbers_with_raw(header_light)
    circular_number = all_matches[0][0] if all_matches else None

    issue_date = parse_date(text[:3000])
    effective_date = extract_effective_date(text)
    department = extract_department(circular_number) if circular_number else None
    circular_type = classify_circular_type(text) if circular_number else "NOT_A_CIRCULAR"
    title = extract_title(text)
    amendment_serial = None
    if circular_type == "AMENDMENT":
        amendment_serial = extract_amendment_serial(text)

    canonical_id = canonicalize_circular_id(circular_number) if circular_number else None

    return {
        "circular_number":  circular_number,
        "canonical_id":     canonical_id,
        "issue_date":       issue_date,
        "effective_date":   effective_date,
        "department":       department,
        "circular_type":    circular_type,
        "title":            title,
        "amendment_serial": amendment_serial,
    }


# ─────────────────────────────────────────────────────────────────────────────
# RELATIONSHIP CLASSIFICATION — from llm_extractor.py
# Extended with modifies, rescinds, enforces, implements
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
# GRAPH FACTORIES — from build_graph.py
# ─────────────────────────────────────────────────────────────────────────────

def create_graph_node(
    circ_id:          str,
    issue_date:       Optional[str] = None,
    effective_date:   Optional[str] = None,
    circular_type:    Optional[str] = None,
    department:       Optional[str] = None,
    title:            Optional[str] = None,
    amendment_serial: Optional[int] = None,
    node_type:        str = "referenced",
    source_type:      str = "stub",
    is_stub:          bool = True,
) -> dict:
    """Create a graph node with the full Reference Chain Graph schema.

    circ_id is kept as its DISPLAY form (sanitize_text only — spacing
    preserved); canonical_id is derived separately as the match-key.
    """
    circ_id = sanitize_text(circ_id)
    canonical_id = canonicalize_circular_id(circ_id)
    dept_full = DEPT_MAP.get(department, department) if department else None

    return {
        # Identity
        "id":               circ_id,
        "canonical_id":     canonical_id,
        "label":            circ_id,
        "title":            sanitize_text(title),
        # Dates
        "issue_date":       issue_date,
        "effective_date":   effective_date,
        # Classification
        "department":       department,
        "department_full":  dept_full,
        "circular_type":    circular_type or "REGULAR",
        "amendment_serial": amendment_serial,
        # Graph metadata
        "node_type":        node_type,
        "source_type":      source_type,
        "is_stub":          is_stub,
        "lifecycle_status": "UNKNOWN",
        # Metrics (computed after edge generation)
        "incoming_edges":   0,
        "outgoing_edges":   0,
        "importance_score": 0,
        # Extensible metadata
        "metadata":         {},
    }


def create_graph_edge(
    source:              str,
    target:              str,
    relationship:        str = "references",
    confidence:          float = 0.5,
    trigger_phrase:      Optional[str] = None,
    target_canonical_id: Optional[str] = None,
) -> dict:
    """Create a graph edge with the full Reference Chain Graph schema."""
    source = sanitize_text(source)
    target = sanitize_text(target)
    return {
        "source":              source,
        "target":              target,
        "source_canonical_id": canonicalize_circular_id(source),
        "target_canonical_id": target_canonical_id or canonicalize_circular_id(target),
        "relationship":        relationship.lower(),
        "confidence":          confidence,
        "trigger_phrase":      sanitize_text(trigger_phrase),
        "direction":           "OUTGOING",
    }


# =============================================================================
#
#  SCHEMA REFERENCE EXTRACTOR — Main Processor Class
#
# =============================================================================

class SchemaReferenceExtractor(BaseProcessor):
    """R&D processor that builds a Reference Chain Graph from a single SEBI circular."""

    name = "schema_reference_extractor"

    # ── Two-pass regex patterns — reused from hybrid_reference_extractor ──────

    PATTERNS_STRICT: list[re.Pattern] = [
        re.compile(r'SEBI/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'HO/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'NSE/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'NCL/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'AFD/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'CIR/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'IMD/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'MIRSD/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'MRD/[\w/().\-]+', re.IGNORECASE),
        re.compile(r'sebi-[\w/().\-]+', re.IGNORECASE),
        re.compile(r'ho-[\w/().\-]+', re.IGNORECASE),
        re.compile(r'CFD/POD[\w/().\-]+', re.IGNORECASE),
        re.compile(r'\b[CML][A-Z]{2,3}\d{5,6}\b', re.IGNORECASE),
        re.compile(r'HO_[\w/().\-]+', re.IGNORECASE),
    ]

    # -------------------------------------------------------------------------
    # PERSISTENCE: save extracted references
    # -------------------------------------------------------------------------

    def _persist_references(
        self, record: CircularRecord, resolved: list[dict],
    ) -> None:
        """Persist all deduplicated resolved references to circular_references.

        Uses the already-resolved list produced by _resolve_references() so
        reference_uuid FK values are available without a second DB lookup.
        Delegates entirely to CircularReferenceRepository -- no SQL here.

        This method is idempotent: re-running the processor for the same
        circular replaces all existing rows for that circular_uuid.
        """
        refs: list[CircularReference] = []
        seen: set[str] = set()

        for ref in resolved:
            ref_text = sanitize_text(ref.get("reference_circular_no") or "")
            if not ref_text or ref_text in seen:
                continue
            seen.add(ref_text)

            relationship = ref.get("relationship") or "references"
            reference_uuid = ref.get("reference_circular_id")  # UUID | None

            refs.append(
                CircularReference(
                    relationship=relationship,
                    reference_text=ref_text,
                    reference_uuid=reference_uuid,
                )
            )

        try:
            self.reference_repo.replace_references(record.id, refs)
            self.logger.info(
                "Persisted %d reference(s) for circular_id=%s",
                len(refs),
                record.circular_id,
            )
        except Exception:
            self.logger.exception(
                "Failed to persist references for circular_id=%s",
                record.circular_id,
            )

    # -------------------------------------------------------------------------
    # STEP 1: LOCATE PDF
    # -------------------------------------------------------------------------

    PATTERNS_LOOSE: list[re.Pattern] = [
        re.compile(r'SEBI/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'HO/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'NSE/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'NCL/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'AFD/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'CIR/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'IMD/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'MIRSD/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'MRD/[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'sebi-[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'ho-[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'CFD/POD[\w/().\- \n]+', re.IGNORECASE),
        re.compile(r'\b[CML][A-Z]{2,3}\d{5,6}\b', re.IGNORECASE),
        re.compile(r'HO_[\w/().\- \n]+', re.IGNORECASE),
    ]

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

    def __init__(self, db_pool: Any):
        super().__init__(db_pool)
        self.circular_repo = CircularRepository(db_pool)
        self.asset_repo = AssetRepository(db_pool)
        self.reference_repo = CircularReferenceRepository(db_pool)

    # ─────────────────────────────────────────────────────────────────────────
    # MAIN PIPELINE
    # ─────────────────────────────────────────────────────────────────────────

    def process(self, record: CircularRecord) -> dict:
        self.logger.info(
            "Starting reference extraction for circular_id=%s",
            sanitize_text(record.circular_id),
        )

        # Step 1: Locate PDF
        pdf_path = self._get_pdf_path(record)
        self.logger.info("PDF located: %s", pdf_path)

        # Step 2: Extract text
        raw_text, extraction_method, page_count = self._extract_text(pdf_path)
        if not raw_text.strip():
            self.logger.warning(
                "Empty text extracted for circular_id=%s", record.circular_id,
            )
            result = self._build_empty_result(record, pdf_path)
            self._print_reference_output(result)
            return result

        # Step 3: Extract metadata
        metadata = self._extract_metadata(raw_text)
        self.logger.info(
            "Metadata: circular_number=%s, department=%s",
            metadata.get("circular_number"),
            metadata.get("department"),
        )

        # Step 4: Extract reference candidates (two-pass regex)
        candidates = self._extract_candidates(raw_text, metadata)
        self.logger.info("Found %d regex candidates", len(candidates))

        # Step 5: Normalize references using SEBI patterns (covers 1992-2026 formats)
        normalized = self._normalize_references(raw_text, metadata)
        self.logger.info("Found %d normalized references (SEBI patterns)", len(normalized))

        # Step 6: LLM classification — ONLY for candidates regex couldn't
        # confidently classify.
        llm_results: list[dict] = []
        ambiguous_candidates = self._select_candidates_for_llm(candidates, normalized)
        if ambiguous_candidates:
            llm_results = self._classify_references_llm(ambiguous_candidates, raw_text)
            self.logger.info(
                "LLM classified %d/%d candidates (rest resolved by regex trigger-phrase match)",
                len(ambiguous_candidates), len(candidates),
            )

        # Step 7: Merge and deduplicate
        merged = self._deduplicate_references(
            llm_results, normalized, record, metadata,
        )
        self.logger.info("After deduplication: %d unique references", len(merged))

        # Step 8: Resolve references
        resolved = self._resolve_references(merged)

        # Step 9: Build minimal output (source + references only -- nothing else)
        result = self._build_reference_output(record, metadata, resolved)

        # Step 10: Persist extracted references to circular_references table
        self._persist_references(record, resolved)

        self._print_reference_output(result)

        self.logger.info(
            "Reference extraction complete: %d references for circular_id=%s",
            result["total_references"], record.circular_id,
        )
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 1: LOCATE PDF
    # ─────────────────────────────────────────────────────────────────────────

    def _get_pdf_path(self, record: CircularRecord) -> str:
        assets = self.asset_repo.list_assets(record.id)
        for role in ['extracted_pdf', 'original_pdf']:
            for asset in assets:
                if (
                    asset.asset_role == role
                    and asset.file_path
                    and asset.file_path.lower().endswith('.pdf')
                ):
                    return asset.file_path

        if record.file_path and record.file_path.lower().endswith('.pdf'):
            return record.file_path

        raise ValueError(f"No PDF file path found for circular: {record.circular_id}")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 2: EXTRACT TEXT
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_text(self, pdf_path: str) -> tuple[str, str, int]:
        self.logger.info("Extracting text from PDF: %s", pdf_path)

        target = Path(pdf_path)
        page_count = 0

        if str(pdf_path).startswith("s3://"):
            s3_client = get_s3_client()
            pdf_bytes = s3_client.download_bytes(str(pdf_path))
            tmp_dir_ctx = TemporaryDirectory()
            tmp_dir = tmp_dir_ctx.__enter__()
            tmp_path = Path(tmp_dir) / "doc.pdf"
            tmp_path.write_bytes(pdf_bytes)
            actual_path = str(tmp_path)
        else:
            if not target.exists():
                raise FileNotFoundError(f"PDF file not found: {pdf_path}")
            actual_path = str(target)
            tmp_dir_ctx = None

        try:
            text, method, page_count = self._extract_with_pymupdf(actual_path)
            if len(text.strip()) >= 200:
                cleaned = clean_text(text)
                self.logger.info(
                    "PyMuPDF succeeded — %d characters (after cleaning)", len(cleaned),
                )
                return cleaned, method, page_count

            self.logger.warning(
                "PyMuPDF got only %d chars — trying pdfplumber fallback",
                len(text.strip()),
            )

            text, method, page_count = self._extract_with_pdfplumber(actual_path)
            cleaned = clean_text(text)
            self.logger.info(
                "pdfplumber extracted %d characters (after cleaning)", len(cleaned),
            )
            return cleaned, method, page_count

        finally:
            if tmp_dir_ctx is not None:
                tmp_dir_ctx.__exit__(None, None, None)

    def _extract_with_pymupdf(self, pdf_path: str) -> tuple[str, str, int]:
        try:
            import fitz  # PyMuPDF
        except ImportError:
            try:
                import pymupdf as fitz
            except ImportError:
                return "", "failed (missing PyMuPDF)", 0

        text_parts: list[str] = []
        with fitz.open(pdf_path) as doc:
            page_count = len(doc)
            for page in doc:
                blocks = page.get_text("blocks", sort=True)
                page_text = ""
                for block in blocks:
                    if len(block) >= 5 and block[6] == 0:
                        page_text += block[4] + "\n"
                text_parts.append(page_text)

        return "\n".join(text_parts), "pymupdf", page_count

    def _extract_with_pdfplumber(self, pdf_path: str) -> tuple[str, str, int]:
        try:
            import pdfplumber
        except ImportError as exc:
            raise RuntimeError(
                "pdfplumber is not installed. Install it for scanned PDF support."
            ) from exc

        text_parts: list[str] = []
        page_count = 0
        with pdfplumber.open(pdf_path) as pdf:
            page_count = len(pdf.pages)
            for page in pdf.pages:
                page_text = page.extract_text(x_tolerance=2, y_tolerance=2)
                if page_text:
                    text_parts.append(page_text)

        return "\n".join(text_parts), "pdfplumber", page_count

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 3: EXTRACT METADATA
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_metadata(self, text: str) -> dict:
        return extract_circular_metadata(text)

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4: EXTRACT CANDIDATES
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_candidates(self, text: str, metadata: dict) -> list[CandidateMatch]:
        strict_matches = self._extract_with_patterns(text, self.PATTERNS_STRICT)

        strict_positions = {(m.start, m.end) for m in strict_matches}
        loose_matches = self._extract_with_patterns(text, self.PATTERNS_LOOSE)
        for m in loose_matches:
            if (m.start, m.end) not in strict_positions:
                strict_matches.append(m)

        deduped = self._deduplicate_by_position(strict_matches)

        main_circ = metadata.get("circular_number")
        main_norm = normalise_circ_num(main_circ) if main_circ else None
        if not main_norm:
            header_light = normalize_text_light(text[:2000])
            auto_matches = extract_all_circular_numbers(header_light)
            main_norm = auto_matches[0] if auto_matches else None

        filtered = []
        for c in deduped:
            norm_num = normalise_circ_num(c.matched_text)

            is_self = False
            if main_norm:
                mn = normalise_circ_num(main_norm)
                nn = norm_num
                mn_core = mn[5:] if mn.startswith("SEBI/") else mn
                nn_core = nn[5:] if nn.startswith("SEBI/") else nn
                if (mn == nn
                        or mn_core == nn_core
                        or mn.endswith("/" + nn)
                        or nn.endswith("/" + mn)
                        or mn_core.endswith(nn_core)
                        or nn_core.endswith(mn_core)):
                    is_self = True

            if main_norm and is_same_circular(norm_num, main_norm):
                is_self = True

            if not is_self:
                filtered.append(c)

        return filtered

    def _extract_with_patterns(
        self,
        text: str,
        patterns: list[re.Pattern],
    ) -> list[CandidateMatch]:
        matches: list[CandidateMatch] = []
        for pattern in patterns:
            for match in pattern.finditer(text):
                matched_str = match.group().strip()
                matched_str = re.sub(r'[\r\n ]+', ' ', matched_str).strip()
                if not matched_str:
                    continue

                start = match.start()
                end = match.end()

                source: Optional[str] = None
                for src_pattern, src in self.SOURCE_PATTERNS:
                    if src_pattern.match(matched_str):
                        source = src
                        break

                if not source:
                    continue

                left_start = max(0, start - 50)
                right_end = min(len(text), end + 50)
                left_context = text[left_start:start].replace('\r', ' ').replace('\n', ' ').strip()
                right_context = text[end:right_end].replace('\r', ' ').replace('\n', ' ').strip()

                matches.append(CandidateMatch(
                    matched_text=matched_str,
                    start=start,
                    end=end,
                    source=source,
                    left_context=left_context,
                    right_context=right_context,
                ))

        return matches

    def _deduplicate_by_position(
        self, matches: list[CandidateMatch],
    ) -> list[CandidateMatch]:
        if not matches:
            return []

        sorted_matches = sorted(
            matches, key=lambda m: (m.start, -(m.end - m.start)),
        )
        result: list[CandidateMatch] = []
        last_end = -1

        for m in sorted_matches:
            if m.start >= last_end:
                result.append(m)
                last_end = m.end

        normalized_map: dict[str, CandidateMatch] = {}
        for m in result:
            norm = re.sub(r'[\r\n ]+', ' ', m.matched_text).strip()
            existing = normalized_map.get(norm)
            if existing is None:
                normalized_map[norm] = m
            else:
                if (m.end - m.start) > (existing.end - existing.start):
                    normalized_map[norm] = m

        return list(normalized_map.values())

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 5: NORMALIZE REFERENCES
    # ─────────────────────────────────────────────────────────────────────────

    def _normalize_references(
        self, text: str, metadata: dict,
    ) -> list[dict]:
        """Extract references using SEBI's pattern set, preserving PDF spacing
        in the display field while using a fully space-stripped match-key
        internally for validity/self-reference/dedup checks.
        """
        norm_text = normalize_text_light(text)
        main_circ = metadata.get("circular_number")
        main_norm = normalise_circ_num(main_circ) if main_circ else None

        if not main_norm:
            header_light = normalize_text_light(text[:2000])
            auto_matches = extract_all_circular_numbers(header_light)
            main_norm = auto_matches[0] if auto_matches else None

        results: list[dict] = []
        seen: set[str] = set()

        for pattern in CIRCULAR_PATTERNS:
            for m in pattern.finditer(norm_text):
                raw_num = m.group(0)
                display_num = sanitize_text(raw_num) or raw_num.strip()  # spacing as in PDF
                norm_num = normalise_circ_num(raw_num)                   # match-key only

                if not is_valid_circular_number(norm_num):
                    continue

                # Self-reference check (match-key only)
                if main_norm:
                    mn = normalise_circ_num(main_norm)
                    nn = norm_num
                    mn_core = mn[5:] if mn.startswith("SEBI/") else mn
                    nn_core = nn[5:] if nn.startswith("SEBI/") else nn
                    if (mn == nn
                            or mn_core == nn_core
                            or mn.endswith("/" + nn)
                            or nn.endswith("/" + mn)
                            or mn_core.endswith(nn_core)
                            or nn_core.endswith(mn_core)):
                        continue

                if main_norm and is_same_circular(norm_num, main_norm):
                    continue
                if any(is_same_circular(norm_num, s) for s in seen):
                    continue
                seen.add(norm_num)

                ctx = get_context(norm_text, m.start(), m.end())
                rel_type, trigger = classify_relationship(ctx)
                ref_date = parse_date(ctx)
                target_canonical_id = canonicalize_circular_id(norm_num)

                results.append({
                    "referenced_circular_number": display_num,  # PDF spacing preserved
                    "target_canonical_id":        target_canonical_id,
                    "referenced_date":            ref_date,
                    "relationship_type":          rel_type,
                    "trigger_phrase":             sanitize_text(trigger),
                    "confidence":                 0.85 if rel_type != "REFERENCES" else 0.60,
                })

        return results

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 6: LLM CLASSIFICATION
    # ─────────────────────────────────────────────────────────────────────────

    def _select_candidates_for_llm(
        self,
        candidates: list[CandidateMatch],
        normalized: list[dict],
    ) -> list[CandidateMatch]:
        """Filter candidates down to only those regex couldn't confidently classify.

        PATCHED: matches candidates against confidently-classified
        canonical IDs using fuzzy is_same_circular() comparison instead of
        exact set membership, so a candidate written with/without a
        "SEBI/" prefix (or with different spacing) is still correctly
        recognized as already-classified and skipped from the LLM call.
        """
        confident_canons: list[str] = []
        for ref in normalized:
            is_confident = not (
                ref["relationship_type"] == "REFERENCES"
                and ref.get("trigger_phrase") == "referred to"
            )
            if is_confident:
                canon = ref.get("target_canonical_id") or canonicalize_circular_id(
                    ref["referenced_circular_number"]
                )
                if canon:
                    confident_canons.append(canon)

        needs_llm: list[CandidateMatch] = []
        for c in candidates:
            norm = normalise_circ_num(c.matched_text)
            canon = canonicalize_circular_id(norm)
            already_confident = canon is not None and any(
                is_same_circular(canon, cc) for cc in confident_canons
            )
            if already_confident:
                continue
            needs_llm.append(c)

        return needs_llm

    def _classify_references_llm(
        self,
        candidates: list[CandidateMatch],
        text: str,
    ) -> list[dict]:
        """Call LLM to normalize IDs and classify relationships for all candidates.

        NOTE: circular_id is only lightly sanitized (sanitize_text), NOT
        run through normalise_circ_num — we want to preserve the LLM's
        returned spacing as-is for display; matching/dedup elsewhere
        already uses canonicalize_circular_id, which strips all spacing
        regardless, so display spacing here can't break internal matching.
        """
        if not candidates:
            return []

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
                    self.logger.debug(
                        "Skipping LLM result (no slash): %s", clean_id,
                    )
                    continue
                _PROSE_WORDS = {"DATED", "CIRCULAR", "MASTER", "VIDE", "THE", "AND", "FOR"}
                id_upper = clean_id.upper()
                if any(w in id_upper for w in _PROSE_WORDS) and id_upper.count('/') < 2:
                    self.logger.debug(
                        "Skipping LLM result (looks like prose): %s", clean_id,
                    )
                    continue
                results.append({
                    "reference_circular_no":  clean_id.upper(),
                    "relationship_nature":    (sanitize_text(r.relationship_nature) or "references").lower(),
                })
            return results

        except Exception as e:
            self.logger.warning(
                "LLM classification failed: %s — falling back to regex candidates", e,
            )
            return self._fallback_classification(candidates)

    def _fallback_classification(
        self, candidates: list[CandidateMatch],
    ) -> list[dict]:
        """When LLM fails, return regex candidates with default relationship.

        Preserves spacing as extracted — no normalise_circ_num() call here.
        """
        results: list[dict] = []
        for c in candidates:
            clean_id = sanitize_text(c.matched_text)
            if not clean_id:
                continue
            results.append({
                "reference_circular_no":  clean_id.upper(),
                "relationship_nature":    "references",
            })
        return results

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 7: MERGE AND DEDUPLICATE
    # ─────────────────────────────────────────────────────────────────────────

    def _deduplicate_references(
        self,
        llm_results: list[dict],
        normalized: list[dict],
        record: CircularRecord,
        metadata: dict,
    ) -> list[dict]:
        """Merge LLM results with SEBI-pattern results, deduplicate, and remove self-refs.

        PATCHED: merging now uses fuzzy is_same_circular() matching against
        existing ref_map keys instead of requiring an EXACT canonical-id
        string match. This is what fixes duplicate entries like
        "SEBI/HO/17/..." vs "HO/17/..." (same circular, one with a leading
        "SEBI/" prefix) — they're now recognized as the same circular and
        merged into ONE output entry, keeping the higher-priority
        relationship and the longer/fuller raw ID string for display.
        """
        ref_map: dict[str, dict] = {}

        def find_existing_key(canon: str) -> Optional[str]:
            if canon in ref_map:
                return canon
            for existing_key in ref_map:
                if is_same_circular(canon, existing_key):
                    return existing_key
            return None

        # Layer 1: SEBI-pattern normalized results
        for ref in normalized:
            circ_no = ref["referenced_circular_number"]
            canon = canonicalize_circular_id(circ_no) or circ_no.upper()
            key = find_existing_key(canon)
            if key is None:
                ref_map[canon] = {
                    "reference_circular_no":  circ_no,
                    "canonical_id":           canon,
                    "relationship":           ref["relationship_type"].lower(),
                    "confidence":             ref.get("confidence", 0.60),
                    "trigger_phrase":         ref.get("trigger_phrase"),
                    "referenced_date":        ref.get("referenced_date"),
                    "target_canonical_id":    ref.get("target_canonical_id"),
                    "source":                 "sebi_patterns",
                }
            else:
                existing = ref_map[key]
                new_rel = ref["relationship_type"].upper()
                old_rel = existing["relationship"].upper()
                if REL_PRIORITY.get(new_rel, 0) > REL_PRIORITY.get(old_rel, 0):
                    existing["relationship"] = ref["relationship_type"].lower()
                    existing["trigger_phrase"] = ref.get("trigger_phrase")
                    existing["confidence"] = ref.get("confidence", 0.60)
                if len(circ_no) > len(existing["reference_circular_no"]):
                    existing["reference_circular_no"] = circ_no
                    existing["canonical_id"] = canon

        # Layer 2: LLM results
        for ref in llm_results:
            circ_no = ref["reference_circular_no"]
            canon = canonicalize_circular_id(circ_no) or circ_no.upper()
            key = find_existing_key(canon)
            if key is None:
                ref_map[canon] = {
                    "reference_circular_no":  circ_no,
                    "canonical_id":           canon,
                    "relationship":           ref["relationship_nature"].lower(),
                    "confidence":             0.75,
                    "trigger_phrase":         None,
                    "referenced_date":        None,
                    "target_canonical_id":    canon,
                    "source":                 "llm",
                }
            else:
                existing = ref_map[key]
                new_rel = ref["relationship_nature"].upper()
                old_rel = existing["relationship"].upper()
                if REL_PRIORITY.get(new_rel, 0) > REL_PRIORITY.get(old_rel, 0):
                    existing["relationship"] = ref["relationship_nature"].lower()
                    existing["confidence"] = max(existing.get("confidence", 0.60), 0.75)
                    if existing["source"] == "sebi_patterns":
                        existing["source"] = "merged"
                # NOTE: deliberately NOT overwriting reference_circular_no /
                # canonical_id here even if the LLM's string is "longer".
                # The LLM reformats/normalizes IDs (e.g. adding a "SEBI/"
                # prefix that isn't in the PDF, or dropping a genuine
                # space) — it is not a verbatim quote of the source text.
                # If this entry already exists (from regex/sebi_patterns),
                # that regex-extracted string IS the verbatim PDF text and
                # must win for display purposes, regardless of length.
                # The LLM's text is only ever used as the display value
                # when regex found NOTHING for this circular at all (i.e.
                # the "if key is None" branch above, not this one).

        # Remove self-references
        current_ids: set[str] = set()
        if record.circular_id:
            current_ids.add(record.circular_id.upper())
            cid_canon = canonicalize_circular_id(record.circular_id)
            if cid_canon:
                current_ids.add(cid_canon)
        if hasattr(record, 'full_reference') and record.full_reference:
            current_ids.add(record.full_reference.upper())
            fr_canon = canonicalize_circular_id(record.full_reference)
            if fr_canon:
                current_ids.add(fr_canon)
        if metadata.get("circular_number"):
            current_ids.add(metadata["circular_number"].upper())
            cn_canon = canonicalize_circular_id(metadata["circular_number"])
            if cn_canon:
                current_ids.add(cn_canon)

        result: list[dict] = []
        for canon, ref in ref_map.items():
            ref_upper = ref["reference_circular_no"].upper()
            if ref_upper in current_ids or canon in current_ids:
                continue
            if any(
                is_same_circular(ref_upper, cid)
                for cid in current_ids
            ):
                continue

            rel = ref["relationship"].lower()
            if rel not in SUPPORTED_RELATIONSHIPS:
                rel_map = {
                    "partial_amendment": "amends",
                    "consolidates": "references",
                }
                rel = rel_map.get(rel, "references")
                ref["relationship"] = rel

            result.append(ref)

        return result

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 8: RESOLVE REFERENCES
    # ─────────────────────────────────────────────────────────────────────────

    def _resolve_references(self, references: list[dict]) -> list[dict]:
        for ref in references:
            circ_no = ref["reference_circular_no"]
            try:
                resolved = self.circular_repo.get_record_by_full_reference(circ_no)
                ref["reference_circular_id"] = resolved.id if resolved else None
                ref["resolved"] = resolved is not None
            except Exception:
                ref["reference_circular_id"] = None
                ref["resolved"] = False

        return references

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 9: GENERATE GRAPH NODES
    # ─────────────────────────────────────────────────────────────────────────

    def _generate_nodes(
        self,
        record: CircularRecord,
        metadata: dict,
        references: list[dict],
    ) -> list[dict]:
        nodes: list[dict] = []

        source_node = create_graph_node(
            circ_id=metadata.get("circular_number") or record.circular_id,
            issue_date=metadata.get("issue_date"),
            effective_date=metadata.get("effective_date"),
            circular_type=metadata.get("circular_type"),
            department=metadata.get("department"),
            title=metadata.get("title"),
            amendment_serial=metadata.get("amendment_serial"),
            node_type="source",
            source_type="pdf_extracted",
            is_stub=False,
        )
        source_node["metadata"] = {
            "record_id":     record.id,
            "circular_id":   record.circular_id,
            "source":        getattr(record, 'source', None),
            "file_path":     getattr(record, 'file_path', None),
        }
        nodes.append(source_node)

        seen_ids: set[str] = {source_node["id"]}
        for ref in references:
            circ_no = ref["reference_circular_no"]
            if circ_no in seen_ids:
                continue
            seen_ids.add(circ_no)

            ref_node = create_graph_node(
                circ_id=circ_no,
                issue_date=ref.get("referenced_date"),
                node_type="referenced",
                source_type="resolved" if ref.get("resolved") else "stub",
                is_stub=not ref.get("resolved", False),
            )
            ref_node["metadata"] = {
                "reference_circular_id": ref.get("reference_circular_id"),
                "resolved":              ref.get("resolved", False),
            }
            nodes.append(ref_node)

        return nodes

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 10: GENERATE GRAPH EDGES
    # ─────────────────────────────────────────────────────────────────────────

    def _generate_edges(
        self,
        record: CircularRecord,
        metadata: dict,
        references: list[dict],
    ) -> list[dict]:
        source_id = metadata.get("circular_number") or record.circular_id
        edges: list[dict] = []
        seen_targets: set[str] = set()

        for ref in references:
            target_id = ref["reference_circular_no"]
            if target_id in seen_targets:
                continue
            seen_targets.add(target_id)

            edge = create_graph_edge(
                source=source_id,
                target=target_id,
                relationship=ref.get("relationship", "references"),
                confidence=ref.get("confidence", 0.5),
                trigger_phrase=ref.get("trigger_phrase"),
                target_canonical_id=ref.get("target_canonical_id"),
            )
            edges.append(edge)

        return edges

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 11: COMPUTE LIFECYCLE STATUS
    # ─────────────────────────────────────────────────────────────────────────

    def _compute_lifecycle_status(
        self, nodes: list[dict], edges: list[dict],
    ) -> None:
        superseded_ids: set[str] = set()
        amended_ids: set[str] = set()

        for edge in edges:
            rel = edge.get("relationship", "").lower()
            target = edge["target"]
            if rel in ("supersedes", "rescinds"):
                superseded_ids.add(target)
            elif rel in ("amends", "modifies"):
                amended_ids.add(target)

        for node in nodes:
            nid = node["id"]
            if nid in superseded_ids:
                node["lifecycle_status"] = "SUPERSEDED"
            elif nid in amended_ids:
                node["lifecycle_status"] = "AMENDED"
            elif node.get("is_stub"):
                node["lifecycle_status"] = "UNKNOWN"
            else:
                node["lifecycle_status"] = "ACTIVE"

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 12: COMPUTE NODE METRICS
    # ─────────────────────────────────────────────────────────────────────────

    def _compute_node_metrics(
        self, nodes: list[dict], edges: list[dict],
    ) -> None:
        incoming: Counter = Counter()
        outgoing: Counter = Counter()

        for edge in edges:
            outgoing[edge["source"]] += 1
            incoming[edge["target"]] += 1

        for node in nodes:
            nid = node["id"]
            inc = incoming.get(nid, 0)
            out = outgoing.get(nid, 0)
            node["incoming_edges"] = inc
            node["outgoing_edges"] = out
            node["importance_score"] = inc * 2 + out

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 13: BUILD REFERENCE CHAIN
    # ─────────────────────────────────────────────────────────────────────────

    def _build_reference_chain(
        self, nodes: list[dict], edges: list[dict],
    ) -> dict:
        rel_counts: Counter = Counter()
        for edge in edges:
            rel_counts[edge.get("relationship", "references")] += 1

        lifecycle_counts: Counter = Counter()
        stub_count = 0
        for node in nodes:
            lifecycle_counts[node.get("lifecycle_status", "UNKNOWN")] += 1
            if node.get("is_stub"):
                stub_count += 1

        chain: dict = {
            "meta": {
                "pipeline_version":  "1.0.0",
                "processor":         self.name,
                "built_at":          datetime.now(timezone.utc).isoformat(),
                "total_nodes":       len(nodes),
                "total_edges":       len(edges),
                "stub_nodes":        stub_count,
                "full_nodes":        len(nodes) - stub_count,
                "relationship_distribution": dict(rel_counts),
                "lifecycle_counts":  dict(lifecycle_counts),
            },
            "nodes": nodes,
            "edges": edges,
        }

        return chain

    # ─────────────────────────────────────────────────────────────────────────
    # OUTPUT: STRUCTURED CONSOLE PRINT
    # ─────────────────────────────────────────────────────────────────────────

    def _build_reference_output(
        self,
        record: CircularRecord,
        metadata: dict,
        resolved: list[dict],
    ) -> dict:
        """Build the minimal output: source circular details + reference list only.

        Every string field is run through sanitize_text() only — spacing
        exactly as it appears in the PDF is preserved for both the source
        circular_id and every reference's circular_id. Deduplication of
        the final reference list uses the spacing-invariant canonical
        match-key so spacing differences never cause a duplicate entry,
        but the displayed value itself is never space-stripped.
        """
        source_circ = sanitize_text(metadata.get("circular_number") or record.circular_id)
        source_dept = metadata.get("department")

        references: list[dict] = []
        seen_targets: set[str] = set()
        for ref in resolved:
            target_circ = sanitize_text(ref["reference_circular_no"])
            if not target_circ:
                continue
            dedup_key = canonicalize_circular_id(target_circ) or target_circ.upper()
            if dedup_key in seen_targets:
                continue
            seen_targets.add(dedup_key)

            target_dept = extract_department(target_circ)
            references.append({
                "circular_id":       target_circ,
                "department":        target_dept,
                "department_full":   DEPT_MAP.get(target_dept, target_dept) if target_dept else None,
                "relationship_type": ref.get("relationship", "references"),
                "confidence":        round(float(ref.get("confidence", 0.5)), 2),
                "source_text":       sanitize_text(ref.get("trigger_phrase")),
                "referenced_date":   ref.get("referenced_date"),
            })

        references.sort(key=lambda r: r["confidence"], reverse=True)

        return {
            "source": {
                "circular_id":     source_circ,
                "department":      source_dept,
                "department_full": DEPT_MAP.get(source_dept, source_dept) if source_dept else None,
                "issue_date":      metadata.get("issue_date"),
                "circular_type":   metadata.get("circular_type", "REGULAR"),
            },
            "references": references,
            "total_references": len(references),
        }

    def _build_empty_result(
        self, record: CircularRecord, pdf_path: str,
    ) -> dict:
        """Minimal result when no text could be extracted from the PDF."""
        circ_id = sanitize_text(record.circular_id)
        return {
            "source": {
                "circular_id":     circ_id,
                "department":      None,
                "department_full": None,
                "issue_date":      None,
                "circular_type":   None,
            },
            "references": [],
            "total_references": 0,
            "error": "empty_text_extraction",
        }

    def _print_reference_output(self, result: dict) -> None:
        def clean(v) -> str:
            return sanitize_text(str(v)) if v is not None else ""

        sep = "-" * 60
        src = result["source"]

        sys.stderr.flush()

        print(f"\n{sep}")
        print("  SOURCE CIRCULAR")
        print(sep)
        print(f"  circular_id : {clean(src.get('circular_id')) or 'N/A'}")
        print(f"  department  : {clean(src.get('department_full') or src.get('department')) or 'N/A'}")
        print(f"  issue_date  : {clean(src.get('issue_date')) or 'N/A'}")

        refs = result["references"]
        print(f"\n{sep}")
        print(f"  REFERENCES ({len(refs)})")
        print(sep)
        if not refs:
            print("  (no references found)")
        for i, r in enumerate(refs, 1):
            print(f"  {i}. {clean(r.get('circular_id')) or 'N/A'}")
            print(f"     department : {clean(r.get('department_full') or r.get('department')) or 'N/A'}")
            print(f"     relation   : {clean(r.get('relationship_type'))}")
            print(f"     confidence : {r.get('confidence', 0.0):.2f}")
            if r.get("source_text"):
                print(f"     source     : \"{clean(r.get('source_text'))}\"")
        print(sep)

        if result.get("error"):
            print(f"  note: {clean(result['error'])}")
            print(sep)

        sys.stdout.flush()


# ─────────────────────────────────────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Build a Reference Chain Graph from a SEBI circular.",
    )
    parser.add_argument(
        "--circular_id",
        type=str,
        required=True,
        help="The circular ID to process (e.g. SEBI/HO/CFD/CMD/CIR/P/2023/001)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    logger.info("Building Reference Chain Graph for: %s", args.circular_id)

    try:
        db_client = get_postgres_client()
        pool = db_client.get_pool()
        repo = CircularRepository(pool)

        record = repo.get_record_by_circular_id(args.circular_id)
        if not record:
            print(
                f"No circular found with ID: {args.circular_id}",
                file=sys.stderr,
            )
            sys.exit(1)

        processor = SchemaReferenceExtractor(pool)
        success = processor.run(record)

        if success:
            logger.info("Reference Chain Graph generated successfully.")
        else:
            print("Failed to generate Reference Chain Graph.", file=sys.stderr)
            sys.exit(1)

    except Exception as e:
        logger.exception("Error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()