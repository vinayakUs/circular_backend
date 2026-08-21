from __future__ import annotations

import io
import logging
import re

logger = logging.getLogger(__name__)


# pypdf sometimes emits non-whitespace glyphs as line separators
# (e.g. U+25A0 black square). Normalize them to newlines so the
# extraction regex can rely on whitespace boundaries.
_SEPARATOR_NORMALIZER = re.compile(r"[■▮█]+")


# Match a US-style date like "February 06, 2026" or "May 21, 2024".
# SEBI master-circular PDFs always print the reference on the same line as
# the issuance date in this format, so we anchor on the date rather than
# the (highly variable) text between the header and the reference.
_DATE_PATTERN = re.compile(
    r"\b(?:January|February|March|April|May|June|"
    r"July|August|September|October|November|December)"
    r"\s+\d{1,2},\s+\d{4}\b",
    re.IGNORECASE,
)

# Reference token, anchored to a known SEBI prefix. The prefix tolerates
# optional whitespace around the "/" separator (pypdf sometimes inserts
# "SEBI/ HO/") and the body allows en/em-dashes (pypdf renders "AFD-PoD"
# as "AFD – PoD"). The body is lazy + terminates on a lookahead so it
# never swallows trailing metadata like "ISSUED ON" or the uppercase
# "<MONTH> <DAY>" date printed on the same line as the reference.
_REFERENCE_TOKEN = re.compile(
    r"(?:SEBI\s*/\s*HO\s*/\s*|HO\s*/\s*|SEBI\s*/\s*)"
    r"[A-Z0-9/\-\.\(\)\s_–—]+?"
    r"(?=\s+(?:ISSUED(?:\s+ON)?|"
    r"(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|"
    r"SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+\d{1,2})"
    r"|\n|\Z)",
    re.IGNORECASE,
)

# Validation charset for the full captured reference. Tolerates pypdf
# artefacts (internal whitespace around "/" and en/em-dashes) so the stored
# reference matches what's printed in the PDF.
_REFERENCE_CHARSET = re.compile(r"^[A-Z0-9/\-\.\(\)\s_–—]+$", re.IGNORECASE)
# Valid SEBI master-circular reference prefixes: "HO/...", "SEBI/...", "SEBI/HO/..."
# (e.g. HO/49/14/14(7)2025-CFD-POD2/I/3762/2026, SEBI/HO/MIRSD/...).
_REFERENCE_LEADER = re.compile(
    r"^(?:SEBI\s*/\s*HO\s*/\s*|HO\s*/\s*|SEBI\s*/\s*)",
    re.IGNORECASE,
)


def _looks_like_reference(candidate: str) -> bool:
    """Reject obviously-wrong captures like the Sub: body text.

    Valid SEBI master-circular references (per the samples
    HO/49/14/14(7)2025-CFD-POD2/I/3762/2026,
    SEBI/HO/MIRSD/MIRSD-PoD/P/CIR/2025/91, and
    SEBI/HO/OIAE/OIAE_IAD-1/P/CIR/2023/145) always have:
      - a recognised prefix: SEBI/HO/, HO/, or SEBI/
      - 2+ forward slashes
      - only A-Z, 0-9, /, -, ., (, ), _, plus internal whitespace and
        en/em-dashes (pypdf artefacts — kept as-is so the stored reference
        matches what's printed in the PDF)
    """
    stripped = candidate.strip()
    if len(stripped) < 5 or len(stripped) > 200:
        return False
    if not _REFERENCE_LEADER.match(stripped):
        return False
    if stripped.count("/") < 2:
        return False
    if not _REFERENCE_CHARSET.match(stripped):
        return False
    return True


def _clean(candidate: str) -> str:
    """Trim leading/trailing whitespace and trailing punctuation. The
    captured value is otherwise kept exactly as pypdf produced it (no
    internal-whitespace stripping or dash normalization), so the stored
    reference matches what's printed in the PDF."""
    cleaned = candidate.strip()
    cleaned = cleaned.rstrip(",.;:")
    return cleaned


def _normalize_separators(text: str) -> str:
    """Replace pypdf's non-whitespace line separators with newlines."""
    return _SEPARATOR_NORMALIZER.sub("\n", text)


def _extract_via_llm(text: str) -> str | None:
    """Fallback: ask an LLM to identify the master circular reference when
    regex extraction fails. Slower and more expensive than the regex, so
    this is only invoked for residual edge cases (non-standard layouts,
    OCR artefacts, exotic reference shapes). The response is re-validated
    through _looks_like_reference so a hallucinated value can't slip through.
    """
    try:
        from pydantic import BaseModel, Field

        from config import Config
        from utils.llm_providers import get_llm_provider
    except ImportError as exc:
        logger.warning("LLM fallback unavailable, missing import: %s", exc)
        return None

    class _ReferenceResponse(BaseModel):
        reference: str | None = Field(
            default=None,
            description=(
                "The SEBI master-circular reference number exactly as printed on "
                "page 1, e.g. 'SEBI/HO/MIRSD/MIRSD-PoD/P/CIR/2025/91'. Return null "
                "if no clear reference is present. Do NOT return the short numeric "
                "ID from the URL (e.g. 75220), the page number, or any other code."
            ),
        )

    prompt = (
        "Extract the SEBI master circular reference number from the following "
        "page-1 text.\n\n"
        "A SEBI master circular reference is a code like:\n"
        "  HO/49/14/14(7)2025-CFD-POD2/I/3762/2026\n"
        "  SEBI/HO/MIRSD/MIRSD-PoD/P/CIR/2025/91\n"
        "  SEBI/HO/OIAE/OIAE_IAD-1/P/CIR/2023/145\n\n"
        "It appears near the top of the page, right after the 'MASTER CIRCULAR' "
        "header (with optional subtitle / version note in between) and before "
        "the 'To,' addressees block. Return the reference exactly as printed. "
        "If the page has no clear master circular reference, return null.\n\n"
        f"Page text:\n{text}"
    )

    try:
        client = get_llm_provider(Config.LLM_PROVIDER)
        results = client.create_completions_parallel(
            prompts=[prompt],
            model=Config.ACTION_ITEM_MODEL,
            response_model=_ReferenceResponse,
            max_tokens=200,
        )
    except Exception as exc:
        logger.warning("LLM extraction call failed: %s", exc)
        return None

    response = results[0] if results else None
    if response is None or not response.reference:
        return None
    candidate = _clean(response.reference)
    # The LLM occasionally echoes back trailing metadata like "ISSUED ON"
    # or an uppercase "<MONTH> <DAY>" date printed on the same line as the
    # reference. Strip them so the validation charset accepts the value.
    candidate = re.sub(
        r"\s+(?:ISSUED(?:\s+ON)?|"
        r"(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|"
        r"SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+\d{1,2})$",
        "",
        candidate,
        flags=re.IGNORECASE,
    )
    if not _looks_like_reference(candidate):
        logger.warning("LLM-extracted reference failed validation: %r", candidate)
        return None
    return candidate


def extract_master_circular_reference(pdf_bytes: bytes) -> str | None:
    """Extract the real circular reference number from the first page of a
    SEBI Master Circular PDF. Returns None if no reference can be found or
    the PDF can't be parsed.

    Strategy:
      1. Regex pass — fast and deterministic. Covers the standard layout
         where the issuance reference is the first reference-shaped token
         on page 1 (right after the MASTER CIRCULAR header).
      2. LLM fallback — only when the regex returns nothing. The LLM's
         response is re-validated through _looks_like_reference, so a
         hallucinated value cannot slip through.
    """
    try:
        from pypdf import PdfReader
    except ImportError:
        logger.warning("pypdf is not installed; cannot extract references from PDFs")
        return None

    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        if not reader.pages:
            return None
        first_page = reader.pages[0]
        text = first_page.extract_text() or ""
    except Exception as exc:
        logger.warning("Failed to read PDF for reference extraction: %s", exc)
        return None

    if not text:
        return None

    # pypdf often returns non-whitespace line separators (e.g. U+25A0 ■)
    # instead of \n. Normalize so the regex below sees a single text stream
    # with real whitespace boundaries.
    text = _normalize_separators(text)

    # SEBI master-circular page 1 always lays out the issuance reference at
    # the top — right after the MASTER CIRCULAR header (with optional
    # subtitle / version note in between) and before the "To," addressees
    # block. The first reference token on the page that passes validation
    # is therefore the issuance reference. Body-text mentions of older
    # circulars appear further down and don't pass the leader/prefix gate.
    for ref_match in _REFERENCE_TOKEN.finditer(text):
        candidate = _clean(ref_match.group(0))
        if _looks_like_reference(candidate):
            return candidate

    # === DEBUG: only print on regex failure ===
    print("\n" + "=" * 80)
    print("[extract_master_circular_reference] REGEX FAILED — falling back to LLM")
    print("[extract_master_circular_reference] Page-1 text fed to regex:")
    print("-" * 80)
    print(text)
    print("-" * 80)
    # === END DEBUG ===

    # Step 2 — LLM fallback for residual edge cases.
    llm_candidate = _extract_via_llm(text)
    if llm_candidate:
        print(
            f"[extract_master_circular_reference] LLM fallback succeeded -> {llm_candidate!r}"
        )
        return llm_candidate

    print("[extract_master_circular_reference] LLM fallback also failed")
    print("=" * 80 + "\n")
    return None