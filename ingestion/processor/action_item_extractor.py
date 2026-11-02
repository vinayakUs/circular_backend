"""
python -m ingestion.processor.action_item_extractor --circular_id "SEBI/HO/CFD/..."
"""
import argparse
import logging
import re
import sys
from datetime import date
from typing import Any, Literal, Optional
from pydantic import BaseModel, Field
from config import Config
from db.postgres_client import get_postgres_client
from ingestion.indexer.pdf_extractor import PDFTextExtractor
from ingestion.processor.base import BaseProcessor
from ingestion.repository.action_item_repository import ActionItemRepository
from ingestion.repository.circular_repository import CircularRepository, CircularRecord
from ingestion.repository.processor_repository import ProcessorRepository
from utils.exchange_source import ExchangeSource
from utils.llm_providers import get_llm_provider
# Cap extraction at the front matter and operative clauses. Full text overflows
# the model context window on long circulars.
MAX_PAGES = 10
# Length control. The prompt asks for TARGET words; code only retries above HARD
# so borderline cases don't trigger a needless rewrite call.
TARGET_WORDS = 25
HARD_MAX_WORDS = 30
# True  -> keep the original rule: if any item has a date, drop all undated items;
#          if none has a date, keep only one summary item.
# False -> keep undated obligations alongside dated ones.
ENFORCE_NO_MIXING = True

# --------------------------------------------------------------------------- #
# Schemas
# --------------------------------------------------------------------------- #
class ActionItemDraft(BaseModel):
    """What the LLM returns. Converted to ActionItemSingle before persisting."""
    evidence: str = Field(
        ...,
        description=(
            "Verbatim sentence or phrase (max 250 chars) copied from the circular "
            "that this item is based on. Do not paraphrase."
        ),
    )
    action_item: str = Field(
        ...,
        description=(
            f"ONE sentence, max {TARGET_WORDS} words, in the form "
            "'<Who> must <do what> by <date>.' Add the scheme/audit/period name only if "
            "needed to tell this item apart from another. No background, reasons, "
            "circular references, submission procedure or penalties. "
            "Example: 'Type-III trading members must submit the preliminary system audit "
            "report by Nov 30, 2026.'"
        ),
    )
    date_type: Literal["deadline", "effective_date", "none"] = Field(
        ...,
        description=(
            "'deadline' if someone must act by the date; 'effective_date' if a rule or "
            "event simply starts/applies from that date; 'none' if no date applies."
        ),
    )
    deadline: Optional[str] = Field(
        None,
        description=(
            "The date in YYYY-MM-DD, converted from the date stated in the text. "
            "Resolve relative dates ('within 15 days') using the circular date if given. "
            "Null if no specific date applies."
        ),
    )

class ActionItemDraftBatch(BaseModel):
    """All action items extracted from a circular."""
    # Listed first on purpose: makes the model enumerate dates before extracting,
    # which improves recall. Not persisted.
    dates_found: list[str] = Field(
        ...,
        description="Every date mentioned in the text, verbatim. Empty list if none.",
    )
    action_items: list[ActionItemDraft] = Field(
        ...,
        description="One entry per distinct obligation or deadline in the circular.",
    )

class ActionItemSingle(BaseModel):
    """Persisted shape (unchanged from the original schema)."""
    action_item: str
    deadline: Optional[str] = None

class ShortenedBatch(BaseModel):
    """Response model for the rewrite pass."""
    items: list[str] = Field(
        ...,
        description="Rewritten action items, same count and same order as the input.",
    )

# --------------------------------------------------------------------------- #
# Prompts
# --------------------------------------------------------------------------- #
EXTRACTION_PROMPT = """You are a regulatory compliance assistant.
Circular ID: {circular_id}
Circular date: {circular_date}
<circular_text>
{text}
</circular_text>
Extract the action items, obligations, and deadlines from the circular above.
Guidelines:
1. Output one action item per distinct obligation or deadline. When one obligation has
   several deliverables with different dates (e.g. an audit plan due one day, a report due
   another), output one item per deliverable.
2. Extract only obligations that require someone to act or comply. Skip informational
   statements, background, definitions, and minor procedural details.
3. Each action_item is ONE sentence, max {target_words} words: who must act, what they must
   do, and the date. Add the audit/period/scheme name only if two items would otherwise be
   indistinguishable.
   Do NOT include: reasons for the requirement, circular references, how to submit,
   penalties, or anything not needed to act.
4. Merge items that have the same actor, the same action and the same date into one sentence.
5. deadline must be YYYY-MM-DD and must match the date in the action_item. Convert dates
   stated in the text; do not invent dates. Resolve relative dates using the circular date.
6. Set date_type to 'deadline' only if someone must act by that date. Use 'effective_date'
   when something merely starts or applies from that date.
7. Every item needs an evidence quote copied verbatim from the circular.
8. If the circular contains no dates at all, output exactly one summary item with
   deadline null and date_type 'none'.
Too long (bad):
"Pursuant to the revised framework, all trading members registered as Type-III who conduct
algorithmic trading are required to ensure that they submit the preliminary audit report
for the half-yearly system audit for the period ending September 30, 2026, on or before
November 30, 2026."
Good:
"Type-III trading members must submit the preliminary half-yearly system audit report by Nov 30, 2026."
More good examples:
- action_item: "Trading in HDFC Bank's Non-Convertible Securities is suspended from Apr 17, 2026."
  date_type: "effective_date", deadline: "2026-04-17"
- action_item: "ZCZP minimum subscription for Social Stock Exchange reduced to 50%."
  date_type: "none", deadline: null
"""
SHORTEN_PROMPT = """Rewrite each action item below as ONE sentence of max {target_words} words
(who must do what, by when). Keep every fact needed to act: the actor, the action, the
scheme/period name if it distinguishes the item, and the date. Drop reasons, background,
procedure, and circular references. Do not change the meaning or the date.
Return exactly {n} items in the same order.
{items}
"""

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip().lower()

def _word_count(s: str) -> int:
    return len(s.split())

# --------------------------------------------------------------------------- #
# Processor
# --------------------------------------------------------------------------- #
class ActionItemProcessor(BaseProcessor):
    """Processor to extract and persist action items from circulars."""
    name = "action_item_extractor"  # class attribute (matches siblings)
    SKIP_SOURCES = frozenset({ExchangeSource.SEBI_MASTER})
    def __init__(self, db_pool: Any):
        super().__init__(db_pool)
        self.action_repo = ActionItemRepository(db_pool)
        self.pdf_extractor = PDFTextExtractor()
    def run(self, record: CircularRecord) -> bool:
        if record.source in self.SKIP_SOURCES:
            self.logger.info(
                "Skipping action item extraction source=%s circular_id=%s",
                record.source, record.circular_id,
            )
            # Marked COMPLETED, not left untouched: the pending query selects
            # circulars with no processing_tasks row, so skipping without
            # recording one re-selects this circular on every run.
            self.processor_repo.mark_task_completed(record.id, self.name)
            return True
        return super().run(record)
    # ------------------------------------------------------------------ #
    # Main flow
    # ------------------------------------------------------------------ #
    def process(self, record: CircularRecord) -> None:
        # Step 1: locate PDF (inherited helper, raises ValueError if missing)
        pdf_path = self._get_pdf_path(record)
        # Step 2: extract text (PDFTextExtractor handles S3, %PDF check, FileNotFound;
        # capped at MAX_PAGES, so long circulars are truncated with a warning)
        try:
            text = self.pdf_extractor.extract(pdf_path, max_pages=MAX_PAGES)
        except Exception as e:
            raise RuntimeError(f"Failed to extract text from PDF at {pdf_path}: {e}") from e
        if not text.strip():
            raise ValueError("Extracted text from PDF is empty.")
        # Step 3: call LLM
        llm_client = get_llm_provider(Config.LLM_PROVIDER)
        model = Config.ACTION_ITEM_MODEL
        circular_date = getattr(record, "circular_date", None) or "unknown"
        prompt = EXTRACTION_PROMPT.format(
            circular_id=record.circular_id,
            circular_date=circular_date,
            text=text,
            target_words=TARGET_WORDS,
        )
        try:
            response = llm_client.create_completions_parallel(
                prompts=[prompt],
                model=model,
                response_model=ActionItemDraftBatch,
            )[0]
        except Exception as e:
            raise RuntimeError(f"LLM extraction failed: {e}") from e
        # Step 4: validate (dates, grounding)
        drafts = self._validate(response.action_items, text)
        # Step 5: enforce brevity (one cheap rewrite pass for over-long items)
        drafts = self._shorten_long_items(llm_client, model, drafts)
        # Step 6: convert to the persisted shape and apply the dated/undated rule
        items = [ActionItemSingle(action_item=d.action_item.strip(), deadline=d.deadline) for d in drafts]
        items = self._apply_dating_rule(items)
        # Step 7: persist (idempotent: delete-then-insert)
        self.action_repo.delete_action_items_for_circular(record.id)
        self.action_repo.insert_action_items(record.id, items)
        self.logger.info("Inserted %d action items for circular_id=%s", len(items), record.id)
    # ------------------------------------------------------------------ #
    # Validation
    # ------------------------------------------------------------------ #
    def _validate(self, drafts: list[ActionItemDraft], text: str) -> list[ActionItemDraft]:
        norm_text = _norm(text)
        kept: list[ActionItemDraft] = []
        for d in drafts:
            if not d.action_item.strip():
                continue
            # Grounding: log only. PDF text normalisation can break exact matches,
            # so dropping on mismatch would lose real items.
            if _norm(d.evidence) not in norm_text:
                self.logger.warning("Evidence not found verbatim; item=%r", d.action_item)
            # Date sanity: must parse as a real date
            if d.deadline:
                try:
                    date.fromisoformat(d.deadline)
                except ValueError:
                    self.logger.warning("Invalid deadline %r; setting null", d.deadline)
                    d.deadline = None
            # An effective date that is not an action deadline is still stored in the
            # deadline column (so it shows up on the calendar); adjust here if you
            # want to keep only date_type == 'deadline'.
            kept.append(d)
        return kept
    # ------------------------------------------------------------------ #
    # Brevity enforcement
    # ------------------------------------------------------------------ #
    def _shorten_long_items(
        self, llm_client: Any, model: str, drafts: list[ActionItemDraft]
    ) -> list[ActionItemDraft]:
        long_idx = [i for i, d in enumerate(drafts) if _word_count(d.action_item) > HARD_MAX_WORDS]
        if not long_idx:
            return drafts
        self.logger.info("Shortening %d over-long action items", len(long_idx))
        listing = "\n".join(
            f"{n + 1}. {drafts[i].action_item} (deadline: {drafts[i].deadline})"
            for n, i in enumerate(long_idx)
        )
        prompt = SHORTEN_PROMPT.format(
            target_words=TARGET_WORDS, n=len(long_idx), items=listing
        )
        try:
            result = llm_client.create_completions_parallel(
                prompts=[prompt],
                model=model,
                response_model=ShortenedBatch,
            )[0]
        except Exception as e:
            # Non-fatal: keep the long versions rather than failing the circular.
            self.logger.warning("Shortening pass failed, keeping originals: %s", e)
            return drafts
        if len(result.items) != len(long_idx):
            self.logger.warning(
                "Shortening returned %d items, expected %d; keeping originals",
                len(result.items), len(long_idx),
            )
            return drafts
        for i, short in zip(long_idx, result.items):
            short = short.strip()
            # Accept only if it actually got shorter and is non-empty
            if short and _word_count(short) < _word_count(drafts[i].action_item):
                drafts[i].action_item = short
        return drafts
    # ------------------------------------------------------------------ #
    # Dated / undated rule
    # ------------------------------------------------------------------ #
    @staticmethod
    def _apply_dating_rule(items: list[ActionItemSingle]) -> list[ActionItemSingle]:
        if not ENFORCE_NO_MIXING:
            return items
        # Dated rows win outright; undated rows survive only when the circular has
        # no dates at all (max 1).
        dated = [i for i in items if i.deadline]
        return dated if dated else items[:1]

def main() -> None:
    parser = argparse.ArgumentParser(description="Extract action items from a Circular using LLM.")
    parser.add_argument("--circular_id", type=str, help="Process a specific circular by ID")
    parser.add_argument("--limit", type=int, default=100, help="Max pending circulars to process")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    db_client = get_postgres_client()
    pool = db_client.get_pool()
    processor = ActionItemProcessor(pool)
    if args.circular_id:
        logger.info("Extracting action items for: %s", args.circular_id)
        repo = CircularRepository(pool)
        record = repo.get_record_by_circular_id(args.circular_id)
        if not record:
            logger.error("No circular found with ID: %s", args.circular_id)
            sys.exit(1)
        success = processor.run(record)
        sys.exit(0 if success else 1)
    else:
        processor_repo = ProcessorRepository(pool)
        pending = processor_repo.get_pending_circulars_for_processor(processor.name, limit=args.limit)
        logger.info("Found %d pending circulars for '%s'", len(pending), processor.name)
        for record in pending:
            logger.info("Processing: %s", record.circular_id)
            processor.run(record)
        logger.info("Completed %d circulars.", len(pending))

if __name__ == "__main__":
    main()