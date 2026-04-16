import argparse
import sys
from typing import List, Optional

from pydantic import BaseModel, Field

from config import Config
from db.client import get_db_client
from ingestion.indexer.pdf_extractor import PDFTextExtractor
from ingestion.repository.circular_repository import CircularRepository
from utils.llm_client import get_llm_client


class ActionItem(BaseModel):
    """Represents a single extracted action item from a circular."""

    entity: str = Field(
        ...,
        description="The entity responsible for the action (e.g., 'Promoters of Creative Merchants Ltd', 'Trading members').",
    )
    action: str = Field(
        ...,
        description="The specific action they must take (e.g., 'must purchase shares from public shareholders', 'must submit offers').",
    )
    timeline: Optional[str] = Field(
        None,
        description="When the action must be completed or temporal context (e.g., 'on April 16, 2026', 'before trading on May 11, 2026').",
    )


class ActionItemList(BaseModel):
    """A list of extracted action items."""

    items: List[ActionItem]


def extract_action_items(circular_id: Optional[str] = None, pdf_path: Optional[str] = None) -> ActionItemList:
    """Extract action items from a given circular ID by reading its PDF.
    
    Args:
        circular_id: The ID of the circular.
        pdf_path: Direct path to a PDF file.
        
    Returns:
        ActionItemList containing the extracted items.
    """
    if pdf_path:
        file_path = pdf_path
    elif circular_id:
        db_pool = get_db_client().get_pool()
        repo = CircularRepository(db_pool)

        record = repo.get_record_by_circular_id(circular_id)
        if not record:
            raise ValueError(f"No circular found with ID: {circular_id}")

        if not record.file_path:
            # Fallback: Check if there's a primary asset
            asset = repo.get_primary_asset(record.id)
            if not asset or not asset.file_path:
                raise ValueError(f"No PDF file path found for circular: {circular_id}")
            file_path = asset.file_path
        else:
            file_path = record.file_path
    else:
        raise ValueError("Must provide either circular_id or pdf_path")

    # Extract text from the PDF
    extractor = PDFTextExtractor()
    try:
        text = extractor.extract(file_path)
    except Exception as e:
        raise RuntimeError(f"Failed to extract text from PDF at {file_path}: {e}")

    if not text.strip():
        raise ValueError("Extracted text from PDF is empty.")

    # Call LLM
    llm_client = get_llm_client()
    model = Config.ACTION_ITEM_MODEL

    prompt = f"""
    You are a regulatory compliance assistant. Extract the highest-level summarized action items, obligations, and deadlines mentioned in the following circular text.
    Do NOT extract every single minor detail. Instead, group closely related actions together into a single concise action item.
    Limit your response to a MAXIMUM of 2 or 3 extremely condensed action items. 
    Format them clearly specifying the entity responsible, the summarized action they must take, and any related timeline or deadline.

    Examples of good action items:
    - Entity: 'Clearing members'
      Action: 'must note revised cut-off times for Early Pay-in of Funds and Securities'
      Timeline: 'on April 17, 2026'
    - Entity: 'Promoters of Creative Merchants Ltd, Matra Kaushal Enterprise Ltd, and Twinstar Industries Ltd'
      Action: 'must purchase shares from public shareholders as per fair value'

    Text to extract from:
    {text}
    """

    try:
        response = llm_client.chat.completions.create(
            model=model,
            response_model=ActionItemList,
            messages=[{"role": "user", "content": prompt}],
        )
        return response
    except Exception as e:
        raise RuntimeError(f"LLM extraction failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Extract action items from a Circular PDF using LLM.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--circular_id", type=str, help="The circular ID to process (e.g. SEBI/HO/CFD/... )")
    group.add_argument("--pdf_path", type=str, help="Direct path to a PDF file to process.")
    
    args = parser.parse_args()

    title = args.circular_id if args.circular_id else args.pdf_path
    print(f"Extracting action items for: {title}")
    try:
        action_items = extract_action_items(circular_id=args.circular_id, pdf_path=args.pdf_path)
        if not action_items.items:
            print("No action items found.")
            return

        print("\n=== Extracted Action Items ===\n")
        for i, item in enumerate(action_items.items, 1):
            print(f"{i}. Entity: {item.entity}")
            print(f"   Action: {item.action}")
            if item.timeline:
                print(f"   Timeline: {item.timeline}")
            print()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

