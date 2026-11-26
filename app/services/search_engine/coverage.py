"""Include nearby sections when a search has narrowed to one circular."""
import logging

logger = logging.getLogger(__name__)


def document_passages(retrieval, circular_id, matches):
    """Keep ranked hits and add distinct leading chunks in document order."""
    passages = list(matches)
    seen = {row["text"].strip() for row in passages if row.get("text")}
    try:
        leading = retrieval.chunks_for([circular_id], per=12)
    except Exception:
        logger.warning("Could not load additional circular sections", exc_info=True)
        return passages
    for row in leading:
        text = (row.get("text") or "").strip()
        if row.get("circular_id") == circular_id and text and text not in seen:
            passages.append(row)
            seen.add(text)
    return passages
