from typing import Any

from ingestion.indexer.dto import SearchHit
from utils import multi_snippet, highlight


def search_hit_to_dict(hit: SearchHit, query: str) -> dict[str, Any]:
    doc = hit.document
    return {
        "id": doc.circular_db_id,
        "score": hit.score,
        "chunkId": doc.chunk_id,
        "circularId": doc.circular_id,
        "fullReference": doc.full_reference,
        "department": doc.department,
        "source": doc.source,
        "title": doc.title,
        "issueDate": doc.issue_date.isoformat(),
        "url": doc.url,
        "chunkIndex": doc.chunk_index,
        "preview": _build_preview(doc.chunk_text, query),
    }


def _build_preview(chunk_text: str, query: str) -> str:
    if not chunk_text:
        return ""
    snippets = multi_snippet(chunk_text, query)
    if snippets:
        highlighted = [highlight(s, query) for s in snippets]
        return f"<div class='preview'>{' ... '.join(highlighted)}</div>"
    fallback = chunk_text[:200] + ("..." if len(chunk_text) > 200 else "")
    return f"<div class='preview'>{highlight(fallback, query)}</div>"