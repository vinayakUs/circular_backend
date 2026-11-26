"""Data access for the search engine: the only module that talks to Postgres and Elasticsearch.

Notebook function          -> repo logic
db_lookup                  -> CircularRepository.get_record_by_full_reference / get_record_by_circular_id
db_filter                  -> SQL on circulars (id list) + CircularRepository.get_records_by_ids
db_search (title)          -> ElasticsearchClient._build_v2_title_query (+ collapse on circular_db_id)
db_chunk_search            -> ElasticsearchClient.search(strategy="hybrid")
db_chunks_for              -> ES terms query on circular_id ordered by chunk_index
db_phrase_search           -> ES match_phrase candidates, then regex verification on the raw text
short_summary              -> SummaryRepository.get_summary_text (S3), optional
"""
from __future__ import annotations

import logging
import math
import re
from typing import Any
from uuid import UUID

from langsmith import traceable

from app.services.search_engine.settings import CHUNK_MIN_OVERLAP
from app.services.search_engine.textutil import content_tokens, coverage, norm, tok_in

logger = logging.getLogger(__name__)

Row = dict[str, Any]


def record_to_row(rec: Any, **extra: Any) -> Row:
    return {
        "id": str(rec.id), "circular_id": rec.circular_id, "source": rec.source,
        "full_reference": rec.full_reference, "department": rec.department, "title": rec.title,
        "issue_date": rec.issue_date.isoformat() if hasattr(rec.issue_date, "isoformat") else str(rec.issue_date),
        "status": rec.status, "url": rec.url, "pdf_url": rec.pdf_url,
        "short_summary": None, "score": None, "match": None, **extra,
    }


def _metadata(issuers: Any = (), dates: dict | None = None, circular_ids: list[str] | None = None) -> dict[str, Any]:
    """Filter dict understood by ElasticsearchClient._build_filters."""
    meta: dict[str, Any] = {}
    if issuers:
        meta["source"] = list(issuers)
    if dates:
        meta["from_date"], meta["to_date"] = dates["from"], dates["to"]
    if circular_ids:
        meta["circular_id"] = list(circular_ids)
    return meta


def _phrase_tokens(p: str) -> list[str]:
    return re.findall(r"[^\W_]+", p.lower())


def phrase_regex(p: str) -> re.Pattern | None:
    """Same words in the same order; any punctuation, spacing or line break may sit between them."""
    toks = _phrase_tokens(p)
    if not toks:
        return None
    return re.compile(r"(?<![^\W_])" + r"[\W_]+".join(re.escape(t) for t in toks) + r"(?![^\W_])", re.I)


def _snippet(text: str, s: int, e: int, chunk_index: int | None, width: int) -> dict[str, Any]:
    clean = lambda t: re.sub(r"\s+", " ", t).strip()  # noqa: E731
    return {"page": None, "chunk": chunk_index,
            "before": ("…" if s - width > 0 else "") + clean(text[max(0, s - width):s]),
            "match": clean(text[s:e]),
            "after": clean(text[e:e + width]) + ("…" if e + width < len(text) else "")}


class Retrieval:
    """``repo``: CircularRepository, ``es``: ElasticsearchClient, ``pool``: Postgres pool,
    ``summary_repo``: optional SummaryRepository."""

    def __init__(self, repo: Any, es: Any, pool: Any, summary_repo: Any | None = None) -> None:
        self.repo, self.es, self.pool, self.summary_repo = repo, es, pool, summary_repo

    # ── Postgres ────────────────────────────────────────────────
    @traceable(name="db_lookup", run_type="retriever")
    def lookup(self, circular_no: str) -> list[Row]:
        rec = self.repo.get_record_by_full_reference(circular_no) or self.repo.get_record_by_circular_id(circular_no)
        if rec is None or not rec.is_active:
            return []
        return [record_to_row(rec, score=1.0, match="exact_reference")]

    @traceable(name="db_filter", run_type="retriever")
    def filter(self, issuers: Any, dates: dict | None, limit: int = 50) -> list[Row]:
        where, args = ["is_active = TRUE", "status = 'FETCHED'"], []
        if issuers:
            where.append("source = ANY(%s)")
            args.append(list(issuers))
        if dates:
            where.append("issue_date BETWEEN %s AND %s")
            args += [dates["from"], dates["to"]]
        with self.pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT id FROM circulars WHERE {' AND '.join(where)} "
                        "ORDER BY issue_date DESC, id DESC LIMIT %s", (*args, limit))
            ids = [r[0] for r in cur.fetchall()]
        rows = self._rows_by_ids(ids)
        return [{**r, "match": "filter"} for r in rows]

    def _rows_by_ids(self, ids: list[Any]) -> list[Row]:
        """Active circulars for the given ids, in the given order."""
        uuids = [i if isinstance(i, UUID) else UUID(str(i)) for i in dict.fromkeys(ids)]
        by_id = {str(r.id): r for r in self.repo.get_records_by_ids(uuids)}
        return [record_to_row(by_id[str(u)]) for u in uuids if str(u) in by_id and by_id[str(u)].is_active]

    def _rows_for_hits(self, hits: list[Any]) -> dict[str, Row]:
        rows = self._rows_by_ids([h.document.circular_db_id for h in hits])
        return {r["id"]: r for r in rows}

    def attach_summaries(self, rows: list[Row], top: int = 5) -> None:
        """Best-effort: S3 failures never block results."""
        if not self.summary_repo:
            return
        for r in rows[:top]:
            try:
                text = self.summary_repo.get_summary_text(UUID(r["id"]))
            except Exception:  # noqa: BLE001
                continue
            if text:
                r["short_summary"] = text[:600]

    # ── Elasticsearch: titles ───────────────────────────────────
    @traceable(name="db_search", run_type="retriever")
    def title_search(self, clean_query: str, issuers: Any = (), dates: dict | None = None,
                     limit: int = 10) -> tuple[list[Row], dict[str, Any]]:
        toks = content_tokens(clean_query)
        if not toks:
            return [], {"weak": True, "reason": "no content words"}
        es = self.es
        filters = es._build_filters(_metadata(issuers, dates))
        resp = es.client.search(
            index=es.index_name, query=es._build_v2_title_query(clean_query, filters),
            collapse={"field": "circular_db_id"}, size=limit * 3,
            sort=[{"_score": {"order": "desc"}}, {"issue_date": {"order": "desc"}}])
        hits = es._parse_hits(resp)
        rows_by_id = self._rows_for_hits(hits)
        q, out = norm(clean_query), []
        for h in hits:
            r = rows_by_id.get(h.document.circular_db_id)
            if not r:
                continue
            words = set((norm(r["title"]) + " " + norm(r["full_reference"]) + " " + norm(r["circular_id"])
                         + " " + norm(r["department"])).split())
            cov = coverage(toks, words)
            exact = norm(r["title"]) == q
            if cov < 0.34 and not exact:
                continue
            out.append({**r, "score": round(h.score or 0.0, 4), "coverage": cov,
                        "match": "exact_title" if exact else "title"})
        out = out[:limit]
        if not out:
            return [], {"weak": True, "reason": "no title/keyword match"}
        top = out[0]
        weak = top["coverage"] < 0.5 and top["match"] != "exact_title"
        return out, {"weak": weak, "top_coverage": round(top["coverage"], 2),
                     "reason": "low title/keyword match" if weak else "ok"}

    # ── Elasticsearch: passages ─────────────────────────────────
    @traceable(name="db_chunk_search", run_type="retriever")
    def chunk_search(self, query: str, issuers: Any = (), dates: dict | None = None,
                     circular_ids: list[str] | None = None, limit: int = 6, per_circular: int = 2) -> list[Row]:
        """Hybrid (BM25 + kNN) passages. Kept only if they share enough words with the query: the
        kNN leg always returns neighbours, so without this guard nothing would ever be 'not found'."""
        toks = content_tokens(query)
        if not toks:
            return []
        hits = self.es.search(query, _metadata(issuers, dates, circular_ids), size=limit * 4, strategy="hybrid")
        rows_by_id = self._rows_for_hits(hits)
        need = max(1, math.ceil(CHUNK_MIN_OVERLAP * len(toks)))
        out, per = [], {}
        for h in hits:
            d, r = h.document, rows_by_id.get(h.document.circular_db_id)
            if not r or per.get(d.circular_id, 0) >= per_circular:
                continue
            words = set(norm(d.chunk_text + " " + d.title).split())
            overlap = sum(1 for t in toks if tok_in(t, words))
            if overlap < need:
                continue
            per[d.circular_id] = per.get(d.circular_id, 0) + 1
            out.append({**r, "text": d.chunk_text, "chunk_index": d.chunk_index, "page": None,
                        "score": round(overlap / len(toks), 2), "match": "chunk"})
            if len(out) >= limit:
                break
        return out

    def chunks_for(self, circular_ids: list[str], per: int = 2) -> list[Row]:
        """Leading chunks of given circulars (to summarize a circular that matched by title)."""
        if not circular_ids:
            return []
        es = self.es
        resp = es.client.search(index=es.index_name,
                                query={"bool": {"filter": [{"terms": {"circular_id": list(circular_ids)}}]}},
                                sort=[{"chunk_index": {"order": "asc"}}], size=per * len(circular_ids) * 3)
        hits = es._parse_hits(resp)
        rows_by_id = self._rows_for_hits(hits)
        out, count = [], {}
        for h in hits:
            d, r = h.document, rows_by_id.get(h.document.circular_db_id)
            if not r or count.get(d.circular_id, 0) >= per:
                continue
            count[d.circular_id] = count.get(d.circular_id, 0) + 1
            out.append({**r, "text": d.chunk_text, "chunk_index": d.chunk_index, "page": None, "score": None})
        return out

    # ── Exact phrases ───────────────────────────────────────────
    @traceable(name="db_phrase_search", run_type="retriever")
    def phrase_search(self, phrases: list[str], issuers: Any = (), dates: dict | None = None,
                      circular_ref: str | None = None, topic: str = "", limit: int = 20,
                      max_snippets: int = 3, width: int = 110) -> tuple[list[Row], dict[str, Any]]:
        """All phrases must appear (title or body). ES finds candidates (stemmed, so looser than we want);
        every candidate is re-checked with a regex over the original text, so nothing shows without a real match.
        Limitation: a phrase split across two chunks is not found."""
        pats = [(p, rx) for p in phrases if (rx := phrase_regex(p))]
        if not pats:
            return [], {}
        es = self.es
        filters = es._build_filters(_metadata(issuers, dates))
        per_circ: dict[str, dict[str, Any]] = {}   # circular_db_id -> {"row", "per_phrase": {phrase: {...}}}
        cand: set[str] | None = None
        for p, rx in pats:
            found: dict[str, dict[str, Any]] = {}
            chunk_hits = es._parse_hits(es.client.search(
                index=es.index_name, query=es._build_v2_phrase_query(p, filters), size=200))
            title_hits = es._parse_hits(es.client.search(
                index=es.index_name, size=50, collapse={"field": "circular_db_id"},
                query={"bool": {"must": [{"match_phrase": {"title": {"query": p}}}], "filter": filters}}))
            for h in title_hits:
                if rx.search(h.document.title):
                    found.setdefault(h.document.circular_db_id, {"in_title": False, "count": 0, "snippets": []})["in_title"] = True
            for h in chunk_hits:
                spans = [(m.start(), m.end()) for m in rx.finditer(h.document.chunk_text)]
                if not spans:
                    continue
                f = found.setdefault(h.document.circular_db_id, {"in_title": False, "count": 0, "snippets": []})
                f["count"] += len(spans)
                for s, e in spans:
                    if len(f["snippets"]) < max_snippets:
                        f["snippets"].append(_snippet(h.document.chunk_text, s, e, h.document.chunk_index, width))
            for cid, f in found.items():
                per_circ.setdefault(cid, {"per_phrase": {}})["per_phrase"][p] = f
            cand = set(found) if cand is None else cand & set(found)
        if not cand:
            return [], {}
        rows = self._rows_by_ids(list(cand))
        if circular_ref:
            ref = circular_ref.upper()
            rows = [r for r in rows if r["full_reference"].upper() == ref or r["circular_id"].upper() == ref]
        topic_toks = content_tokens(topic)
        hits, matches = [], {}
        for r in rows:
            fs = per_circ[r["id"]]["per_phrase"]
            in_title = any(f["in_title"] for f in fs.values())
            count = sum(f["count"] for f in fs.values())
            snippets = [s for f in fs.values() for s in f["snippets"]][:max_snippets]
            title_marked = r["title"]
            if in_title:
                for p, rx in pats:
                    title_marked = rx.sub(lambda m: "«" + m.group(0) + "»", title_marked)
            cov = coverage(topic_toks, set(norm(r["title"] + " " + r["department"]).split())) if topic_toks else 0.0
            hits.append({**r, "score": count + (1 if in_title else 0),
                         "match": "phrase_title" if in_title else "phrase_body", "_cov": cov})
            matches[r["circular_id"]] = {"in_title": in_title, "count": count, "snippets": snippets,
                                         "title_marked": title_marked if in_title else None,
                                         "phrases": [p for p, _ in pats]}
        hits.sort(key=lambda h: h["issue_date"], reverse=True)  # newest first on ties
        hits.sort(key=lambda h: (not matches[h["circular_id"]]["in_title"], -h["_cov"], -matches[h["circular_id"]]["count"]))
        for h in hits:
            h.pop("_cov", None)
        return hits[:limit], matches
