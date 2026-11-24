"""CLI: python -m app.services.search_engine "SEBI circulars about KYC from last month" [--json] [--trace] [--summaries]"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys


def build_engine(include_summaries: bool = False):
    """Wire the engine to the real Postgres / Elasticsearch / (optional) S3 summaries."""
    from db.postgres_client import get_postgres_client
    from ingestion.indexer.es_provider import get_es_client
    from ingestion.repository.circular_repository import CircularRepository
    from app.services.search_engine.graph import SearchEngine
    from app.services.search_engine.llm import build_models
    from app.services.search_engine.retrieval import Retrieval

    pool = get_postgres_client().get_pool()
    summary_repo = None
    if include_summaries:
        from ingestion.repository.summary_repository import SummaryRepository
        summary_repo = SummaryRepository(db_pool=pool)
    rt = Retrieval(repo=CircularRepository(db_pool=pool), es=get_es_client(), pool=pool, summary_repo=summary_repo)
    return SearchEngine(rt, build_models(), include_summaries=include_summaries)


def render(out: dict) -> str:
    r, p = out["response"], out["plan"]
    lines = [f"Understood as: {r['understood_as']}",
             f"intent={p['intent']} route={out.get('route')} mode={r['mode']} agent_steps={out.get('steps', 0)} parser={p['source']}"]
    if r.get("exact_matches"):
        lines += ["", r["exact_matches"]["label"] + ":", r["exact_matches"]["text"]]
    if r["summary"]:
        lines += ["", r["summary"]["label"] + ":", r["summary"]["text"]]
    srcs = (r["summary"] or r.get("exact_matches") or {}).get("sources", [])
    if srcs:
        lines += ["", "SOURCES"] + [f"  [{x['n']}] {x['source']} {x['circular_id']} | {x['date']} | {x['title'][:70]}" for x in srcs]
    if r["message"]:
        lines += ["", "MESSAGE: " + r["message"]]
    lines += ["", f"FACETS: {r['facets']}", f"CIRCULARS ({len(r['groups'])} cards)"]
    for g in r["groups"]:
        x = g["latest"]
        lines.append(f"  [{x['source']}] {x['circular_id']} | {x['issue_date']} | {x['title'][:70]}")
        mt = x.get("matches")
        if mt:
            lines.append(f"      phrase: {'in title, ' if mt['in_title'] else ''}{mt['count']} mention(s) in body")
        if g["similar_count"]:
            lines.append(f"      +{g['similar_count']} similar notices with the same title, showing the latest")
    if r["warnings"]:
        lines += ["", f"warnings: {r['warnings']}"]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m app.services.search_engine", description="Natural-language circular search.")
    ap.add_argument("query", nargs="+")
    ap.add_argument("--json", action="store_true", help="print the full response as JSON")
    ap.add_argument("--trace", action="store_true", help="send traces to LangSmith (needs LANGSMITH_API_KEY)")
    ap.add_argument("--summaries", action="store_true", help="attach S3 summaries to the top results")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

    if args.trace:
        if not os.getenv("LANGSMITH_API_KEY"):
            print("--trace needs LANGSMITH_API_KEY", file=sys.stderr)
            return 2
        os.environ.update(LANGSMITH_TRACING="true", LANGCHAIN_TRACING_V2="true")
        os.environ.setdefault("LANGSMITH_PROJECT", "circular-search")

    out = build_engine(args.summaries).run(" ".join(args.query), tags=["cli"])
    if args.json:
        print(json.dumps({"plan": out["plan"], "response": out["response"]}, indent=2, ensure_ascii=False, default=str))
    else:
        print(render(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
