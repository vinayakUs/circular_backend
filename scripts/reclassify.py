"""Reclassify: repopulate circular_persona_matches for all existing circulars.

Reads every row from `circulars` and calls `upsert_circular` on it.
`upsert_circular` (recently extended) does a wipe + re-insert of
persona matches in a single transaction, so this script is the
canonical way to re-derive matches after:

  - editing config/personas.json
  - fixing data issues
  - adding a new classifier feature

Idempotent: re-running produces the same matches.

Usage:
    python -m scripts.reclassify              # all circulars
    python -m scripts.reclassify --limit 100  # first 100 only (smoke test)
"""
from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

# Make repo root importable when run as a script
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from db.postgres_client import get_postgres_client
from ingestion.repository.circular_repository import CircularRepository


# A duck-typed shape matching what upsert_circular expects.
# It's a SimpleNamespace so upsert_circular's getattr(...) calls work.
def _to_circular(row: tuple) -> SimpleNamespace:
    (
        # SELECT order matches the cursor below
        _id,                   # uuid (DB id) — not used by upsert but we read it
        circular_id,           # external id (e.g. "SEBI/HO/CFD/...")
        source,
        source_item_key,
        full_reference,
        department,
        title,
        issue_date,
        url,
        pdf_url,
        content_hash,
        status,                # not used
        detected_at,
        applicable_to_nse,
        is_active,
    ) = row
    return SimpleNamespace(
        circular_id=circular_id,
        source=source,
        source_item_key=source_item_key,
        full_reference=full_reference or "",
        department=department or "",
        title=title or "",
        issue_date=issue_date,
        url=url or "",
        pdf_url=pdf_url or "",
        content_hash=content_hash,
        applicable_to_nse=bool(applicable_to_nse) if applicable_to_nse is not None else False,
        is_active=bool(is_active) if is_active is not None else True,
        detected_at=detected_at,
    )


def reclassify(limit: int | None = None, batch_size: int = 500) -> None:
    pool = get_postgres_client().get_pool()
    repo = CircularRepository(pool)

    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM circulars")
        total = cur.fetchone()[0]
    print(f"Found {total} circulars in DB. Reclassifying...")

    offset = 0
    processed = 0
    errors = 0
    started = time.monotonic()

    while True:
        with pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, circular_id, source, source_item_key, full_reference,
                       department, title, issue_date, url, pdf_url,
                       content_hash, status, detected_at,
                       applicable_to_nse, is_active
                FROM   circulars
                ORDER  BY id
                LIMIT  %s OFFSET %s
                """,
                (batch_size, offset),
            )
            rows = cur.fetchall()

        if not rows:
            break

        for row in rows:
            try:
                circular = _to_circular(row)
                repo.upsert_circular(circular)
                processed += 1
                if processed % 100 == 0:
                    elapsed = time.monotonic() - started
                    rate = processed / elapsed if elapsed > 0 else 0
                    print(f"  ... {processed}/{total} ({rate:.0f}/sec)")
            except Exception as exc:
                errors += 1
                print(f"  ERROR on {row[1]}: {exc}")

        offset += batch_size
        if limit is not None and processed >= limit:
            break

    elapsed = time.monotonic() - started
    rate = processed / elapsed if elapsed > 0 else 0
    print()
    print(f"Done. {processed} circulars reclassified, {errors} errors, {elapsed:.1f}s ({rate:.0f}/sec)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after reclassifying this many circulars (for smoke-testing).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Rows fetched per query (default 500).",
    )
    args = parser.parse_args()
    reclassify(limit=args.limit, batch_size=args.batch_size)
    return 0


if __name__ == "__main__":
    sys.exit(main())