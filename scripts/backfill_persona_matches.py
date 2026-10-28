"""Backfill persona matches for existing circulars.

Recomputes `circular_persona_matches` from `config/personas.json` for every
row in `circulars`.

`circulars` is read-only here: the only columns read are `id`, `source` and
`department`, and the only writes go to `circular_persona_matches`. This
differs deliberately from scripts/reclassify.py, which reuses
`upsert_circular` and therefore rewrites each circular row — resetting its
`status` to 'DISCOVERED' and bumping `updated_at`, which knocks every fetched
circular out of the persona API, the processor queue and the notification
eligibility query.

Only `source` and `department` drive a match (see
app/services/circular_classifier.py), so circulars that match no persona get
no rows. A circular may match several personas.

Usage:
    python -m scripts.backfill_persona_matches
    python -m scripts.backfill_persona_matches --dry-run
    python -m scripts.backfill_persona_matches --show-unmatched
    python -m scripts.backfill_persona_matches --limit 100
    python -m scripts.backfill_persona_matches --batch-size 200
"""
from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.services.circular_classifier import CircularRef, classify
from app.services.personas import get_personas
from db.postgres_client import get_postgres_client
from psycopg2.extras import execute_values

DEFAULT_BATCH_SIZE = 500


def _fetch_batch(conn, last_id, batch_size):
    if last_id is None:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, circular_id, source, department, title "
            "FROM circulars ORDER BY id LIMIT %s",
            (batch_size,),
        )
    else:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, circular_id, source, department, title "
            "FROM circulars "
            "WHERE id > %s::uuid ORDER BY id LIMIT %s",
            (last_id, batch_size),
        )
    rows = cur.fetchall()
    cur.close()
    return rows


def _existing_pairs(conn, record_ids):
    cur = conn.cursor()
    cur.execute(
        "SELECT circular_id::text, persona_slug FROM circular_persona_matches "
        "WHERE circular_id = ANY(SELECT unnest(%s::text[])::uuid)",
        (record_ids,),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def _write_batch(pool, record_ids, pairs):
    with pool.acquire() as conn:
        with conn.transaction():
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM circular_persona_matches "
                "WHERE circular_id = ANY(SELECT unnest(%s::text[])::uuid)",
                (record_ids,),
            )
            if pairs:
                execute_values(
                    cur,
                    "INSERT INTO circular_persona_matches (circular_id, persona_slug) "
                    "VALUES %s ON CONFLICT DO NOTHING",
                    pairs,
                )
            cur.close()


def backfill(limit=None, batch_size=DEFAULT_BATCH_SIZE, dry_run=False, show_unmatched=False):
    personas = get_personas()
    pool = get_postgres_client().get_pool()

    with pool.acquire() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM circulars")
        total = cur.fetchone()[0]
        cur.close()
    print(f"Found {total} circulars. Personas: {', '.join(p.slug for p in personas)}")
    if dry_run:
        print("DRY RUN — no rows will be written.")

    last_id = None
    scanned = 0
    matched = 0
    unmatched = 0
    added = 0
    removed = 0
    unchanged = 0
    per_persona = Counter()
    unmatched_rows = []
    started = time.monotonic()

    while True:
        with pool.acquire() as conn:
            rows = _fetch_batch(conn, last_id, batch_size)
        if not rows:
            break

        wanted = []
        for record_id, external_id, source, department, title in rows:
            slugs = classify(
                CircularRef(source=source, department=department),
                personas,
            )
            if slugs:
                matched += 1
                for slug in slugs:
                    wanted.append((str(record_id), slug))
                    per_persona[slug] += 1
            else:
                unmatched += 1
                unmatched_rows.append((external_id, source, department, title))

        record_ids = [str(r[0]) for r in rows]

        if dry_run:
            with pool.acquire() as conn:
                current = set(_existing_pairs(conn, record_ids))
            wanted_set = set(wanted)
            added += len(wanted_set - current)
            removed += len(current - wanted_set)
            unchanged += len(wanted_set & current)
        else:
            _write_batch(pool, record_ids, wanted)

        scanned += len(rows)
        last_id = rows[-1][0]
        print(f"  ... {scanned}/{total} scanned, {len(wanted)} matches in batch")

        if limit is not None and scanned >= limit:
            break

    elapsed = time.monotonic() - started
    print()
    print(f"Scanned {scanned} circulars in {elapsed:.1f}s")
    print(f"  matched   : {matched}")
    print(f"  unmatched : {unmatched}")
    if dry_run:
        print(f"  would add : {added}")
        print(f"  would drop: {removed}")
        print(f"  unchanged : {unchanged}")
    print("  per persona:")
    for slug, count in sorted(per_persona.items(), key=lambda kv: -kv[1]):
        print(f"    {slug:<20} {count}")

    if show_unmatched:
        _print_unmatched(unmatched_rows)


def _print_unmatched(rows):
    """List the circulars no persona matches, grouped by source + department.

    A (source, department) pair covered by no persona filter is a coverage
    gap in config/personas.json, so the grouping is the actionable part.
    """
    if not rows:
        print()
        print("Unmatched circulars: none.")
        return

    print()
    print(f"Unmatched circulars ({len(rows)}) — no persona matches these:")
    grouped = Counter((source, department) for _, source, department, _ in rows)
    for (source, department), count in sorted(grouped.items()):
        print(f"  [{count:>3}] {source} / {department or '(no department)'}")
    print()
    for external_id, source, department, title in rows:
        print(f"  {source:<6} | {external_id} | {department or '(no department)'}")
        print(f"         {title}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after scanning this many circulars.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows fetched per query (default {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing.",
    )
    parser.add_argument(
        "--show-unmatched",
        action="store_true",
        help="List the circulars that match no persona.",
    )
    args = parser.parse_args()
    backfill(
        limit=args.limit,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        show_unmatched=args.show_unmatched,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
