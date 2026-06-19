"""Prefect tasks for watchdog discovery."""
from __future__ import annotations

from typing import Any

from prefect import task, get_run_logger
from db.postgres_client import get_postgres_client

REGISTERED_PROCESSORS = ["nse_applicability_processor", "designation_extractor_processor"]


@task(name="find_incomplete_circulars", log_prints=True)
def find_incomplete_circulars(limit: int = 100) -> list[dict[str, Any]]:
    """
    Query DB for circulars that need processing.

    Returns list of dicts with circular info and per-stage flags:
    - needs_index: status=FETCHED and es_indexed_at IS NULL
    - needs_process: status=FETCHED and not all registered processors COMPLETED
    - needs_notify: status=FETCHED, detected_at< 24h ago, not notified
    """
    logger = get_run_logger()
    db_client = get_postgres_client()
    db_pool = db_client.get_pool()

    with db_pool.acquire() as conn:
        cursor = conn.cursor()
        processor_count = len(REGISTERED_PROCESSORS)

        placeholders = ",".join(["%s"] * processor_count)
        cursor.execute(f"""
            WITH processor_status AS (
                SELECT
                    pt.circular_id,
                    COUNT(*) FILTER (WHERE pt.status = 'COMPLETED') as completed_count
                FROM processing_tasks pt
                WHERE pt.processor_name IN ({placeholders})
                GROUP BY pt.circular_id
            ),
            notified AS (
                SELECT distinct (variables->>'circular_uuid')::uuid AS circular_uuid
                FROM notification_logs
                WHERE template_name = 'circular_notification.html'
                  AND status = 'SENT'
                  AND variables->>'circular_uuid' IS NOT NULL
            )
            SELECT
                c.id,
                c.circular_id,
                c.full_reference,
                c.source,
                c.status,
                c.detected_at,
                CASE
                    WHEN c.status = 'FETCHED' AND c.es_indexed_at IS NULL THEN TRUE
                    ELSE FALSE
                END AS needs_index,
                CASE
                    WHEN c.status = 'FETCHED'
                     AND (ps.completed_count IS NULL OR ps.completed_count < %s)
                    THEN TRUE
                    ELSE FALSE
                END AS needs_process,
                CASE
                    WHEN c.status = 'FETCHED'
                     AND c.detected_at >= NOW() - INTERVAL '24 hours'
                     AND n.circular_uuid IS NULL
                    THEN TRUE
                    ELSE FALSE
                END AS needs_notify
            FROM circulars c
            LEFT JOIN processor_status ps ON c.id = ps.circular_id
            LEFT JOIN notified n ON c.id = n.circular_uuid
            WHERE
                (c.status = 'FETCHED' AND c.es_indexed_at IS NULL)
                OR (c.status = 'FETCHED' AND (ps.completed_count IS NULL OR ps.completed_count < %s))
                OR (c.status = 'FETCHED' AND c.detected_at >= NOW() - INTERVAL '24 hours' AND n.circular_uuid IS NULL)
            ORDER BY c.detected_at ASC
            LIMIT %s
        """, (*REGISTERED_PROCESSORS, processor_count, processor_count, limit))

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

    results = []
    for row in rows:
        record = dict(zip(columns, row))
        results.append({
            "id": str(record["id"]),
            "circular_id": record["circular_id"],
            "full_reference": record["full_reference"],
            "source": record["source"],
            "status": record["status"],
            "detected_at": record["detected_at"].isoformat() if record["detected_at"] else None,
            "needs_index": record["needs_index"],
            "needs_process": record["needs_process"],
            "needs_notify": record["needs_notify"],
        })

    logger.info(f"Found {len(results)} incomplete circulars")
    return results