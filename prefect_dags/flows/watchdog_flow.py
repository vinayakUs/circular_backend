"""Prefect flow for watchdog discovery."""
from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger
from prefect_dags.tasks.watchdog_tasks import find_incomplete_circulars


@flow(
    name="watchdog-flow",
    log_prints=True,
)
def watchdog_flow(limit: int = 100) -> list[dict[str, Any]]:
    """
    Watchdog discovery flow.

    Scans DB for circulars that need processing (index, process, or notify).
    Logs each incomplete circular with its flags. Does NOT trigger processing.

    Args:
        limit: Max number of circulars to return per run.

    Returns:
        List of incomplete circular records with per-stage flags.
    """
    logger = get_run_logger()
    logger.info("Watchdog starting")

    results = find_incomplete_circulars(limit=limit)

    if not results:
        logger.info("Watchdog found no incomplete circulars")
        return results

    logger.info(f"Watchdog found {len(results)} incomplete circulars:")
    for r in results:
        logger.info(
            f"id={r['id']} "
            f"circular_id={r['circular_id']} source={r['source']} "
            f"full_reference={r['full_reference']} "
            f"status={r['status']} "
            f"needs_index={r['needs_index']} "
            f"needs_process={r['needs_process']} "
            f"needs_notify={r['needs_notify']}"
        )

    return results