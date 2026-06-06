"""Main Prefect flow for circular scraping."""
from __future__ import annotations

import datetime
import logging
import os
from typing import Any

from prefect import flow, get_run_logger
from prefect_dags.tasks.scrape_tasks import scrape_source


class NoPrefectFilter(logging.Filter):
    """Filter out prefect.* logs to avoid duplication."""

    def filter(self, record):
        return not record.name.startswith("prefect.")


def generate_scraper_run_name():
    """Generate flow run name with current local date and time."""
    now = datetime.datetime.now()
    return f"scraper-{now:%Y%m%d-%H%M%S}"


@flow(name="scraper-flow", log_prints=True, flow_run_name=generate_scraper_run_name)
def scraper_flow(
    sources: list[str] | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> list[dict[str, Any]]:
    """
    Simple scraper DAG - runs one task per source.

    Args:
        sources: List of source names to scrape (e.g., ["NSE", "SEBI"]).
                 If None, uses all enabled sources from config.
        from_date: Start date for scraping (YYYY-MM-DD)
        to_date: End date for scraping (YYYY-MM-DD)

    Returns:
        List of results from each source scrape
    """
    # Setup file logging
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/scraper-{datetime.datetime.now():%Y%m%d-%H%M%S}.log"
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    handler.addFilter(NoPrefectFilter())
    logging.getLogger().addHandler(handler)

    try:
        logger = get_run_logger()
        logger.info(f"Starting scraper flow sources={sources} from_date={from_date} to_date={to_date}")

        if sources is None:
            from config import Config
            sources = list(Config.SCRAPER_ENABLED_SOURCES) if Config.SCRAPER_ENABLED_SOURCES else ["NSE", "SEBI"]

        results = []
        for source_name in sources:
            result = scrape_source(source_name, from_date, to_date)
            results.append(result)

        logger.info(f"Scraper flow completed results={results}")
        return results
    finally:
        logging.getLogger().removeHandler(handler)
        handler.close()
