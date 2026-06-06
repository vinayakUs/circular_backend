"""Prefect tasks for circular scraping."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

from prefect import task, get_run_logger
from config import Config
from db.postgres_client import get_postgres_client
from ingestion.scrapper.orchestrator import ScraperOrchestrator
from storage.s3_client import S3StorageClient

_HANDLER_ADDED = False


def _setup_scraper_logging(prefect_logger: logging.Logger) -> None:
    """Connect all scraper module loggers to Prefect."""
    global _HANDLER_ADDED

    if _HANDLER_ADDED:
        return

    class PrefectForwardHandler(logging.Handler):
        def emit(self, record):
            prefect_logger.log(record.levelno, f"[{record.name}] {record.getMessage()}")

    handler = PrefectForwardHandler()
    handler.setLevel(logging.INFO)
    scraper_logger = logging.getLogger("ingestion")
    scraper_logger.addHandler(handler)
    scraper_logger.setLevel(logging.INFO)
    _HANDLER_ADDED = True


@task(name="scrape_source", retries=2, retry_delay_seconds=60, log_prints=True)
def scrape_source(
    source_name: str,
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict[str, Any]:
    """
    Scrape circulars from a specific source.

    Args:
        source_name: Source name (NSE, SEBI)
        from_date: Start date for scraping (YYYY-MM-DD). Defaults to lookback.
        to_date: End date for scraping (YYYY-MM-DD). Defaults to today.

    Returns:
        Dictionary with scrape statistics
    """
    logger = get_run_logger()
    _setup_scraper_logging(logger)

    logger.info(f"Starting scrape for source={source_name}")

    db_client = get_postgres_client()
    db_pool = db_client.get_pool()

    s3_client = S3StorageClient(
        bucket=Config.AWS_S3_BUCKET,
        region=Config.AWS_S3_REGION,
        aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
        endpoint_url=Config.AWS_S3_ENDPOINT_URL,
    ) if Config.AWS_S3_BUCKET else None

    parsed_from_date = date.fromisoformat(from_date) if from_date else None
    parsed_to_date = date.fromisoformat(to_date) if to_date else None

    orchestrator = ScraperOrchestrator(
        db_pool=db_pool,
        s3_client=s3_client,
        default_lookback_days=Config.SCRAPER_DEFAULT_LOOKBACK_DAYS,
        enabled_sources=(source_name,),
        from_date=parsed_from_date,
        to_date=parsed_to_date or date.today(),
    )

    orchestrator.run()
    logger.info(f"Completed scrape for source={source_name}")

    return {
        "source": source_name,
        "from_date": from_date,
        "to_date": to_date or str(date.today()),
        "status": "completed",
    }
