from __future__ import annotations

import argparse
from datetime import date
import logging

from config import Config
from db.postgres_client import get_postgres_client
from ingestion.logging_utils import configure_logging
from ingestion.scrapper.orchestrator import ScraperOrchestrator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run circular ingestion for the enabled scraper sources."
    )
    parser.add_argument(
        "--sources",
        help="Comma-separated list of source names to run, e.g. NSE or SEBI.",
    )
    parser.add_argument(
        "--from-date",
        help="Start date for scraping (YYYY-MM-DD). Defaults to lookback days.",
    )
    parser.add_argument(
        "--to-date",
        help="End date for scraping (YYYY-MM-DD). Defaults to today.",
    )
    return parser


def _parse_sources(raw_sources: str | None) -> tuple[str, ...] | None:
    if not raw_sources:
        return None

    return tuple(
        source.strip().upper() for source in raw_sources.split(",") if source.strip()
    )


def _parse_date(raw_date: str | None) -> date | None:
    if not raw_date:
        return None
    return date.fromisoformat(raw_date)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(Config.LOG_LEVEL)
    logger = logging.getLogger(__name__)
    selected_sources = _parse_sources(args.sources)
    from_date = _parse_date(args.from_date)
    to_date = _parse_date(args.to_date)

    logger.info(
        "Starting runner sources_override=%s env_enabled_sources=%s log_level=%s from_date=%s to_date=%s",
        selected_sources,
        Config.SCRAPER_ENABLED_SOURCES,
        Config.LOG_LEVEL,
        from_date,
        to_date,
    )
    db_client = get_postgres_client()
    db_pool = db_client.get_pool()

    orchestrator = ScraperOrchestrator(
        db_pool=db_pool,
        storage_path=Config.RAW_STORAGE_PATH,
        default_lookback_days=Config.SCRAPER_DEFAULT_LOOKBACK_DAYS,
        enabled_sources=selected_sources,
        from_date=from_date,
        to_date=to_date,
    )
    try:
        orchestrator.run()
    except Exception:
        logger.exception("Ingestion run failed")
        db_client.close()
        return 1

    logger.info("Ingestion completed successfully")
    db_client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
