from __future__ import annotations

import argparse
import logging

from config import Config
from db import get_db_client
from ingestion.logging_utils import configure_logging
from ingestion.scrapper.orchestrator import ScraperOrchestrator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run circular ingestion for the enabled scraper sources."
    )
    parser.add_argument(
        "--sources",
        help="Comma-separated list of source names to run, e.g. NSE or NSE.",
    )
    return parser


def _parse_sources(raw_sources: str | None) -> tuple[str, ...] | None:
    if not raw_sources:
        return None

    return tuple(
        source.strip().upper() for source in raw_sources.split(",") if source.strip()
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(Config.LOG_LEVEL)
    logger = logging.getLogger(__name__)
    selected_sources = _parse_sources(args.sources)

    logger.info(
        "Starting runner sources_override=%s env_enabled_sources=%s log_level=%s",
        selected_sources,
        Config.SCRAPER_ENABLED_SOURCES,
        Config.LOG_LEVEL,
    )
    db_client = get_db_client()
    db_pool = db_client.get_pool()

    orchestrator = ScraperOrchestrator(
        db_pool=db_pool,
        storage_path=Config.RAW_STORAGE_PATH,
        default_lookback_days=Config.SCRAPER_DEFAULT_LOOKBACK_DAYS,
        enabled_sources=selected_sources,
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
