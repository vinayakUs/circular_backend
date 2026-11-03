from __future__ import annotations

import argparse
from datetime import date
import logging
import sys
from logging.handlers import TimedRotatingFileHandler

from config import Config
from db.postgres_client import get_postgres_client
from ingestion.indexer.es_provider import get_es_client
from ingestion.scrapper.orchestrator import ScraperOrchestrator
from storage.s3_client import S3StorageClient


def _setup_logging(log_file: str | None, level: str = "INFO") -> None:
    """Mirror ingestion.processor.runner: stdout always, rotating file if --log-file given.

    Rotates at midnight each day, files named scrapper.log.2026-11-03, etc.
    backupCount=9999 effectively keeps all files forever.
    """
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Remove any pre-existing handlers (e.g. from logging.basicConfig calls at import time)
    # to avoid duplicate log lines.
    root.handlers.clear()

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    root.addHandler(stdout_handler)

    if log_file:
        file_handler = TimedRotatingFileHandler(
            log_file,
            when="midnight",
            interval=1,
            backupCount=9999,
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)


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
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Path to rotating log file (e.g. /root/circular_backend/logs/scrapper.log).",
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
    _setup_logging(args.log_file, Config.LOG_LEVEL)
    # Hardcoded module path: under `python -m ingestion.scrapper.runner`,
    # __name__ is "__main__", which makes logs unreadable to grep.
    logger = logging.getLogger("ingestion.scrapper.runner")
    selected_sources = _parse_sources(args.sources)
    from_date = _parse_date(args.from_date)
    to_date = _parse_date(args.to_date)

    logger.info(
        "run.start sources=%s from=%s to=%s log_level=%s",
        selected_sources or Config.SCRAPER_ENABLED_SOURCES,
        from_date or "checkpoint",
        to_date or "today",
        Config.LOG_LEVEL,
    )
    db_client = get_postgres_client()
    db_pool = db_client.get_pool()
    s3_client = S3StorageClient(
        bucket=Config.AWS_S3_BUCKET,
        region=Config.AWS_S3_REGION,
        aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
        endpoint_url=Config.AWS_S3_ENDPOINT_URL,
    ) if Config.AWS_S3_BUCKET else None

    orchestrator = ScraperOrchestrator(
        db_pool=db_pool,
        s3_client=s3_client,
        es_client=get_es_client(),
        default_lookback_days=Config.SCRAPER_DEFAULT_LOOKBACK_DAYS,
        enabled_sources=selected_sources,
        from_date=from_date,
        to_date=to_date,
    )
    try:
        orchestrator.run()
    except Exception:
        logger.exception("run.failed")
        db_client.close()
        return 1

    logger.info("run.done status=ok")
    db_client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
