"""Run the circular_backend API server.

Usage:
  python run.py
  python run.py --log-file /root/circular_backend/logs/app.log
"""
from __future__ import annotations

import argparse
import logging
import sys
from logging.handlers import TimedRotatingFileHandler

from config import Config
from app import create_app


def _setup_logging(log_file: str | None) -> None:
    """Mirror ingestion.processor.runner: console always, rotating file if --log-file given.

    Rotates at midnight each day, files named app.log.2026-11-02, etc.
    backupCount=9999 effectively keeps all files forever.
    """
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    root = logging.getLogger()
    root.setLevel(getattr(logging, Config.LOG_LEVEL, logging.INFO))
    root.handlers.clear()

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    if log_file:
        file_handler = TimedRotatingFileHandler(
            log_file,
            when="midnight",
            interval=1,
            backupCount=9999,
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the circular_backend API server.")
    parser.add_argument(
        "--log-file",
        default=None,
        help="Path to rotating log file (e.g. /root/circular_backend/logs/app.log).",
    )
    args = parser.parse_args()
    # Configure logging before create_app() so the numba JIT warmup logs are captured.
    _setup_logging(args.log_file)
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
