"""
Processor pipeline runner with rotating log output.
Usage: python -m ingestion.processor.runner --limit 50 --log-file /var/log/processor.log
"""
import argparse
import logging
import sys
from logging.handlers import TimedRotatingFileHandler

# Suppress urllib3 raw HTTP debug output (PrintHandler writes directly to stdout).
# Lines like "HTTP Request: POST ... HTTP/1.1 200 OK" bypass Python logging entirely.
import urllib3
urllib3_logger = logging.getLogger("urllib3")
urllib3_logger.setLevel(logging.WARNING)
urllib3_logger.handlers.clear()
try:
    # urllib3 >= 2.0
    import http.client
    http.client.HTTPConnection.debuglevel = 0
except Exception:
    pass

from db.postgres_client import get_postgres_client
from ingestion.processor.pipeline import ProcessorPipeline
from ingestion.processor.nse_applicability_processor import NSEApplicabilityProcessor
from ingestion.processor.designation_extractor_processor import DesignationExtractorProcessor
from ingestion.processor.document_summarizer import DocumentSummarizerProcessor
# from ingestion.processor.hybrid_reference_extractor import HybridReferenceExtractor


def main():
    parser = argparse.ArgumentParser(description="Run the processing pipeline for circulars.")
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of pending circulars to process per processor."
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Path to rotating log file (e.g. /var/log/processor.log)."
    )
    args = parser.parse_args()

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Remove any pre-existing handlers (e.g. from logging.basicConfig calls at import time)
    # to avoid duplicate/duplicate log lines.
    root.handlers.clear()

    # Always write to stdout (captured by nohup redirect)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    root.addHandler(stdout_handler)

    # Optionally also write to a date-based rotating log file.
    # Rotates at midnight each day, files named processor.log.2026-06-18, etc.
    # backupCount=9999 effectively keeps all files forever.
    if args.log_file:
        file_handler = TimedRotatingFileHandler(
            args.log_file,
            when="midnight",
            interval=1,
            backupCount=9999,
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    logger = logging.getLogger(__name__)

    db_client = get_postgres_client()
    pool = db_client.get_pool()

    # Pipeline mode
    logger.info("Initializing processor pipeline...")
    try:
        pipeline = ProcessorPipeline(pool)

        # Register processors
        pipeline.register_processor(NSEApplicabilityProcessor(pool))
        pipeline.register_processor(DesignationExtractorProcessor(pool))
        pipeline.register_processor(DocumentSummarizerProcessor(pool))
        # pipeline.register_processor(HybridReferenceExtractor(pool))

        logger.info(f"Running pipeline with limit {args.limit}...")
        pipeline.run(limit_per_processor=args.limit)

        logger.info("Pipeline execution completed successfully.")

    except Exception as e:
        logger.exception("Pipeline execution failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
