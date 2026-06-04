'''
'''
import argparse
import logging
import sys

from db.postgres_client import get_postgres_client
from ingestion.processor.pipeline import ProcessorPipeline
from ingestion.processor.nse_applicability_processor import NSEApplicabilityProcessor
from ingestion.processor.designation_extractor_processor import DesignationExtractorProcessor
from ingestion.processor.hybrid_reference_extractor import HybridReferenceExtractor


def main():
    parser = argparse.ArgumentParser(description="Run the processing pipeline for circulars.")
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of pending circulars to process per processor."
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
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
        pipeline.register_processor(HybridReferenceExtractor(pool))

        logger.info(f"Running pipeline with limit {args.limit}...")
        pipeline.run(limit_per_processor=args.limit)

        logger.info("Pipeline execution completed successfully.")

    except Exception as e:
        logger.exception("Pipeline execution failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
