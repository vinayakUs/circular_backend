'''
python -m ingestion.scrapper.runner --sources SEBI --from-date 2026-01-01 --to-date 2026-03-31    
'''
import argparse
import logging
import sys

from db.client import get_db_client
from ingestion.processor.pipeline import ProcessorPipeline
from ingestion.processor.action_item_extractor import ActionItemProcessor
from ingestion.processor.llm_reference_extractor import LLMCircularReferenceExtractor
from ingestion.processor.document_summarizer import DocumentSummarizerProcessor


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

    db_client = get_db_client()
    pool = db_client.get_pool()

    # Pipeline mode
    logger.info("Initializing processor pipeline...")
    try:
        pipeline = ProcessorPipeline(pool)

        # Register processors
        pipeline.register_processor(ActionItemProcessor(pool))
        pipeline.register_processor(LLMCircularReferenceExtractor(pool))
        pipeline.register_processor(DocumentSummarizerProcessor(pool))

        logger.info(f"Running pipeline with limit {args.limit}...")
        pipeline.run(limit_per_processor=args.limit)

        logger.info("Pipeline execution completed successfully.")

    except Exception as e:
        logger.exception("Pipeline execution failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
