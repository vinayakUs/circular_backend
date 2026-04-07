'''ES Indexer Runner
run-indexer --delete-index --reset-db --reset-bloom


Create the ES index once:

run-indexer --setup-index

Index pending fetched circulars:

run-indexer

Useful variants:

run-indexer --batch-size 100
run-indexer --record-id <circular-record-uuid>
run-indexer
run-indexer --batch-size 100
run-indexer --setup-index
run-indexer --delete-index
run-indexer --delete-index --reset-db --reset-bloom
run-indexer --record-id 123e4567-e89b-12d3-a456-426614174000

'''


from __future__ import annotations

import argparse
import logging
from uuid import UUID

from config import Config
from db import get_db_client
from ingestion.indexer import ElasticsearchClient, ElasticsearchIndexer, FixedSizeChunker
from ingestion.logging_utils import configure_logging
from ingestion.repository import CircularRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Index fetched circular PDFs into Elasticsearch."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=Config.ES_INDEXER_BATCH_SIZE,
        help="Maximum number of pending circulars to process in one run.",
    )
    parser.add_argument(
        "--setup-index",
        action="store_true",
        help="Create the Elasticsearch index if it does not exist.",
    )
    parser.add_argument(
        "--delete-index",
        action="store_true",
        help="Delete the Elasticsearch index if it exists.",
    )
    parser.add_argument(
        "--reset-db",
        action="store_true",
        help="Clear all ES indexing metadata from Postgres.",
    )
    parser.add_argument(
        "--reset-bloom",
        action="store_true",
        help="Clear bloom/checkpoint state from Postgres.",
    )
    parser.add_argument(
        "--record-id",
        help="Reindex a specific circular record by UUID.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(Config.LOG_LEVEL)
    logger = logging.getLogger(__name__)

    logger.info(
        "Starting ES indexer index_name=%s batch_size=%s setup_index=%s delete_index=%s reset_db=%s reset_bloom=%s record_id=%s",
        Config.ELASTICSEARCH_INDEX_NAME,
        args.batch_size,
        args.setup_index,
        args.delete_index,
        args.reset_db,
        args.reset_bloom,
        args.record_id,
    )
    db_client = get_db_client()
    db_pool = db_client.get_pool()
    repository = CircularRepository(db_pool=db_pool)
    es_client = ElasticsearchClient(
        url=Config.ELASTICSEARCH_URL,
        index_name=Config.ELASTICSEARCH_INDEX_NAME,
        request_timeout_seconds=Config.ES_REQUEST_TIMEOUT_SECONDS,
        username=Config.ELASTICSEARCH_USERNAME,
        password=Config.ELASTICSEARCH_PASSWORD,
    )
    indexer = ElasticsearchIndexer(
        circular_repository=repository,
        es_client=es_client,
        chunker=FixedSizeChunker(
            chunk_size=Config.ES_CHUNK_SIZE,
            overlap=Config.ES_CHUNK_OVERLAP,
        ),
        batch_size=args.batch_size,
    )

    try:
        maintenance_mode = args.delete_index or args.reset_db or args.reset_bloom
        if args.delete_index:
            es_client.delete_index()
            logger.info("Elasticsearch index deletion completed")
        if args.reset_db:
            repository.clear_all_es_index_state()
            logger.info("Postgres ES metadata reset completed")
        if args.reset_bloom:
            repository.reset_bloom_state()
            logger.info("Bloom/checkpoint reset completed")
        if maintenance_mode:
            db_client.close()
            return 0
        if args.setup_index:
            es_client.setup_index()
            if not args.record_id:
                logger.info("Elasticsearch index setup completed")
                db_client.close()
                return 0
        if args.record_id:
            indexer.reindex_record(UUID(args.record_id))
        else:
            indexer.run_once()
    except Exception:
        logger.exception("Indexing run failed")
        db_client.close()
        return 1

    logger.info("Indexing completed successfully")
    db_client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
