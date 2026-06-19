"""Prefect tasks for circular indexing and notification."""
from __future__ import annotations

import logging
from typing import Any

from prefect import task, get_run_logger
from uuid import UUID

from config import Config
from db.postgres_client import get_postgres_client
from ingestion.indexer import ElasticsearchIndexer, ElasticsearchClient, build_embedding_provider
from ingestion.repository import CircularRepository
from services.notification_service import EmailService

_HANDLER_ADDED = False


def _setup_indexer_logging(prefect_logger: logging.Logger) -> None:
    """Connect all loggers to Prefect, excluding prefect's own loggers."""
    global _HANDLER_ADDED

    if _HANDLER_ADDED:
        return

    class PrefectForwardHandler(logging.Handler):
        def emit(self, record):
            # Skip prefect's own loggers to avoid feedback loop
            if record.name.startswith("prefect."):
                return
            prefect_logger.log(record.levelno, f"[{record.name}] {record.getMessage()}")

    handler = PrefectForwardHandler()
    handler.setLevel(logging.INFO)
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    _HANDLER_ADDED = True


@task(name="index_circular", log_prints=True, retries=1, retry_delay_seconds=60)
def index_circular(circular_id: str) -> dict[str, Any]:
    logger = get_run_logger()
    _setup_indexer_logging(logger)

    db_pool = get_postgres_client().get_pool()
    repository = CircularRepository(db_pool=db_pool)
    # import time

    # time.sleep(5)  # pause for 5 seconds
    # return {"circular_id": circular_id, "status": "indexed"}

    embedding_provider = build_embedding_provider(
        Config.ES_EMBEDDING_PROVIDER,
        enabled=Config.ES_ENABLE_VECTORS,
        model_name=Config.ES_EMBEDDING_MODEL_NAME,
        query_instruction=Config.ES_QUERY_EMBEDDING_INSTRUCTION,
    )
    es_client = ElasticsearchClient(
        url=Config.ELASTICSEARCH_URL,
        index_name=Config.ELASTICSEARCH_INDEX_NAME,
        request_timeout_seconds=Config.ES_REQUEST_TIMEOUT_SECONDS,
        username=Config.ELASTICSEARCH_USERNAME,
        password=Config.ELASTICSEARCH_PASSWORD,
        embedding_provider=embedding_provider,
    )
    indexer = ElasticsearchIndexer(
        circular_repository=repository,
        es_client=es_client,
        embedding_provider=embedding_provider,
    )

    try:
        success = indexer.reindex_record(UUID(circular_id))
        if success:
            logger.info(f"Indexed circular_id={circular_id}")
            return {"circular_id": circular_id, "status": "indexed"}
        else:
            logger.warning(f"Indexing failed for circular_id={circular_id}")
            return {"circular_id": circular_id, "status": "failed"}
    except Exception as e:
        logger.exception(f"Indexing error for circular_id={circular_id}")
        raise


@task(name="notify_circular", log_prints=True, retries=1, retry_delay_seconds=60)
def notify_circular(circular_id: str) -> dict[str, Any]:
    logger = get_run_logger()
    db_pool = get_postgres_client().get_pool()
    repo = CircularRepository(db_pool=db_pool)
    record = repo.get_record_by_id(UUID(circular_id))
    if not record:
        return {"circular_id": circular_id, "status": "error", "reason": "not_found"}

    circular_data = {
        "circular_id": record.circular_id,
        "title": record.title,
        "department": record.department,
        "issue_date": record.issue_date.isoformat() if record.issue_date else None,
        "source": record.source,
        "url": record.url,
    }

    email_svc = EmailService()
    success, error = email_svc.send_notification_for_circular_to_all_bcc(circular_id, circular_data)
    if success:
        logger.info(f"Notified circular_id={circular_id}")
        return {"circular_id": circular_id, "status": "notified"}
    else:
        logger.warning(f"Notification failed for circular_id={circular_id} error={error}")
        return {"circular_id": circular_id, "status": "failed", "error": error}


@task(name="run_nse_processor", log_prints=True, retries=1, retry_delay_seconds=60)
def run_nse_processor(circular_id: str,index_result: Any) -> dict[str, Any]:
    from ingestion.processor.nse_applicability_processor import NSEApplicabilityProcessor

    logger = get_run_logger()
    _setup_indexer_logging(logger)
    db_pool = get_postgres_client().get_pool()

    record = CircularRepository(db_pool).get_record_by_id(UUID(circular_id))
    logger.info(f"Running designation processor for circular_id={circular_id} with index_result={index_result}")

    if not record:
        return {"processor": "nse_applicability_processor", "status": "error", "reason": "not_found"}

    try:
        processor = NSEApplicabilityProcessor(db_pool)
        processor.run(record)
        return {"processor": processor.name, "status": "completed"}
    except Exception as e:
        logger.exception(f"NSE processor failed for circular_id={circular_id}")
        return {"processor": "nse_applicability_processor", "status": "failed", "error": str(e)}


@task(name="run_designation_processor", log_prints=True, retries=1, retry_delay_seconds=60)
def run_designation_processor(circular_id: str,index_result: Any) -> dict[str, Any]:
    from ingestion.processor.designation_extractor_processor import DesignationExtractorProcessor

    logger = get_run_logger()
    _setup_indexer_logging(logger)
    db_pool = get_postgres_client().get_pool()

    record = CircularRepository(db_pool).get_record_by_id(UUID(circular_id))

    logger.info(f"Running designation processor for circular_id={circular_id} with index_result={index_result}")
    if not record:
        return {"processor": "designation_extractor_processor", "status": "error", "reason": "not_found"}

    try:
        processor = DesignationExtractorProcessor(db_pool)
        processor.run(record)
        return {"processor": processor.name, "status": "completed"}
    except Exception as e:
        logger.exception(f"Designation processor failed for circular_id={circular_id}")
        return {"processor": "designation_extractor_processor", "status": "failed", "error": str(e)}


@task(name="process_circular", log_prints=True, retries=1, retry_delay_seconds=60)
def process_circular(circular_id: str) -> dict[str, Any]:
    logger = get_run_logger()
    _setup_indexer_logging(logger)

    nse_future = run_nse_processor.submit(circular_id)
    designation_future = run_designation_processor.submit(circular_id)

    nse_result = nse_future.result()
    designation_result = designation_future.result()

    results = [nse_result, designation_result]
    for r in results:
        logger.info(f"Processor {r['processor']} status={r['status']} for circular_id={circular_id}")

    return {"circular_id": circular_id, "processors": results}


@task
def noop(stage: str) -> dict:
    return {"stage": stage, "skipped": True}