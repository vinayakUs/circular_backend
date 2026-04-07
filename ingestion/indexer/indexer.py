from __future__ import annotations

from datetime import datetime, timezone
import logging
from pathlib import Path
from uuid import UUID

from ingestion.indexer.chunker import FixedSizeChunker
from ingestion.indexer.dto import IndexDocument
from ingestion.indexer.es_client import ElasticsearchClient
from ingestion.indexer.pdf_extractor import PDFTextExtractor
from ingestion.repository import CircularRecord, CircularRepository


class ElasticsearchIndexer:
    """Indexes fetched circular PDFs into Elasticsearch."""

    def __init__(
        self,
        circular_repository: CircularRepository,
        es_client: ElasticsearchClient,
        pdf_extractor: PDFTextExtractor | None = None,
        chunker: FixedSizeChunker | None = None,
        batch_size: int = 50,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.circular_repository = circular_repository
        self.es_client = es_client
        self.pdf_extractor = pdf_extractor or PDFTextExtractor()
        self.chunker = chunker or FixedSizeChunker()
        self.batch_size = batch_size

    def run_once(self) -> tuple[int, int]:
        pending_records = self.circular_repository.list_pending_es_records(
            limit=self.batch_size
        )
        processed = 0
        failed = 0
        self.logger.info(
            "Starting ES indexing batch pending_count=%s batch_size=%s index_name=%s",
            len(pending_records),
            self.batch_size,
            self.es_client.index_name,
        )
        for record in pending_records:
            if self._process_record(record):
                processed += 1
            else:
                failed += 1
        self.logger.info(
            "Completed ES indexing batch processed=%s failed=%s",
            processed,
            failed,
        )
        return processed, failed

    def reindex_record(self, record_id: UUID) -> bool:
        record = self.circular_repository.get_record_by_id(record_id)
        if record is None:
            raise ValueError(f"Circular record not found: {record_id}")
        if not record.file_path:
            raise ValueError(f"Circular record has no file_path: {record_id}")
        return self._process_record(record, cleanup_stale_chunks=True)

    def _process_record(
        self, record: CircularRecord, *, cleanup_stale_chunks: bool = False
    ) -> bool:
        if not record.file_path:
            self.logger.warning(
                "Skipping record without file_path record_id=%s circular_id=%s",
                record.id,
                record.circular_id,
            )
            return False

        pdf_path = Path(record.file_path)
        if not pdf_path.exists():
            self.logger.warning(
                "Skipping record with missing file record_id=%s file_path=%s",
                record.id,
                record.file_path,
            )
            return False

        try:
            extracted_text = self.pdf_extractor.extract(pdf_path)
            chunks = self.chunker.chunk(
                extracted_text,
                circular_key=f"{record.source}:{record.circular_id}",
            )
            if not chunks:
                self.logger.warning(
                    "Skipping record with empty extracted text record_id=%s file_path=%s",
                    record.id,
                    record.file_path,
                )
                return False

            indexed_at = datetime.now(timezone.utc)
            documents = [
                IndexDocument(
                    chunk_id=chunk.chunk_id,
                    circular_db_id=str(record.id),
                    circular_id=record.circular_id,
                    source=record.source,
                    title=record.title,
                    department=record.department,
                    issue_date=record.issue_date,
                    effective_date=record.effective_date,
                    full_reference=record.full_reference,
                    url=record.url,
                    pdf_url=record.pdf_url,
                    file_path=record.file_path,
                    content_hash=record.content_hash,
                    chunk_index=chunk.chunk_index,
                    chunk_text=chunk.text,
                    indexed_at=indexed_at,
                )
                for chunk in chunks
            ]
            success_count, failed_count = self.es_client.bulk_index(documents)
            if failed_count or success_count != len(documents):
                self.logger.error(
                    "Failed bulk indexing record_id=%s success_count=%s failed_count=%s expected=%s",
                    record.id,
                    success_count,
                    failed_count,
                    len(documents),
                )
                return False

            if cleanup_stale_chunks:
                self.es_client.delete_stale_documents_for_record(
                    str(record.id),
                    [document.chunk_id for document in documents],
                )
                self.circular_repository.clear_es_index_state(record.id)

            self.circular_repository.mark_es_indexed(
                record.id,
                chunk_count=len(documents),
                index_name=self.es_client.index_name,
            )
            self.logger.info(
                "Indexed circular record_id=%s chunk_count=%s index_name=%s",
                record.id,
                len(documents),
                self.es_client.index_name,
            )
            return True
        except Exception:
            self.logger.exception(
                "Failed ES indexing record_id=%s circular_id=%s",
                record.id,
                record.circular_id,
            )
            return False
