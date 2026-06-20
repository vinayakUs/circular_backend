from __future__ import annotations

from datetime import datetime, timezone
import logging
from uuid import UUID

from config import Config
from ingestion.indexer.chunker import NSEPdfChunkingStrategy
from ingestion.indexer.contextualizer import get_contextualizer
from ingestion.indexer.dto import IndexDocument
from ingestion.indexer.embedding_provider import EmbeddingProvider, NoOpEmbeddingProvider
from ingestion.indexer.es_client import ElasticsearchClient
from ingestion.indexer.pdf_extractor import PDFPlumberExtractor
from ingestion.repository import AssetRepository, CircularAssetRecord, CircularRecord, CircularRepository
from storage.s3_client import S3StorageClient


class ElasticsearchIndexer:
    """Indexes fetched circular PDFs into Elasticsearch."""

    def __init__(
        self,
        circular_repository: CircularRepository,
        es_client: ElasticsearchClient,
        asset_repository: AssetRepository | None = None,
        pdf_extractor: PDFPlumberExtractor | None = None,
        pdf_chunker: NSEPdfChunkingStrategy | None = None,
        embedding_provider: EmbeddingProvider | None = None,
        batch_size: int = 50,
        s3_client: S3StorageClient | None = None,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.circular_repository = circular_repository
        self.asset_repository = asset_repository or AssetRepository(circular_repository.db_pool)
        self.es_client = es_client
        self.pdf_extractor = pdf_extractor or PDFPlumberExtractor()
        self.pdf_chunker = pdf_chunker or NSEPdfChunkingStrategy()
        self.embedding_provider = embedding_provider or NoOpEmbeddingProvider()
        self.batch_size = batch_size
        self.s3_client = s3_client or (S3StorageClient() if Config.AWS_S3_BUCKET else None)

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
        return self._process_record(record, cleanup_stale_chunks=True)

    def _process_record(
        self, record: CircularRecord, *, cleanup_stale_chunks: bool = False
    ) -> bool:
        try:
            assets = self.asset_repository.list_assets(record.id)
            indexable_assets = self._get_indexable_assets(assets)
            if not indexable_assets:
                self.logger.warning(
                    "Skipping record without indexable PDF assets record_id=%s circular_id=%s",
                    record.id,
                    record.circular_id,
                )
                return False

            indexed_at = datetime.now(timezone.utc)
            documents: list[IndexDocument] = []
            for asset in indexable_assets:
                if not self.s3_client:
                    self.logger.warning(
                        "No s3_client configured record_id=%s asset_id=%s",
                        record.id,
                        asset.id,
                    )
                    continue
                if not self.s3_client.exists(asset.file_path):
                    self.logger.warning(
                        "S3 object not found record_id=%s asset_id=%s file_path=%s",
                        record.id,
                        asset.id,
                        asset.file_path,
                    )
                    continue
                try:
                    blocks = self.pdf_extractor.extract_blocks(asset.file_path)
                except Exception:
                    self.logger.exception(
                        "Failed to read S3 asset record_id=%s asset_id=%s file_path=%s",
                        record.id,
                        asset.id,
                        asset.file_path,
                    )
                    continue
                self.logger.info("---------------------------------------")
                self.logger.info("Extracted blocks count=%s record_id=%s asset_id=%s file_path=%s",
                    len(blocks),
                    record.id,
                    asset.id,
                    asset.file_path,
                )
                print(blocks)
                self.logger.info("---------------------------------------")

                chunks = self.pdf_chunker.chunk(
                    blocks,
                    circular_key=(
                        f"{record.source}:{record.circular_id}:"
                        f"{asset.asset_role}:{asset.archive_member_path or asset.file_path}"
                    ),
                )
                self.logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
                self.logger.info("Generated chunks count=%s record_id=%s asset_id=%s circular_key=%s chunks=%s",
                                  len(chunks),
                                  record.id,
                                  asset.id,
                                  (
                                      f"{record.source}:{record.circular_id}:"
                                      f"{asset.asset_role}:{asset.archive_member_path or asset.file_path}"
                                  ),
                                  [
                                      {
                                          "chunk_id": c.chunk_id,
                                          "chunk_index": c.chunk_index,
                                          "text_len": len(c.text),
                                          "text_preview": c.text[:200],
                                      }
                                      for c in chunks
                                  ],
                                )
                self.logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

                # Build full text for contextual retrieval (concatenate chunk texts)
                full_text_for_context = " ".join(c.text for c in chunks)

                # Generate contextual text if enabled
                chunk_texts = [chunk.text for chunk in chunks]
                chunk_contextual_texts = chunk_texts.copy()

                if Config.ES_ENABLE_CONTEXTUAL_RETRIEVAL:
                    try:
                        contextualizer = get_contextualizer()
                        contexts = contextualizer.contextualize_chunks_wrt_full_doc(
                            chunks=chunk_texts,
                            full_doc_text=full_text_for_context,
                            circular_title=record.title,
                            full_reference=record.full_reference,
                        )
                        chunk_contextual_texts = [
                            ctx.get_contextualized_text() for ctx in contexts
                        ]
                        self.logger.debug(
                            "Generated contextual text for %s chunks record_id=%s",
                            len(contexts),
                            record.id,
                        )
                    except Exception as e:
                        self.logger.warning(
                            "Failed to generate contextual text, falling back to original chunks record_id=%s error=%s",
                            record.id,
                            e,
                        )

                chunk_embeddings = self.embedding_provider.embed_texts(
                    chunk_contextual_texts
                )
                documents.extend(
                    IndexDocument(
                        chunk_id=chunk.chunk_id,
                        circular_db_id=str(record.id),
                        circular_id=record.circular_id,
                        asset_id=str(asset.id),
                        asset_role=asset.asset_role,
                        source=record.source,
                        title=record.title,
                        department=record.department,
                        issue_date=record.issue_date,
                        applicable_to_nse=record.applicable_to_nse,
                        full_reference=record.full_reference,
                        url=record.url,
                        pdf_url=record.pdf_url,
                        file_path=asset.file_path,
                        archive_member_path=asset.archive_member_path,
                        content_hash=asset.content_hash or record.content_hash,
                        chunk_index=chunk.chunk_index,
                        chunk_text=chunk.text,
                        chunk_text_contextual=chunk_contextual_texts[position]
                        if Config.ES_ENABLE_CONTEXTUAL_RETRIEVAL
                        else None,
                        embedding=chunk_embeddings[position],
                        indexed_at=indexed_at,
                    )
                    for position, chunk in enumerate(chunks)
                )

            if not documents:
                self.logger.warning(
                    "Skipping record with empty extracted text across all assets record_id=%s circular_id=%s",
                    record.id,
                    record.circular_id,
                )
                return False

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

    def _get_indexable_assets(
        self, assets: list[CircularAssetRecord]
    ) -> list[CircularAssetRecord]:
        extracted_assets = [asset for asset in assets if asset.asset_role == "extracted_pdf"]
        if extracted_assets:
            return extracted_assets
        return [asset for asset in assets if asset.asset_role == "original_pdf"]
