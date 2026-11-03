from datetime import date, datetime, timezone
from uuid import uuid4

from config import Config
from ingestion.indexer.dto import TextChunk
from ingestion.indexer.embedding_provider import NoOpEmbeddingProvider
from ingestion.indexer.indexer import ElasticsearchIndexer
from ingestion.indexer.pdf_extractor import Block
from ingestion.repository import CircularAssetRecord, CircularRecord


def _make_record() -> CircularRecord:
    now = datetime.now(timezone.utc)
    return CircularRecord(
        id=uuid4(),
        source="NSE",
        circular_id="NSE/COMP/2026/123",
        source_item_key="key",
        full_reference="NSE/COMP/2026/123",
        department="COMP",
        title="Half-yearly system audit",
        issue_date=date(2026, 1, 1),
        effective_date=None,
        url="https://example.com/circular",
        pdf_url="https://example.com/circular.pdf",
        content_hash=None,
        status="ACTIVE",
        error_message=None,
        detected_at=now,
        created_at=now,
        updated_at=now,
    )


def _make_asset(circular_id) -> CircularAssetRecord:
    now = datetime.now(timezone.utc)
    return CircularAssetRecord(
        id=uuid4(),
        circular_id=circular_id,
        asset_role="extracted_pdf",
        file_path="s3://bucket/doc.pdf",
        content_hash=None,
        mime_type="application/pdf",
        archive_member_path=None,
        file_size_bytes=100,
        created_at=now,
        updated_at=now,
    )


class FakeCircularRepository:
    def __init__(self, record):
        self.record = record
        self.marked = []
        self.cleared = []

    def list_pending_es_records(self, limit):
        return [self.record]

    def get_record_by_id(self, record_id):
        return self.record if record_id == self.record.id else None

    def mark_es_indexed(self, record_id, chunk_count, index_name):
        self.marked.append((record_id, chunk_count, index_name))

    def clear_es_index_state(self, record_id):
        self.cleared.append(record_id)


class FakeAssetRepository:
    def __init__(self, asset):
        self.asset = asset

    def list_assets(self, record_id):
        return [self.asset]


class FakeEsClient:
    index_name = "test-index"

    def __init__(self, fail_bulk=False):
        self.fail_bulk = fail_bulk
        self.deleted = []

    def bulk_index_batches(self, documents, **kwargs):
        if self.fail_bulk:
            return 0, len(documents)
        return len(documents), 0

    def delete_stale_documents_for_record(self, circular_db_id, active_chunk_ids):
        self.deleted.append((circular_db_id, list(active_chunk_ids)))


class FakeS3Client:
    def exists(self, path):
        return True


class FakeExtractor:
    def extract_blocks(self, path):
        return [Block("text", "1. First section\nSome content here", 0, 0.0)]


class FakeChunker:
    def chunk(self, blocks, *, circular_key):
        return [
            TextChunk(chunk_id=f"{circular_key}_0", chunk_index=0, text="t0"),
            TextChunk(chunk_id=f"{circular_key}_1", chunk_index=1, text="t1"),
        ]


def _make_indexer(record, fail_bulk=False):
    es_client = FakeEsClient(fail_bulk=fail_bulk)
    repository = FakeCircularRepository(record)
    indexer = ElasticsearchIndexer(
        circular_repository=repository,
        es_client=es_client,
        asset_repository=FakeAssetRepository(_make_asset(record.id)),
        pdf_extractor=FakeExtractor(),
        pdf_chunker=FakeChunker(),
        sebi_master_chunker=FakeChunker(),
        embedding_provider=NoOpEmbeddingProvider(),
        s3_client=FakeS3Client(),
    )
    return indexer, repository, es_client


def _disable_contextual_retrieval(monkeypatch):
    monkeypatch.setattr(Config, "ES_ENABLE_CONTEXTUAL_RETRIEVAL", False)


def test_batch_run_prunes_stale_chunks_after_success(monkeypatch):
    _disable_contextual_retrieval(monkeypatch)
    record = _make_record()
    indexer, repository, es_client = _make_indexer(record)

    processed, failed = indexer.run_once()

    assert (processed, failed) == (1, 0)
    expected_ids = [f"NSE:{record.circular_id}:extracted_pdf:s3://bucket/doc.pdf_0",
                    f"NSE:{record.circular_id}:extracted_pdf:s3://bucket/doc.pdf_1"]
    assert es_client.deleted == [(str(record.id), expected_ids)]
    assert repository.marked == [(record.id, 2, "test-index")]


def test_reindex_prunes_stale_chunks_after_success(monkeypatch):
    _disable_contextual_retrieval(monkeypatch)
    record = _make_record()
    indexer, repository, es_client = _make_indexer(record)

    assert indexer.reindex_record(record.id) is True
    assert len(es_client.deleted) == 1
    assert es_client.deleted[0][0] == str(record.id)


def test_batch_run_skips_prune_when_bulk_fails(monkeypatch):
    _disable_contextual_retrieval(monkeypatch)
    record = _make_record()
    indexer, repository, es_client = _make_indexer(record, fail_bulk=True)

    processed, failed = indexer.run_once()

    assert (processed, failed) == (0, 1)
    assert es_client.deleted == []
    assert repository.marked == []
