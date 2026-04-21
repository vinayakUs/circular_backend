from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock

from ingestion.indexer import ElasticsearchIndexer, FixedSizeChunker
from ingestion.indexer.embedding_provider import NoOpEmbeddingProvider
from ingestion.repository import CircularAsset
from ingestion.scrapper.dto import Circular
from tests.fakes import FakeCircularRepository


class IndexerRepositoryTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = FakeCircularRepository()

    def _insert_fetched_record(self) -> tuple[str, FakeCircularRepository]:
        circular = Circular(
            source="NSE",
            circular_id="FAOP73629",
            full_reference="NSE/FAOP/73629",
            department="FAOP",
            title="Business Continuity",
            issue_date=date(2026, 4, 6),
            url="https://example.com/circular",
            pdf_url="https://example.com/circular.pdf",
        )
        record_id, _created = self.repository.upsert_circular(circular)
        self.repository.update_file_path(record_id, "/tmp/example.pdf", "hash")
        self.repository.replace_assets(
            record_id,
            [
                CircularAsset(
                    asset_role="original_pdf",
                    file_path="/tmp/example.pdf",
                    content_hash="hash",
                    mime_type="application/pdf",
                )
            ],
        )
        self.repository.update_status(record_id, "FETCHED")
        return str(record_id), self.repository

    def test_list_pending_es_records_returns_only_unindexed_fetched_records(self) -> None:
        record_id, repository = self._insert_fetched_record()
        pending = repository.list_pending_es_records(limit=10)

        self.assertEqual(len(pending), 1)
        self.assertEqual(str(pending[0].id), record_id)

        repository.mark_es_indexed(pending[0].id, chunk_count=3, index_name="chunks")
        self.assertEqual(repository.list_pending_es_records(limit=10), [])

    def test_clear_es_index_state_makes_record_pending_again(self) -> None:
        _record_id, repository = self._insert_fetched_record()
        record = repository.list_pending_es_records(limit=10)[0]
        repository.mark_es_indexed(record.id, chunk_count=2, index_name="chunks")

        repository.clear_es_index_state(record.id)
        refreshed = repository.get_record_by_id(record.id)

        self.assertIsNotNone(refreshed)
        assert refreshed is not None
        self.assertIsNone(refreshed.es_indexed_at)
        self.assertIsNone(refreshed.es_chunk_count)
        self.assertIsNone(refreshed.es_index_name)


class ChunkerTestCase(unittest.TestCase):
    def test_fixed_size_chunker_is_deterministic(self) -> None:
        chunker = FixedSizeChunker(chunk_size=10, overlap=2)
        text = "abcdefghij klmnopqrst uvwxyz"

        first = chunker.chunk(text, circular_key="NSE:1")
        second = chunker.chunk(text, circular_key="NSE:1")

        self.assertEqual(first, second)
        self.assertGreater(len(first), 1)
        self.assertEqual(first[0].chunk_index, 0)


class IndexerWorkflowTestCase(unittest.TestCase):
    def _build_repository_with_file(self, temp_dir: str) -> tuple[FakeCircularRepository, Path]:
        repository = FakeCircularRepository()
        circular = Circular(
            source="NSE",
            circular_id="FAOP73629",
            full_reference="NSE/FAOP/73629",
            department="FAOP",
            title="Business Continuity",
            issue_date=date(2026, 4, 6),
            url="https://example.com/circular",
            pdf_url="https://example.com/circular.pdf",
        )
        record_id, _created = repository.upsert_circular(circular)
        pdf_path = Path(temp_dir) / "circular.pdf"
        pdf_path.write_bytes(b"not-really-a-pdf")
        repository.update_file_path(record_id, str(pdf_path), "hash")
        repository.replace_assets(
            record_id,
            [
                CircularAsset(
                    asset_role="original_pdf",
                    file_path=str(pdf_path),
                    content_hash="hash",
                    mime_type="application/pdf",
                )
            ],
        )
        repository.update_status(record_id, "FETCHED")
        return repository, pdf_path

    def test_indexer_marks_record_indexed_after_successful_bulk_index(self) -> None:
        with TemporaryDirectory() as temp_dir:
            repository, _pdf_path = self._build_repository_with_file(temp_dir)
            es_client = Mock()
            es_client.index_name = "circulars_chunks"
            extractor = Mock()
            extractor.extract.return_value = "alpha beta gamma delta " * 100
            es_client.bulk_index.side_effect = lambda docs: (len(docs), 0)
            indexer = ElasticsearchIndexer(
                circular_repository=repository,
                es_client=es_client,
                pdf_extractor=extractor,
                chunker=FixedSizeChunker(chunk_size=50, overlap=10),
                embedding_provider=NoOpEmbeddingProvider(),
                batch_size=10,
            )

            processed, failed = indexer.run_once()

            self.assertEqual((processed, failed), (1, 0))
            record = repository.list_records()[0]
            self.assertIsNotNone(record.es_indexed_at)
            self.assertEqual(record.es_index_name, "circulars_chunks")
            self.assertGreater(record.es_chunk_count or 0, 1)

    def test_indexer_leaves_record_pending_when_bulk_index_fails(self) -> None:
        with TemporaryDirectory() as temp_dir:
            repository, _pdf_path = self._build_repository_with_file(temp_dir)
            es_client = Mock()
            es_client.index_name = "circulars_chunks"
            es_client.bulk_index.return_value = (1, 1)
            extractor = Mock()
            extractor.extract.return_value = "alpha beta gamma delta " * 100
            indexer = ElasticsearchIndexer(
                circular_repository=repository,
                es_client=es_client,
                pdf_extractor=extractor,
                chunker=FixedSizeChunker(chunk_size=50, overlap=10),
                embedding_provider=NoOpEmbeddingProvider(),
                batch_size=10,
            )

            processed, failed = indexer.run_once()

            self.assertEqual((processed, failed), (0, 1))
            record = repository.list_records()[0]
            self.assertIsNone(record.es_indexed_at)
            self.assertEqual(len(repository.list_pending_es_records(limit=10)), 1)

    def test_indexer_processes_multiple_extracted_pdf_assets_for_one_circular(self) -> None:
        with TemporaryDirectory() as temp_dir:
            repository = FakeCircularRepository()
            circular = Circular(
                source="NSE",
                circular_id="ZIP123",
                full_reference="NSE/ZIP123",
                department="OPS",
                title="ZIP Circular",
                issue_date=date(2026, 4, 6),
                url="https://example.com/circular",
                pdf_url="https://example.com/archive.zip",
            )
            record_id, _created = repository.upsert_circular(circular)
            archive_path = Path(temp_dir) / "source.zip"
            first_pdf = Path(temp_dir) / "first.pdf"
            second_pdf = Path(temp_dir) / "second.pdf"
            archive_path.write_bytes(b"zip")
            first_pdf.write_bytes(b"%PDF-1.4 one")
            second_pdf.write_bytes(b"%PDF-1.4 two")
            repository.update_file_path(record_id, str(archive_path), "zip-hash")
            repository.replace_assets(
                record_id,
                [
                    CircularAsset(
                        asset_role="original_zip",
                        file_path=str(archive_path),
                        content_hash="zip-hash",
                        mime_type="application/zip",
                    ),
                    CircularAsset(
                        asset_role="extracted_pdf",
                        file_path=str(first_pdf),
                        content_hash="zip-hash",
                        mime_type="application/pdf",
                        archive_member_path="folder/first.pdf",
                    ),
                    CircularAsset(
                        asset_role="extracted_pdf",
                        file_path=str(second_pdf),
                        content_hash="zip-hash",
                        mime_type="application/pdf",
                        archive_member_path="folder/second.pdf",
                    ),
                ],
            )
            repository.update_status(record_id, "FETCHED")

            es_client = Mock()
            es_client.index_name = "circulars_chunks"
            captured_documents = []

            def bulk_index(documents):
                captured_documents.extend(documents)
                return len(documents), 0

            es_client.bulk_index.side_effect = bulk_index
            extractor = Mock()
            extractor.extract.side_effect = ["alpha beta " * 30, "gamma delta " * 30]
            indexer = ElasticsearchIndexer(
                circular_repository=repository,
                es_client=es_client,
                pdf_extractor=extractor,
                chunker=FixedSizeChunker(chunk_size=40, overlap=5),
                embedding_provider=NoOpEmbeddingProvider(),
                batch_size=10,
            )

            processed, failed = indexer.run_once()

            self.assertEqual((processed, failed), (1, 0))
            self.assertGreaterEqual(len(captured_documents), 2)
            self.assertEqual(
                {document.archive_member_path for document in captured_documents},
                {"folder/first.pdf", "folder/second.pdf"},
            )
            self.assertEqual(
                {document.asset_role for document in captured_documents},
                {"extracted_pdf"},
            )

    def test_reindex_preserves_existing_db_state_when_replacement_fails(self) -> None:
        with TemporaryDirectory() as temp_dir:
            repository, _pdf_path = self._build_repository_with_file(temp_dir)
            record = repository.list_records()[0]
            repository.mark_es_indexed(
                record.id,
                chunk_count=4,
                index_name="circulars_chunks",
            )

            es_client = Mock()
            es_client.index_name = "circulars_chunks"
            es_client.bulk_index.return_value = (1, 1)
            extractor = Mock()
            extractor.extract.return_value = "alpha beta gamma delta " * 100
            indexer = ElasticsearchIndexer(
                circular_repository=repository,
                es_client=es_client,
                pdf_extractor=extractor,
                chunker=FixedSizeChunker(chunk_size=50, overlap=10),
                embedding_provider=NoOpEmbeddingProvider(),
                batch_size=10,
            )

            result = indexer.reindex_record(record.id)

            self.assertFalse(result)
            refreshed = repository.get_record_by_id(record.id)
            self.assertIsNotNone(refreshed)
            assert refreshed is not None
            self.assertIsNotNone(refreshed.es_indexed_at)
            self.assertEqual(refreshed.es_chunk_count, 4)
            es_client.delete_stale_documents_for_record.assert_not_called()
            es_client.bulk_index.assert_called_once()

    def test_reindex_cleans_up_stale_chunks_after_successful_replacement(self) -> None:
        with TemporaryDirectory() as temp_dir:
            repository, _pdf_path = self._build_repository_with_file(temp_dir)
            record = repository.list_records()[0]
            repository.mark_es_indexed(
                record.id,
                chunk_count=4,
                index_name="circulars_chunks",
            )

            es_client = Mock()
            es_client.index_name = "circulars_chunks"
            es_client.bulk_index.side_effect = lambda docs: (len(docs), 0)
            extractor = Mock()
            extractor.extract.return_value = "alpha beta gamma delta " * 100
            indexer = ElasticsearchIndexer(
                circular_repository=repository,
                es_client=es_client,
                pdf_extractor=extractor,
                chunker=FixedSizeChunker(chunk_size=50, overlap=10),
                embedding_provider=NoOpEmbeddingProvider(),
                batch_size=10,
            )

            result = indexer.reindex_record(record.id)

            self.assertTrue(result)
            refreshed = repository.get_record_by_id(record.id)
            self.assertIsNotNone(refreshed)
            assert refreshed is not None
            self.assertIsNotNone(refreshed.es_indexed_at)
            self.assertGreater(refreshed.es_chunk_count or 0, 1)
            es_client.bulk_index.assert_called_once()
            es_client.delete_stale_documents_for_record.assert_called_once()

    def test_indexer_attaches_embeddings_when_provider_is_enabled(self) -> None:
        with TemporaryDirectory() as temp_dir:
            repository, _pdf_path = self._build_repository_with_file(temp_dir)
            es_client = Mock()
            es_client.index_name = "circulars_chunks"
            captured_documents = []

            def bulk_index(documents):
                captured_documents.extend(documents)
                return len(documents), 0

            es_client.bulk_index.side_effect = bulk_index
            extractor = Mock()
            extractor.extract.return_value = "margin trading exposure collateral " * 50
            mock_embedding_provider = Mock()
            mock_embedding_provider.embed_texts.return_value = [[0.1] * 16]
            mock_embedding_provider.embed_query.return_value = [0.1] * 16
            indexer = ElasticsearchIndexer(
                circular_repository=repository,
                es_client=es_client,
                pdf_extractor=extractor,
                chunker=FixedSizeChunker(chunk_size=60, overlap=10),
                embedding_provider=mock_embedding_provider,
                batch_size=10,
            )

            processed, failed = indexer.run_once()

            self.assertEqual((processed, failed), (1, 0))
            self.assertTrue(captured_documents)
            self.assertTrue(all(document.embedding is not None for document in captured_documents))
            self.assertEqual(len(captured_documents[0].embedding or []), 16)
