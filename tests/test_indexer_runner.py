import unittest
from unittest.mock import patch

from ingestion.indexer import runner


class IndexerRunnerTestCase(unittest.TestCase):
    @patch("ingestion.indexer.runner.CircularRepository")
    @patch("ingestion.indexer.runner.get_db_client")
    @patch("ingestion.indexer.runner.ElasticsearchIndexer")
    @patch("ingestion.indexer.runner.ElasticsearchClient")
    @patch("sys.argv", ["run-indexer", "--batch-size", "25", "--setup-index"])
    def test_main_builds_indexer_and_runs_once(
        self, es_client_cls, indexer_cls, get_db_client, repository_cls
    ) -> None:
        db_client = get_db_client.return_value
        db_client.get_pool.return_value = object()
        es_client = es_client_cls.return_value
        repository = repository_cls.return_value

        result = runner.main()

        es_client.setup_index.assert_called_once_with()
        indexer_cls.return_value.run_once.assert_not_called()
        indexer_cls.assert_called_once()
        _, kwargs = indexer_cls.call_args
        self.assertEqual(kwargs["batch_size"], 25)
        self.assertIs(kwargs["circular_repository"], repository)
        db_client.close.assert_called_once_with()
        self.assertEqual(result, 0)

    @patch("ingestion.indexer.runner.CircularRepository")
    @patch("ingestion.indexer.runner.get_db_client")
    @patch("ingestion.indexer.runner.ElasticsearchIndexer")
    @patch("ingestion.indexer.runner.ElasticsearchClient")
    @patch("sys.argv", ["run-indexer"])
    def test_main_runs_indexing_when_setup_flag_is_absent(
        self, es_client_cls, indexer_cls, get_db_client, repository_cls
    ) -> None:
        db_client = get_db_client.return_value
        db_client.get_pool.return_value = object()
        indexer = indexer_cls.return_value

        result = runner.main()

        es_client_cls.return_value.setup_index.assert_not_called()
        indexer.run_once.assert_called_once_with()
        db_client.close.assert_called_once_with()
        self.assertEqual(result, 0)

    @patch("ingestion.indexer.runner.CircularRepository")
    @patch("ingestion.indexer.runner.get_db_client")
    @patch("ingestion.indexer.runner.ElasticsearchIndexer")
    @patch("ingestion.indexer.runner.ElasticsearchClient")
    @patch("sys.argv", ["run-indexer", "--delete-index"])
    def test_main_deletes_index_and_exits(
        self, es_client_cls, indexer_cls, get_db_client, repository_cls
    ) -> None:
        db_client = get_db_client.return_value
        db_client.get_pool.return_value = object()
        es_client = es_client_cls.return_value
        indexer = indexer_cls.return_value

        result = runner.main()

        es_client.delete_index.assert_called_once_with()
        es_client.setup_index.assert_not_called()
        indexer.run_once.assert_not_called()
        db_client.close.assert_called_once_with()
        self.assertEqual(result, 0)

    @patch("ingestion.indexer.runner.CircularRepository")
    @patch("ingestion.indexer.runner.get_db_client")
    @patch("ingestion.indexer.runner.ElasticsearchIndexer")
    @patch("ingestion.indexer.runner.ElasticsearchClient")
    @patch("sys.argv", ["run-indexer", "--delete-index", "--reset-db", "--reset-bloom"])
    def test_main_runs_combined_maintenance_flags_and_exits(
        self, es_client_cls, indexer_cls, get_db_client, repository_cls
    ) -> None:
        db_client = get_db_client.return_value
        db_client.get_pool.return_value = object()
        es_client = es_client_cls.return_value
        repository = repository_cls.return_value

        result = runner.main()

        es_client.delete_index.assert_called_once_with()
        repository.clear_all_es_index_state.assert_called_once_with()
        repository.reset_bloom_state.assert_called_once_with()
        indexer_cls.return_value.run_once.assert_not_called()
        db_client.close.assert_called_once_with()
        self.assertEqual(result, 0)
