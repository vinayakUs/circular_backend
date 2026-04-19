import unittest
from unittest.mock import Mock, patch

from ingestion.indexer.contextualizer import Contextualizer, ChunkContext, ContextResponse


class TestContextualizer(unittest.TestCase):
    def test_contextualize_chunks_empty(self):
        contextualizer = Contextualizer()
        result = contextualizer.contextualize_chunks(
            chunks=[],
            circular_title="Test Circular",
            full_reference="TEST/123",
        )
        self.assertEqual(result, [])

    @patch("ingestion.indexer.contextualizer.get_llm_client")
    def test_contextualize_chunks_success(self, mock_get_llm_client):
        mock_llm_client = Mock()
        mock_response = ContextResponse(context="This is test context")
        mock_llm_client.chat.completions.create.return_value = mock_response
        mock_get_llm_client.return_value = mock_llm_client

        contextualizer = Contextualizer()
        chunks = ["First chunk", "Second chunk"]
        result = contextualizer.contextualize_chunks(
            chunks=chunks,
            circular_title="Test Circular",
            full_reference="TEST/123",
        )

        self.assertEqual(len(result), 2)
        self.assertTrue(all(isinstance(ctx, ChunkContext) for ctx in result))
        self.assertEqual(result[0].chunk_index, 0)
        self.assertEqual(result[0].chunk_text, "First chunk")
        self.assertEqual(result[0].context, "This is test context")
        self.assertEqual(result[1].chunk_index, 1)
        self.assertEqual(result[1].chunk_text, "Second chunk")

    @patch("ingestion.indexer.contextualizer.get_llm_client")
    def test_contextualize_chunks_fallback_on_error(self, mock_get_llm_client):
        mock_llm_client = Mock()
        mock_llm_client.chat.completions.create.side_effect = Exception("API error")
        mock_get_llm_client.return_value = mock_llm_client

        contextualizer = Contextualizer()
        chunks = ["First chunk", "Second chunk"]
        result = contextualizer.contextualize_chunks(
            chunks=chunks,
            circular_title="Test Circular",
            full_reference="TEST/123",
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].context, "")
        self.assertEqual(result[1].context, "")

    def test_chunk_context_get_contextualized_text(self):
        chunk_context = ChunkContext(
            chunk_index=0,
            chunk_text="Original chunk text",
            context="This is contextual prefix",
        )
        result = chunk_context.get_contextualized_text()
        self.assertEqual(result, "This is contextual prefix\n\nOriginal chunk text")

    def test_chunk_context_get_contextualized_text_empty_context(self):
        chunk_context = ChunkContext(
            chunk_index=0,
            chunk_text="Original chunk text",
            context="",
        )
        result = chunk_context.get_contextualized_text()
        self.assertEqual(result, "\n\nOriginal chunk text")

    def test_build_prompt(self):
        contextualizer = Contextualizer()
        prompt = contextualizer._build_prompt(
            chunk_text="Test chunk content",
            chunk_index=0,
            total_chunks=5,
            circular_title="Test Circular",
            full_reference="TEST/123",
        )

        self.assertIn("Test Circular", prompt)
        self.assertIn("TEST/123", prompt)
        self.assertIn("Test chunk content", prompt)
        self.assertIn("Chunk 1 of 5", prompt)
        self.assertIn("regulatory compliance expert", prompt)
        self.assertIn("50-100 tokens", prompt)


if __name__ == "__main__":
    unittest.main()
