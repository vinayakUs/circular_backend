import logging
import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

from langchain_classic.chains.combine_documents.base import BaseCombineDocumentsChain
from langchain_classic.chains.combine_documents.reduce import ReduceDocumentsChain
from langchain_core.documents import Document

from config import Config
from ingestion.processor.document_summarizer import (
    MAX_CHUNK_CHARS,
    DocumentSummarizerProcessor,
)


class NonShrinkingCombineChain(BaseCombineDocumentsChain):
    calls: int = 0

    def prompt_length(self, docs: list[Document], **kwargs) -> int:
        return len(docs) * 2

    def combine_docs(self, docs: list[Document], **kwargs) -> tuple[str, dict]:
        self.calls += 1
        return docs[0].page_content, {}

    async def acombine_docs(
        self,
        docs: list[Document],
        **kwargs,
    ) -> tuple[str, dict]:
        return self.combine_docs(docs, **kwargs)


class DocumentSummarizerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.processor = DocumentSummarizerProcessor.__new__(
            DocumentSummarizerProcessor
        )
        self.processor.logger = logging.getLogger("test.document_summarizer")
        self.processor.asset_repo = MagicMock()
        self.processor.summary_repo = MagicMock()
        self.processor.circular_repo = MagicMock()

        self.record = SimpleNamespace(
            id=uuid4(),
            circular_id="TEST001",
            source="NSE",
            source_item_key="TEST001",
            issue_date=date(2026, 7, 28),
        )
        self.processor.asset_repo.list_assets.return_value = [
            SimpleNamespace(
                asset_role="original_pdf",
                file_path="s3://bucket/source.pdf",
            )
        ]

    def process_with_text(self, text: str, chain: MagicMock) -> MagicMock:
        with (
            patch(
                "ingestion.processor.document_summarizer.PDFTextExtractor"
            ) as extractor_class,
            patch(
                "ingestion.processor.document_summarizer.LangChainLLMAdapter"
            ),
            patch(
                "ingestion.processor.document_summarizer.load_summarize_chain",
                return_value=chain,
            ) as load_chain,
        ):
            extractor_class.return_value.extract.return_value = text
            self.processor.process(self.record)
        return load_chain

    def process_with_text_and_adapter(
        self, text: str, chain: MagicMock
    ) -> tuple[MagicMock, MagicMock]:
        with (
            patch(
                "ingestion.processor.document_summarizer.PDFTextExtractor"
            ) as extractor_class,
            patch(
                "ingestion.processor.document_summarizer.LangChainLLMAdapter"
            ) as adapter_class,
            patch(
                "ingestion.processor.document_summarizer.load_summarize_chain",
                return_value=chain,
            ) as load_chain,
        ):
            extractor_class.return_value.extract.return_value = text
            self.processor.process(self.record)
        return load_chain, adapter_class

    def test_large_document_passes_collapse_retry_limit(self) -> None:
        chain = MagicMock()
        chain.invoke.return_value = {"output_text": "Complete summary"}

        with patch.object(Config, "SUMMARIZER_COLLAPSE_MAX_RETRIES", 3):
            load_chain = self.process_with_text("x" * (MAX_CHUNK_CHARS + 1), chain)

        kwargs = load_chain.call_args.kwargs
        self.assertEqual(kwargs["chain_type"], "map_reduce")
        self.assertEqual(kwargs["collapse_max_retries"], 3)
        self.processor.summary_repo.delete_summary_for_circular.assert_called_once_with(
            self.record.id
        )
        self.processor.summary_repo.upload_and_store_summary.assert_called_once()

    def test_invalid_collapse_retry_limit_fails_before_chain_invocation(self) -> None:
        for invalid_value in (0, -1):
            with self.subTest(collapse_max_retries=invalid_value):
                self.processor.summary_repo.reset_mock()
                with (
                    patch.object(
                        Config,
                        "SUMMARIZER_COLLAPSE_MAX_RETRIES",
                        invalid_value,
                    ),
                    patch(
                        "ingestion.processor.document_summarizer.PDFTextExtractor"
                    ) as extractor_class,
                    patch(
                        "ingestion.processor.document_summarizer.LangChainLLMAdapter"
                    ),
                    patch(
                        "ingestion.processor.document_summarizer.load_summarize_chain"
                    ) as load_chain,
                ):
                    extractor_class.return_value.extract.return_value = (
                        "x" * (MAX_CHUNK_CHARS + 1)
                    )

                    with self.assertRaisesRegex(
                        ValueError,
                        "SUMMARIZER_COLLAPSE_MAX_RETRIES must be at least 1",
                    ):
                        self.processor.process(self.record)

                load_chain.assert_not_called()
                self.processor.summary_repo.delete_summary_for_circular.assert_not_called()
                self.processor.summary_repo.upload_and_store_summary.assert_not_called()

    def test_small_document_keeps_stuff_chain_unchanged(self) -> None:
        chain = MagicMock()
        chain.invoke.return_value = {"output_text": "Small summary"}

        with patch.object(Config, "SUMMARIZER_COLLAPSE_MAX_RETRIES", 0):
            load_chain = self.process_with_text("small document", chain)

        kwargs = load_chain.call_args.kwargs
        self.assertEqual(kwargs["chain_type"], "stuff")
        self.assertNotIn("collapse_max_retries", kwargs)

    def test_collapse_failure_does_not_persist_summary(self) -> None:
        chain = MagicMock()
        chain.invoke.side_effect = ValueError("Exceed 3 tries to collapse document")

        with patch.object(Config, "SUMMARIZER_COLLAPSE_MAX_RETRIES", 3):
            with self.assertRaisesRegex(
                RuntimeError,
                "LangChain summarization failed",
            ):
                self.process_with_text("x" * (MAX_CHUNK_CHARS + 1), chain)

        self.processor.summary_repo.delete_summary_for_circular.assert_not_called()
        self.processor.summary_repo.upload_and_store_summary.assert_not_called()

    def test_langchain_stops_non_shrinking_collapse_at_retry_limit(self) -> None:
        combine_chain = NonShrinkingCombineChain()
        reduce_chain = ReduceDocumentsChain(
            combine_documents_chain=combine_chain,
            token_max=3,
            collapse_max_retries=2,
        )
        docs = [Document(page_content="first"), Document(page_content="second")]

        with self.assertRaisesRegex(ValueError, r"Exceed 2 tries to\s+collapse"):
            reduce_chain.combine_docs(docs)

        self.assertEqual(combine_chain.calls, 4)

    def test_large_document_passes_max_tokens_to_adapter(self) -> None:
        chain = MagicMock()
        chain.invoke.return_value = {"output_text": "Complete summary"}

        with patch.object(Config, "SUMMARIZER_MAX_OUTPUT_TOKENS", 500):
            _, adapter_class = self.process_with_text_and_adapter(
                "x" * (MAX_CHUNK_CHARS + 1), chain
            )

        self.assertEqual(
            adapter_class.call_args.kwargs["max_tokens"], 500
        )

    def test_small_document_passes_max_tokens_to_adapter(self) -> None:
        chain = MagicMock()
        chain.invoke.return_value = {"output_text": "Small summary"}

        with patch.object(Config, "SUMMARIZER_MAX_OUTPUT_TOKENS", 250):
            _, adapter_class = self.process_with_text_and_adapter(
                "small document", chain
            )

        self.assertEqual(
            adapter_class.call_args.kwargs["max_tokens"], 250
        )

    def test_invalid_max_tokens_fails_before_chain_invocation(self) -> None:
        for invalid_value in (0, -1):
            with self.subTest(max_output_tokens=invalid_value):
                self.processor.summary_repo.reset_mock()
                with (
                    patch.object(
                        Config,
                        "SUMMARIZER_MAX_OUTPUT_TOKENS",
                        invalid_value,
                    ),
                    patch(
                        "ingestion.processor.document_summarizer.PDFTextExtractor"
                    ) as extractor_class,
                    patch(
                        "ingestion.processor.document_summarizer.LangChainLLMAdapter"
                    ) as adapter_class,
                    patch(
                        "ingestion.processor.document_summarizer.load_summarize_chain"
                    ) as load_chain,
                ):
                    extractor_class.return_value.extract.return_value = (
                        "x" * (MAX_CHUNK_CHARS + 1)
                    )

                    with self.assertRaisesRegex(
                        ValueError,
                        "SUMMARIZER_MAX_OUTPUT_TOKENS must be at least 1",
                    ):
                        self.processor.process(self.record)

                adapter_class.assert_not_called()
                load_chain.assert_not_called()
                self.processor.summary_repo.delete_summary_for_circular.assert_not_called()
                self.processor.summary_repo.upload_and_store_summary.assert_not_called()


if __name__ == "__main__":
    unittest.main()
