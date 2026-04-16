"""Elasticsearch indexing pipeline for fetched circular PDFs."""

from ingestion.indexer.chunker import FixedSizeChunker
from ingestion.indexer.dto import IndexDocument, SearchHit, TextChunk
from ingestion.indexer.embedding_provider import (
    EmbeddingProvider,
    HashingEmbeddingProvider,
    NoOpEmbeddingProvider,
    SentenceTransformerEmbeddingProvider,
    build_embedding_provider,
)
from ingestion.indexer.es_client import ElasticsearchClient
from ingestion.indexer.indexer import ElasticsearchIndexer
from ingestion.indexer.es_provider import get_es_client
from ingestion.indexer.pdf_extractor import PDFTextExtractor

__all__ = [
    "ElasticsearchClient",
    "ElasticsearchIndexer",
    "EmbeddingProvider",
    "FixedSizeChunker",
    "HashingEmbeddingProvider",
    "IndexDocument",
    "NoOpEmbeddingProvider",
    "PDFTextExtractor",
    "SearchHit",
    "SentenceTransformerEmbeddingProvider",
    "TextChunk",
    "build_embedding_provider",
    "get_es_client",
]
