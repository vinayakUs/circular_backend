# Elasticsearch Indexing Pipeline

## Overview

A separate, decoupled indexing pipeline that reads circulars from the database and indexes their PDF content to Elasticsearch for full-text search.

```
┌─────────────────────────────────────────────────┐     ┌─────────────────────────────────────────────────┐
│         ScraperOrchestrator (EXISTING)          │     │     ElasticsearchIndexer (NEW)                 │
├─────────────────────────────────────────────────┤     ├─────────────────────────────────────────────────┤
│                                                 │     │                                                 │
│  DISCOVERED → FETCHED                         │────►│  Reads: circulars WHERE status = 'FETCHED'     │
│                                                 │     │  AND es_indexed_at IS NULL                    │
│  • Only handles scraping & PDF download        │     │                                                 │
│  • No ES dependencies                          │     │  Process:                                       │
│  • Resilient to ES downtime                    │     │  1. Extract text from PDF                      │
└─────────────────────────────────────────────────┘     │  2. Chunk text                                  │
                                                          │  3. Index to ES                                  │
                                                          │  4. Update status → es_indexed_at                │
                                                          │  5. Update bloom filter                          │
                                                          └─────────────────────────────────────────────────┘
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                   ingestion.indexer                                   │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────────┐    │
│  │                          ElasticsearchIndexer                                │    │
│  │                            (Pipeline Orchestrator)                           │    │
│  ├─────────────────────────────────────────────────────────────────────────────┤    │
│  │  - db_pool: asyncpg.Pool                                                    │    │
│  │  - es_client: ElasticsearchClient                                           │    │
│  │  - pdf_extractor: PDFTextExtractor                                          │    │
│  │  - chunker: ChunkingStrategy                                               │    │
│  │  - bloom_manager: BloomFilterManager                                       │    │
│  │  - batch_size: int                                                         │    │
│  │  - poll_interval: int                                                      │    │
│  ├─────────────────────────────────────────────────────────────────────────────┤    │
│  │  + run(once: bool = False)                                                 │    │
│  │  + _fetch_pending_circulars() → List[CircularRecord]                        │    │
│  │  + _process_circular(circular: CircularRecord) → bool                         │    │
│  │  + _mark_indexed(circular_id: UUID, chunk_count: int)                     │    │
│  │  + _mark_failed(circular_id: UUID, error: str)                             │    │
│  └─────────────────────────────────────────────────────────────────────────────┘    │
│                                          │                                          │
│                                          │ uses                                     │
│              ┌───────────────────────────┼───────────────────────────┐              │
│              │                           │                           │              │
│              ▼                           ▼                           ▼              │
│  ┌───────────────────────┐  ┌───────────────────────┐  ┌───────────────────────┐  │
│  │   PDFTextExtractor    │  │   ElasticsearchClient  │  │  BloomFilterManager   │  │
│  ├───────────────────────┤  ├───────────────────────┤  ├───────────────────────┤  │
│  │ + extract(path) → str │  │ + bulk_index(docs)   │  │ + check(id) → bool    │  │
│  │ + extract_bytes(path) │  │ + index_document(doc)│  │ + add(id)             │  │
│  │   → bytes             │  │ + delete_by_query()  │  │ + persist()           │  │
│  └───────────────────────┘  │ + create_index()     │  │ + load()              │  │
│                             │ + health_check()     │  │ + serialize() → bytes │  │
│                             └───────────────────────┘  └───────────────────────┘  │
│                                          │                                          │
│                                          │ uses                                     │
│                                          ▼                                          │
│                             ┌───────────────────────┐                               │
│                             │   ChunkingStrategy    │                               │
│                             │   <<interface>>       │                               │
│                             ├───────────────────────┤                               │
│                             │ + chunk(text)         │                               │
│                             │   → List[TextChunk]  │                               │
│                             └───────────┬───────────┘                               │
│                                         │ implements                                │
│                                         ▼                                           │
│                             ┌───────────────────────┐                               │
│                             │FixedSizeChunking      │                               │
│                             ├───────────────────────┤                               │
│                             │ - chunk_size: int     │                               │
│                             │ - overlap: int        │                               │
│                             │ + chunk(text)         │                               │
│                             │   → List[TextChunk]  │                               │
│                             └───────────────────────┘                               │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Design Patterns

| Pattern | Class | Purpose |
|---------|-------|---------|
| Pipeline Orchestrator | `ElasticsearchIndexer` | Coordinates workflow steps |
| Strategy | `ChunkingStrategy` | Pluggable text chunking algorithms |
| Facade | `ElasticsearchClient` | Hides ES complexity |
| DTO | `TextChunk`, `IndexDocument` | Data transfer objects |

## Database Schema

### circulars table (additions)
```sql
es_indexed_at TIMESTAMPTZ,      -- When indexed to ES
es_chunk_count INT,             -- Number of chunks created
es_index_name VARCHAR(100),     -- ES index used
```

### scraper_checkpoints table (additions)
```sql
es_bloom_filter BYTEA,          -- Bloom filter of indexed circular_ids
es_last_run_at TIMESTAMPTZ,     -- Last ES indexing run
es_records_processed INT DEFAULT 0
```

### Index for efficient querying
```sql
CREATE INDEX idx_circulars_es_pending 
    ON circulars(status, es_indexed_at) 
    WHERE file_path IS NOT NULL;
```

## Bloom Filter Tracking

### Purpose
Memory-efficient tracking of which circulars have been indexed to ES without querying ES directly.

### How It Works
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         BloomFilterManager                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   load()          check()           add()           persist()               │
│     │               │                │                │                     │
│     ▼               ▼                ▼                ▼                     │
│  ┌──────┐      ┌──────────┐     ┌──────────┐    ┌──────────────┐           │
│  │ Load │      │ Bloom    │     │ Add to   │    │ Update DB:   │           │
│  │ from │─────►│ contains?│────►│ in-memory│────►│ es_bloom_    │           │
│  │ DB   │      │ O(1)     │     │ filter   │     │ filter       │           │
│  └──────┘      └──────────┘     └──────────┘    └──────────────┘           │
│                                                                              │
│  Capacity: 100,000 items @ 1% false positive rate ≈ 1.2 MB                  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Trade-offs
- False positives (~1%): May skip re-indexing ~1% of already-indexed docs (safe)
- False negatives: Impossible (will never skip an unindexed doc)

## Pipeline Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  ElasticsearchIndexer.run()                                                   │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  LOOP (while not stopped)                                             │   │
│  │                                                                      │   │
│  │  1. _fetch_pending_circulars()                                        │   │
│  │     ├── SELECT * FROM circulars                                       │   │
│  │     │   WHERE status = 'FETCHED'                                      │   │
│  │     │     AND file_path IS NOT NULL                                  │   │
│  │     │     AND es_indexed_at IS NULL                                  │   │
│  │     │   LIMIT batch_size                                             │   │
│  │     └── RETURN List[CircularRecord]                                   │   │
│  │                                                                      │   │
│  │  2. FOR EACH circular IN pending:                                    │   │
│  │     ├── _process_circular(circular)                                  │   │
│  │     │   ├── pdf_extractor.extract(file_path)                         │   │
│  │     │   │   └── RETURN text: str                                      │   │
│  │     │   │                                                            │   │
│  │     │   ├── chunker.chunk(text)                                      │   │
│  │     │   │   └── RETURN chunks: List[TextChunk]                       │   │
│  │     │   │                                                            │   │
│  │     │   ├── bloom_manager.check(circular_id)                         │   │
│  │     │   │   └── IF already indexed, SKIP                              │   │
│  │     │   │                                                            │   │
│  │     │   ├── es_client.bulk_index(documents)                          │   │
│  │     │   │   └── RETURN success_count, failed_count                    │   │
│  │     │   │                                                            │   │
│  │     │   └── bloom_manager.add(circular_id)                           │   │
│  │     │                                                                │   │
│  │     └── _mark_indexed(id, chunk_count)                                │   │
│  │         ├── UPDATE circulars SET es_indexed_at = NOW(),              │   │
│  │         |   es_chunk_count = ?, es_index_name = 'circulars_chunks'   │   │
│  │         └── bloom_manager.persist()  # batched                        │   │
│  │                                                                      │   │
│  │  3. sleep(poll_interval)                                             │   │
│  │                                                                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Elasticsearch Index Mapping

```json
{
  "index": "circulars_chunks",
  "mappings": {
    "properties": {
      "chunk_id":    { "type": "keyword" },
      "circular_id": { "type": "keyword" },
      "source":      { "type": "keyword" },
      "title":       { "type": "text", "analyzer": "english" },
      "department":  { "type": "keyword" },
      "issue_date":  { "type": "date" },
      "chunk_index": { "type": "integer" },
      "content":     { "type": "text", "analyzer": "english" },
      "file_path":   { "type": "keyword" },
      "created_at":  { "type": "date" }
    }
  }
}
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `run` | Continuous mode (polls every 60s) |
| `run --once` | Process pending, then exit |
| `health` | Check ES cluster health |
| `setup-index` | Create ES index with mapping |
| `reindex` | Re-index all (reset bloom) |
| `reindex --id <id>` | Re-index specific circular |

## File Structure

```
ingestion/
├── indexer/
│   ├── __init__.py
│   ├── main.py              # CLI entry point
│   ├── indexer.py           # ElasticsearchIndexer
│   ├── pdf_extractor.py     # PDFTextExtractor
│   ├── chunker.py           # ChunkingStrategy + FixedSizeChunking
│   ├── es_client.py         # ElasticsearchClient
│   ├── bloom_filter.py      # BloomFilterManager
│   └── dto.py               # TextChunk, IndexDocument
├── repository/
│   └── circular_repository.py  # Updated with ES methods
└── config.py               # ES configuration
```

## Dependencies

```
# requirements.txt
elasticsearch>=8.0.0
pdfminer.six>=20221105
pybloom-live>=4.0.0
```
