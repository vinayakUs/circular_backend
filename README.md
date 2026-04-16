# Circular Backend

Minimal Flask starter project.

## Setup

```bash
source .venv/bin/activate
pip install -r requirements.txt
flask run
```

## Database Client

Use the shared DB client from one place:

```python
from db import get_db_client

db_client = get_db_client()
pool = db_client.get_pool()
pool.open()
```

Connection settings come from `.env`:
- `DATABASE_URL`
- `DB_MIN_SIZE`
- `DB_MAX_SIZE`
- `LOG_LEVEL`

## Run Ingestion

After installing the project in editable mode:

```bash
run-scrapper
```

Override sources for a single run if needed:

```bash
run-scrapper --sources NSE
run-scrapper --sources NSE,BSE
```

## Scraper Source Toggle

Use `SCRAPER_ENABLED_SOURCES` in `.env` to control which scrapers run.

```bash
SCRAPER_ENABLED_SOURCES=NSE
```

Example values:
- `NSE`
- `NSE,SEBI`
- `NSE,SEBI,BSE`

## Run Elasticsearch Indexer

The indexer reads `FETCHED` circulars from Postgres, extracts text from local PDFs, and indexes chunked documents into Elasticsearch.

```bash
run-indexer --setup-index
run-indexer
run-indexer --batch-size 100
run-indexer --record-id <circular-record-uuid>
```

Additional `.env` settings:
- `ELASTICSEARCH_URL`
- `ELASTICSEARCH_USERNAME`
- `ELASTICSEARCH_PASSWORD`
- `ELASTICSEARCH_INDEX_NAME`
- `ES_INDEXER_BATCH_SIZE`
- `ES_CHUNK_SIZE`
- `ES_CHUNK_OVERLAP`
- `ES_REQUEST_TIMEOUT_SECONDS`
- `ES_ENABLE_VECTORS`
- `ES_EMBEDDING_PROVIDER`
- `ES_EMBEDDING_MODEL_NAME`
- `ES_QUERY_EMBEDDING_INSTRUCTION`

For semantic retrieval with local embeddings, the default setup now uses:

```bash
ES_ENABLE_VECTORS=true
ES_EMBEDDING_PROVIDER=sentence-transformers
ES_EMBEDDING_MODEL_NAME=BAAI/bge-base-en-v1.5
ES_QUERY_EMBEDDING_INSTRUCTION="Represent this sentence for searching relevant passages: "
ES_SEARCH_DEFAULT_STRATEGY=hybrid
```

After changing embedding model or vector dimensions, recreate the index before reindexing:

```bash
run-indexer --delete-index
run-indexer --setup-index
run-indexer
```
