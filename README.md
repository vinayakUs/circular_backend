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
