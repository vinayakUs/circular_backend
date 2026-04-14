import os

from dotenv import load_dotenv


load_dotenv()


def _parse_scraper_sources(raw_value: str | None) -> tuple[str, ...]:
    if raw_value is None:
        return ()

    sources = [item.strip().upper() for item in raw_value.split(",") if item.strip()]
    return tuple(dict.fromkeys(sources))


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key")
    FLASK_ENV = os.getenv("FLASK_ENV", "development")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    DATABASE_URL = os.getenv(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/circular_backend"
    )
    DB_MIN_SIZE = int(os.getenv("DB_MIN_SIZE", "1"))
    DB_MAX_SIZE = int(os.getenv("DB_MAX_SIZE", "5"))
    RAW_STORAGE_PATH = os.getenv("RAW_STORAGE_PATH", "data/regulatory_raw")
    SEBI_DETAIL_TIMEOUT_SECONDS = int(
        os.getenv("SEBI_DETAIL_TIMEOUT_SECONDS", "30")
    )
    SEBI_DETAIL_MAX_RETRIES = int(os.getenv("SEBI_DETAIL_MAX_RETRIES", "3"))
    SEBI_DETAIL_RETRY_BACKOFF_SECONDS = float(
        os.getenv("SEBI_DETAIL_RETRY_BACKOFF_SECONDS", "2")
    )
    SCRAPER_DEFAULT_LOOKBACK_DAYS = int(
        os.getenv("SCRAPER_DEFAULT_LOOKBACK_DAYS", "11") # default to 7 days if no data in db fetching historical data
    ) 
    SCRAPER_ENABLED_SOURCES = _parse_scraper_sources(
        os.getenv("SCRAPER_ENABLED_SOURCES")
    )
    ELASTICSEARCH_URL = os.getenv(
        "ELASTICSEARCH_URL", "https://es-eb59a8.es.us-central1.gcp.cloud.es.io"
    )
    ELASTICSEARCH_USERNAME = os.getenv("ELASTICSEARCH_USERNAME", "elastic")
    ELASTICSEARCH_PASSWORD = os.getenv(
        "ELASTICSEARCH_PASSWORD", "H9QWD2laWoDp3yVLWnz3MNcJ"
    )
    ELASTICSEARCH_INDEX_NAME = os.getenv(
        "ELASTICSEARCH_INDEX_NAME", "circulars_chunks"
    )
    ES_INDEXER_BATCH_SIZE = int(os.getenv("ES_INDEXER_BATCH_SIZE", "50"))
    ES_CHUNK_SIZE = int(os.getenv("ES_CHUNK_SIZE", "1200"))
    ES_CHUNK_OVERLAP = int(os.getenv("ES_CHUNK_OVERLAP", "200"))
    ES_REQUEST_TIMEOUT_SECONDS = int(os.getenv("ES_REQUEST_TIMEOUT_SECONDS", "30"))
