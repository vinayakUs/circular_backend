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

    NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY", "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")
    ACTION_ITEM_MODEL = os.getenv(
        "ACTION_ITEM_MODEL", "minimaxai/minimax-m2.7"
    )
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
        os.getenv("SCRAPER_DEFAULT_LOOKBACK_DAYS", "365") # default to 3 days if no data in db fetching historical data
    ) 
    SCRAPER_ENABLED_SOURCES = _parse_scraper_sources(
        os.getenv("SCRAPER_ENABLED_SOURCES")
    )
    ELASTICSEARCH_URL = os.getenv(
        "ELASTICSEARCH_URL", "http://localhost:9200"
    )
    ELASTICSEARCH_USERNAME = os.getenv("ELASTICSEARCH_USERNAME", "elastic")
    ELASTICSEARCH_PASSWORD = os.getenv(
        "ELASTICSEARCH_PASSWORD", "H9QWD2laWoDp3yVLWnz3MNcJ"
    )
    ELASTICSEARCH_INDEX_NAME = os.getenv(
        "ELASTICSEARCH_INDEX_NAME", "circulars_chunks"
    )
    ES_INDEXER_BATCH_SIZE = int(os.getenv("ES_INDEXER_BATCH_SIZE", "50"))
    ES_CHUNK_SIZE = int(os.getenv("ES_CHUNK_SIZE", "800"))
    ES_CHUNK_OVERLAP = int(os.getenv("ES_CHUNK_OVERLAP", "150"))
    ES_REQUEST_TIMEOUT_SECONDS = int(os.getenv("ES_REQUEST_TIMEOUT_SECONDS", "30"))
    ES_ENABLE_VECTORS = os.getenv("ES_ENABLE_VECTORS", "true").lower() == "true"
    ES_VECTOR_DIMS = int(os.getenv("ES_VECTOR_DIMS", "768"))
    ES_EMBEDDING_PROVIDER = os.getenv(
        "ES_EMBEDDING_PROVIDER", "sentence-transformers"
    ).strip().lower()
    ES_EMBEDDING_MODEL_NAME = os.getenv(
        "ES_EMBEDDING_MODEL_NAME", "BAAI/bge-base-en-v1.5"
    ).strip()
    ES_QUERY_EMBEDDING_INSTRUCTION = os.getenv(
        "ES_QUERY_EMBEDDING_INSTRUCTION",
        "Represent this sentence for searching relevant passages: ",
    )
    ES_SEARCH_DEFAULT_STRATEGY = os.getenv(
        "ES_SEARCH_DEFAULT_STRATEGY", "hybrid"
    ).strip().lower()
    ES_ENABLE_CONTEXTUAL_RETRIEVAL = os.getenv(
        "ES_ENABLE_CONTEXTUAL_RETRIEVAL", "true"
    ).lower() == "true"
    ES_CONTEXTUAL_MODEL = os.getenv(
        "ES_CONTEXTUAL_MODEL", "minimaxai/minimax-m2.7"
    )
    ES_CONTEXTUAL_MAX_TOKENS = int(
        os.getenv("ES_CONTEXTUAL_MAX_TOKENS", "500")
    )
    ES_RRF_WINDOW_SIZE = int(os.getenv("ES_RRF_WINDOW_SIZE", "50"))
    ES_RRF_RANK_CONSTANT = int(os.getenv("ES_RRF_RANK_CONSTANT", "60"))
    RAG_MODEL = os.getenv("RAG_MODEL", "minimaxai/minimax-m2.7")
    RAG_MAX_CHUNKS = int(os.getenv("RAG_MAX_CHUNKS", "10"))
    RAG_MAX_TOKENS = int(os.getenv("RAG_MAX_TOKENS", "4000"))
