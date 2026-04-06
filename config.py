import os


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
        "DATABASE_URL", "postgresql://admin:password@localhost:5432/circular_backend"
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
        os.getenv("SCRAPER_DEFAULT_LOOKBACK_DAYS", "111") # default to 7 days if no data in db fetching historical data
    ) 
    SCRAPER_ENABLED_SOURCES = _parse_scraper_sources(
        os.getenv("SCRAPER_ENABLED_SOURCES")
    )
