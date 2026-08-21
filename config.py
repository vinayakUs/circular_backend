import os

from dotenv import load_dotenv
load_dotenv(override=True)


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
    MINMAX_API_KEY = os.getenv("MINMAX_API_KEY", "")
    MINMAX_BASE_URL = os.getenv("MINMAX_BASE_URL", "https://api.minimax.chat/v1")
    ACTION_ITEM_MODEL = os.getenv(
        "ACTION_ITEM_MODEL", "minimaxai/minimax-m2.7"
    )
    # ==== LDAP Configuration ====
    # For NTLM (Windows AD / Production) - set USE_NTLM=True in ldap_auth.py
    LDAP_SERVER = os.getenv("LDAP_SERVER", "ldap://localhost:389")  # e.g. ldaps://rootdc01.nseroot.com
    LDAP_PORT = int(os.getenv("LDAP_PORT", "389"))  # 636 for LDAPS (NTLM), 389 for simple bind
    LDAP_DOMAIN = os.getenv("LDAP_DOMAIN", "")  # e.g. ENSEROOT (only for NTLM)

    # For Simple Bind (OpenLDAP / Development) - set USE_NTLM=False in ldap_auth.py
    LDAP_BASE_DN = os.getenv("LDAP_BASE_DN", "dc=company,dc=com")  # only for simple bind
    LDAP_USER_DN_TEMPLATE = os.getenv("LDAP_USER_DN_TEMPLATE", "uid={username},ou=users,dc=company,dc=com")  # only for simple bind
    # ==== End LDAP ====
    JWT_SECRET = os.getenv("JWT_SECRET", "s2L65pGQtRN0Tu1ZDAH80SqP1Rl7FgWXOzanvGKeOS0")
    JWT_ALGORITHM = "HS256"
    JWT_EXPIRATION_HOURS = int(os.getenv("JWT_EXPIRATION_HOURS", "24"))
    DATABASE_URL = os.getenv(
        "DATABASE_URL", "oracle+oracledb://circular_user:MyAppPass123@localhost:1521/XEPDB1"
    )
    POSTGRES_URL = os.getenv(
        "POSTGRES_URL", "postgresql://postgres:postgres@localhost:5432/circular_backend"
    )
    DB_MIN_SIZE = int(os.getenv("DB_MIN_SIZE", "3"))
    DB_MAX_SIZE = int(os.getenv("DB_MAX_SIZE", "20"))
    SEBI_DETAIL_TIMEOUT_SECONDS = int(
        os.getenv("SEBI_DETAIL_TIMEOUT_SECONDS", "30")
    )
    SEBI_DETAIL_MAX_RETRIES = int(os.getenv("SEBI_DETAIL_MAX_RETRIES", "3"))
    SEBI_DETAIL_RETRY_BACKOFF_SECONDS = float(
        os.getenv("SEBI_DETAIL_RETRY_BACKOFF_SECONDS", "2")
    )
    SUMMARIZER_COLLAPSE_MAX_RETRIES = int(
        os.getenv("SUMMARIZER_COLLAPSE_MAX_RETRIES", "3")
    )
    SUMMARIZER_MAX_OUTPUT_TOKENS = int(
        os.getenv("SUMMARIZER_MAX_OUTPUT_TOKENS", "500")
    )
    SCRAPER_DEFAULT_LOOKBACK_DAYS = int(
        os.getenv("SCRAPER_DEFAULT_LOOKBACK_DAYS", "4") # default to 1 day if no data in db fetching historical data
    ) 
    SCRAPER_ENABLED_SOURCES = _parse_scraper_sources(
        os.getenv("SCRAPER_ENABLED_SOURCES")
    )
    ENFORCEMENT_CRAWLER_BASE_URL = os.getenv(
        "ENFORCEMENT_CRAWLER_BASE_URL", "http://localhost:8001"
    ).rstrip("/")
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
    ES_CHUNK_OVERLAP = int(os.getenv("ES_CHUNK_OVERLAP", "50"))
    ES_REQUEST_TIMEOUT_SECONDS = int(os.getenv("ES_REQUEST_TIMEOUT_SECONDS", "30"))
    ES_ENABLE_VECTORS = os.getenv("ES_ENABLE_VECTORS", "true").lower() == "true"
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
    # Master-circular splitter's own embedding model. Used inside
    # semantic_sub_chunking for adjacent-sentence cosine similarity
    # (topic-shift detection), NOT for ES chunk or query embeddings.
    # Defaults to a small, fast model. Set to the same value as
    # ES_EMBEDDING_MODEL_NAME if you want to share one model load.
    ES_MASTER_SPLITTER_PROVIDER = os.getenv(
        "ES_MASTER_SPLITTER_PROVIDER", "sentence-transformers"
    ).strip().lower()
    ES_MASTER_SPLITTER_MODEL = os.getenv(
        "ES_MASTER_SPLITTER_MODEL", "all-MiniLM-L6-v2"
    ).strip()
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
        os.getenv("ES_CONTEXTUAL_MAX_TOKENS", "600")
    )
    SUMMARIZATION_MODEL = os.getenv("SUMMARIZATION_MODEL", "minimaxai/minimax-m2.7")
    ES_RRF_WINDOW_SIZE = int(os.getenv("ES_RRF_WINDOW_SIZE", "100"))
    ES_KNN_NUM_CANDIDATES = int(os.getenv("ES_KNN_NUM_CANDIDATES", "1000"))
    ES_RRF_RANK_CONSTANT = int(os.getenv("ES_RRF_RANK_CONSTANT", "60"))
    RAG_MODEL = os.getenv("RAG_MODEL", "minimaxai/minimax-m2.7")
    RAG_MAX_CHUNKS = int(os.getenv("RAG_MAX_CHUNKS", "10"))
    RAG_MAX_TOKENS = int(os.getenv("RAG_MAX_TOKENS", "4000"))
    LLM_PROVIDER = os.getenv("LLM_PROVIDER", "nvidia").strip().lower()
    OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY", "")
    OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
    AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "my-app-dev-bucket")
    AWS_S3_REGION = os.getenv("AWS_S3_REGION", "us-east-1")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    AWS_S3_ENDPOINT_URL = os.getenv("AWS_S3_ENDPOINT_URL", "http://localhost:4566")

    # SMTP / Email settings
    SMTP_HOST = os.getenv("SMTP_HOST", "smtp.company.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
    SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
    SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", "circulars@company.com")
    SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME", "Regulatory Circular System")
    NOTIFICATION_RECIPIENTS = [
        r.strip() for r in os.getenv("NOTIFICATION_RECIPIENTS", "").split(",") if r.strip()
    ]
