"""Issuers, model names and limits for the search engine."""
from __future__ import annotations

from config import Config
from utils.exchange_source import ExchangeSource

# Sources actually stored in circulars.source. Aliases are matched case-insensitively.
ISSUER_ALIASES: dict[str, str] = {
    "sebi": "SEBI",
    "securities and exchange board of india": "SEBI",
    "securities and exchange board": "SEBI",
    "nse": "NSE",
    "national stock exchange": "NSE",
    "sebi master circular": "SEBI_MASTER",
    "sebi master circulars": "SEBI_MASTER",
    "master circular": "SEBI_MASTER",
    "master circulars": "SEBI_MASTER",
}
for _s in ExchangeSource:
    ISSUER_ALIASES.setdefault(_s.value.lower(), _s.value)

ALLOWED_ISSUERS: list[str] = sorted(set(ISSUER_ALIASES.values()))

PARSER_MODEL = Config.SEARCH_PARSER_MODEL
AGENT_MODEL = Config.SEARCH_AGENT_MODEL
SUMMARY_MODEL = Config.SEARCH_SUMMARY_MODEL
MAX_STEPS = Config.SEARCH_AGENT_MAX_STEPS
CHUNK_MIN_OVERLAP = Config.SEARCH_CHUNK_MIN_OVERLAP
