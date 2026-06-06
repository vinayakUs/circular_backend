from __future__ import annotations

import os


def _get_proxy_dict() -> dict[str, str] | None:
    proxies: dict[str, str] = {}
    if os.getenv("CUSTOM_HTTP_PROXY"):
        proxies["http"] = os.getenv("CUSTOM_HTTP_PROXY")
    if os.getenv("CUSTOM_HTTPS_PROXY"):
        proxies["https"] = os.getenv("CUSTOM_HTTPS_PROXY")
    return proxies if proxies else None


def _should_use_proxy(url: str) -> bool:
    """Check if URL matches any configured proxy patterns. If none set, proxy all."""
    patterns = os.getenv("PROXY_URL_PATTERNS", "")
    if not patterns:
        return True
    for pattern in patterns.split(","):
        if pattern.strip() and pattern.strip() in url:
            return True
    return False


def get_requests_proxies(url: str | None = None) -> dict[str, str] | None:
    """Return a proxies dict for requests, or None if no proxy configured.
    If url is provided, only uses proxy if url matches PROXY_URL_PATTERNS env var."""
    if url and not _should_use_proxy(url):
        return None
    return _get_proxy_dict()