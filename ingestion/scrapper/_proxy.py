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


def get_urllib_proxy_opener(url: str | None = None):
    """Return an opener that routes requests through the configured proxy.
    If url is provided, only uses proxy if url matches PROXY_URL_PATTERNS env var.
    Returns None only if no proxy is configured at all (env vars not set)."""
    if url and not _should_use_proxy(url):
        # URL doesn't match proxy patterns - return non-proxied opener
        from urllib.request import build_opener

        return build_opener()
    proxy_dict = _get_proxy_dict()
    if proxy_dict is None:
        return None
    from urllib.request import ProxyHandler, build_opener

    proxy_handler = ProxyHandler(proxy_dict)
    return build_opener(proxy_handler)


def get_requests_proxies(url: str | None = None) -> dict[str, str] | None:
    """Return a proxies dict for requests, or None if no proxy configured.
    If url is provided, only uses proxy if url matches PROXY_URL_PATTERNS env var."""
    if url and not _should_use_proxy(url):
        return None
    return _get_proxy_dict()