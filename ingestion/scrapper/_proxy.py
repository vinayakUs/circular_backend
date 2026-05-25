from __future__ import annotations

from config import Config


def _get_proxy_dict() -> dict[str, str] | None:
    proxies: dict[str, str] = {}
    if Config.HTTP_PROXY:
        proxies["http"] = Config.HTTP_PROXY
    if Config.HTTPS_PROXY:
        proxies["https"] = Config.HTTPS_PROXY
    return proxies if proxies else None


def get_urllib_proxy_opener():
    """Return an opener that routes requests through the configured proxy."""
    from urllib.request import ProxyHandler, build_opener

    proxy_handler = ProxyHandler(_get_proxy_dict() or {})
    return build_opener(proxy_handler)


def get_requests_proxies() -> dict[str, str] | None:
    """Return a proxies dict for requests, or None if no proxy configured."""
    return _get_proxy_dict()