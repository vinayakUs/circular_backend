"""Session composition root.

The only file that imports concrete session classes, mirroring
app/auth/mfa/make_services.py.
"""
from __future__ import annotations

import logging
import threading

from db.postgres_client import get_postgres_client

from app.auth.session.refresh_token_service import RefreshTokenService
from app.auth.session.repository.postgres_refresh_token_repository import (
    PostgresRefreshTokenRepository,
)


logger = logging.getLogger(__name__)


def make_refresh_token_service() -> RefreshTokenService:
    pool = get_postgres_client().get_pool()
    repo = PostgresRefreshTokenRepository(db_pool=pool)
    return RefreshTokenService(repo=repo)


_service: RefreshTokenService | None = None
_lock = threading.Lock()


def get_refresh_token_service() -> RefreshTokenService:
    """Lazy thread-safe singleton."""
    global _service
    if _service is None:
        with _lock:
            if _service is None:
                _service = make_refresh_token_service()
                logger.info("Refresh token service initialized")
    return _service
