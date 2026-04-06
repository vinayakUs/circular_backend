from __future__ import annotations

from threading import Lock
from typing import TYPE_CHECKING, Any

from config import Config

if TYPE_CHECKING:
    from psycopg_pool import ConnectionPool


class DatabaseClient:
    """Singleton wrapper around the PostgreSQL connection pool."""

    _instance: DatabaseClient | None = None
    _instance_lock = Lock()

    def __new__(cls) -> DatabaseClient:
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._pool = None
        return cls._instance

    def get_pool(self) -> Any:
        if self._pool is None:
            self._pool = self._create_pool()
            self._pool.open()
        return self._pool

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None

    def _create_pool(self) -> Any:
        try:
            from psycopg_pool import ConnectionPool
        except ImportError as exc:
            raise RuntimeError(
                "PostgreSQL client is not installed. Install dependencies from "
                "`requirements.txt` before requesting the db client."
            ) from exc

        return ConnectionPool(
            conninfo=Config.DATABASE_URL,
            min_size=Config.DB_MIN_SIZE,
            max_size=Config.DB_MAX_SIZE,
            kwargs={"autocommit": True},
            open=False,
        )


def get_db_client() -> DatabaseClient:
    return DatabaseClient()
