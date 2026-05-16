from __future__ import annotations

from contextlib import contextmanager
from threading import Lock
from typing import TYPE_CHECKING, Any

from config import Config

if TYPE_CHECKING:
    import oracledb


class _ConnectionWrapper:
    """Wrapper around oracledb.Connection that provides conn.execute() via cursor."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def execute(self, sql: str, params: Any = None) -> Any:
        """Execute SQL via a fresh cursor per call."""
        cursor = self._conn.cursor()
        if params is not None:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        return False


class _PoolWrapper:
    """Wrapper around oracledb.ConnectionPool that provides acquire_connection()."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @contextmanager
    def acquire_connection(self):
        """Context manager that yields a connection wrapper with execute()."""
        conn = self._pool.acquire()
        try:
            yield _ConnectionWrapper(conn)
        finally:
            self._pool.release(conn)

    def close(self) -> None:
        self._pool.close()


class DatabaseClient:
    """Singleton wrapper around the Oracle connection pool."""

    _instance: DatabaseClient | None = None
    _instance_lock = Lock()

    def __new__(cls) -> DatabaseClient:
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._pool = None
        return cls._instance

    @property
    def pool(self) -> Any:
        """Return the underlying connection pool."""
        if self._pool is None:
            self._pool = self._create_pool()
        return self._pool

    def get_pool(self) -> Any:
        """Return a wrapper around the pool that provides acquire_connection()."""
        if self._pool is None:
            self._pool = self._create_pool()
        return _PoolWrapper(self._pool)

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None

    def _create_pool(self) -> Any:
        try:
            import oracledb
        except ImportError as exc:
            raise RuntimeError(
                "Oracle client is not installed. Install dependencies from "
                "`requirements.txt` before requesting the db client."
            ) from exc

        pool = oracledb.create_pool(
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            dsn=Config.DB_DSN,
            min=Config.DB_MIN_SIZE,
            max=Config.DB_MAX_SIZE,
        )
        return pool


def get_db_client() -> DatabaseClient:
    """Return the singleton DatabaseClient instance."""
    return DatabaseClient()