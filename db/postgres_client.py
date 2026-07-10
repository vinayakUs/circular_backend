from __future__ import annotations

from contextlib import contextmanager
from threading import Lock
from typing import Any

from config import Config


class _PoolConn:
    """Wraps a psycopg2 connection so code can use conn.execute() instead of cursor.execute()."""

    __slots__ = ("_conn", "_pool")

    def __init__(self, conn: Any, pool: Any) -> None:
        self._conn = conn
        self._pool = pool

    def execute(self, sql: str, params: Any = ()) -> Any:
        cursor = self._conn.cursor()
        cursor.execute(sql, params or ())
        return cursor

    def commit(self) -> None:
        self._conn.commit()

    @contextmanager
    def transaction(self):
        """psycopg2-compatible transaction context. Mirrors psycopg3's API."""
        try:
            yield self
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def __enter__(self) -> "_PoolConn":
        return self

    def __exit__(self, *_: Any) -> None:
        self._pool.putconn(self._conn)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


class _PoolContext:
    """Context manager for ThreadedConnectionPool that provides acquire() method."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    def acquire(self) -> _PoolConn:
        return _PoolConn(self._pool.getconn(), self._pool)

    def __enter__(self) -> "_PoolContext":
        return self

    def __exit__(self, *_: Any) -> None:
        pass


class PostgresClient:
    """Singleton wrapper around the PostgreSQL connection pool."""

    _instance: PostgresClient | None = None
    _instance_lock = Lock()

    def __new__(cls) -> PostgresClient:
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._pool = None
        return cls._instance

    def get_pool(self) -> Any:
        if self._pool is None:
            self._pool = self._create_pool()
        return _PoolContext(self._pool)

    def close(self) -> None:
        if self._pool is not None:
            self._pool.closeall()
            self._pool = None

    def reset(self) -> None:
        """Reset the pool so next get_pool() creates a fresh one."""
        self.close()

    def _create_pool(self) -> Any:
        try:
            import psycopg2
            from psycopg2 import pool
        except ImportError as exc:
            raise RuntimeError(
                "psycopg2 is not installed. Run `pip install psycopg2-binary` "
                "before requesting the postgres client."
            ) from exc

        # Parse POSTGRES_URL for postgres
        url = Config.POSTGRES_URL.replace("postgresql://", "").replace("postgres://", "")

        # URL format: user:password@host:port/dbname
        if "@" not in url:
            raise ValueError("DATABASE_URL must be in format: postgresql://user:password@host:port/dbname")

        user_pass, host_db = url.split("@", 1)
        user, password = user_pass.split(":", 1)

        if "/" in host_db:
            host_port, dbname = host_db.rsplit("/", 1)
        else:
            host_port = host_db
            dbname = "postgres"

        if ":" in host_port:
            host, port = host_port.split(":", 1)
            port = int(port)
        else:
            host = host_port
            port = 5432

        pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=Config.DB_MIN_SIZE,
            maxconn=Config.DB_MAX_SIZE,
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
        return pool


def get_postgres_client() -> PostgresClient:
    return PostgresClient()