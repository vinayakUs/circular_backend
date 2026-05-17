from __future__ import annotations

from threading import Lock
from typing import Any

from config import Config


class _PoolConn:
    """Wraps an oracledb connection so code can use conn.execute() instead of cursor.execute()."""

    __slots__ = ("_conn",)

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def execute(self, sql: str, params: Any = ()) -> Any:
        cursor = self._conn.cursor()
        cursor.execute(sql, params or ())
        return cursor

    def __enter__(self) -> "_PoolConn":
        return self

    def __exit__(self, *_: Any) -> None:
        self._conn.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


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

    def get_pool(self) -> Any:
        if self._pool is None:
            self._pool = self._create_pool()
        return self._pool

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None

    def _create_pool(self) -> Any:
        try:
            import oracledb
        except ImportError as exc:
            raise RuntimeError(
                "Oracle client is not installed. Run `pip install oracledb` "
                "before requesting the db client."
            ) from exc

        url = Config.DATABASE_URL.replace("oracle+oracledb://", "").replace("oracle://", "")
        user_pass, host_service = url.split("@")
        user, password = user_pass.split(":", 1)
        if "/" in host_service:
            host_port, service_name = host_service.rsplit("/", 1)
        else:
            host_port = host_service
            service_name = None

        if ":" in host_port:
            host, port = host_port.split(":", 1)
            port = int(port)
        else:
            host = host_port
            port = 1521

        dsn = f"{host}:{port}/{service_name}" if service_name else f"{host}:{port}"

        pool = oracledb.create_pool(
            user=user,
            password=password,
            dsn=dsn,
            min=Config.DB_MIN_SIZE,
            max=Config.DB_MAX_SIZE,
        )
        return pool


def get_db_client() -> DatabaseClient:
    return DatabaseClient()