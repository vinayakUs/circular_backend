"""Shared database client exports.

The project migrated from Oracle (``db.client.DatabaseClient``) to PostgreSQL
(``db.postgres_client.PostgresClient``). All application code now imports
``get_postgres_client`` directly; the legacy names are kept here as aliases so
older test modules that still do ``from db import get_db_client`` continue to
work.
"""

from db.postgres_client import PostgresClient, get_postgres_client

# Backward-compatible aliases for the pre-PostgreSQL Oracle client.
DatabaseClient = PostgresClient
get_db_client = get_postgres_client

__all__ = [
    "DatabaseClient",
    "get_db_client",
    "PostgresClient",
    "get_postgres_client",
]
