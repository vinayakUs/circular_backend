from __future__ import annotations

from datetime import date
from typing import Any


class CheckpointRepository:

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("CheckpointRepository requires db_pool")
        self.logger = __import__("logging").getLogger(__name__)
        self.db_pool = db_pool

    def get_checkpoint(self, source: str) -> date | None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT last_run_date FROM scraper_checkpoints WHERE source = %s",
                (source.upper(),),
            )
            row = cursor.fetchone()
        return row[0] if row else None

    def set_checkpoint(self, source: str, run_date: date) -> None:
        source_name = source.upper()
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO scraper_checkpoints (source, last_run_date)
                VALUES (%s, %s)
                ON CONFLICT (source) DO UPDATE SET
                    last_run_date = EXCLUDED.last_run_date,
                    updated_at = NOW()
                """,
                (source_name, run_date),
            )
            conn.commit()
        self.logger.info("Checkpoint updated source=%s last_run_date=%s", source_name, run_date)

    def reset_bloom_state(self) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE scraper_checkpoints SET es_bloom_filter = NULL, es_last_run_at = NULL, es_records_processed = 0, updated_at = NOW()"
            )
            conn.commit()
        self.logger.info("Reset bloom/checkpoint state for all sources")