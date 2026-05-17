from __future__ import annotations

from datetime import date
from typing import Any

from ingestion.repository._uuid_utils import _raw_to_uuid


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
                "SELECT last_run_date FROM scraper_checkpoints WHERE source = :1",
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
                MERGE INTO scraper_checkpoints dst
                USING (SELECT :1 AS src, :2 AS last_run_date FROM DUAL) src
                ON (dst.source = src.src)
                WHEN MATCHED THEN UPDATE SET last_run_date = src.last_run_date, updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN INSERT (source, last_run_date) VALUES (src.src, src.last_run_date)
                """,
                (source_name, run_date),
            )
            conn.commit()
        self.logger.info("Checkpoint updated source=%s last_run_date=%s", source_name, run_date)

    def reset_bloom_state(self) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE scraper_checkpoints SET es_bloom_filter = NULL, es_last_run_at = NULL, es_records_processed = 0, updated_at = SYSTIMESTAMP"
            )
            conn.commit()
        self.logger.info("Reset bloom/checkpoint state for all sources")