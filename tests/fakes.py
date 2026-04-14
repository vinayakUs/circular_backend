from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from ingestion.repository import CircularRecord
from ingestion.scrapper.dto import Circular


class FakeCircularRepository:
    def __init__(self) -> None:
        self._records_by_key: dict[tuple[str, str], CircularRecord] = {}
        self._records_by_source_item_key: dict[tuple[str, str], CircularRecord] = {}
        self._records_by_id: dict[UUID, CircularRecord] = {}
        self._checkpoints: dict[str, date] = {}

    def upsert_circular(self, circular: Circular) -> tuple[UUID, bool]:
        key = self._build_key(circular.source, circular.circular_id)
        source_item_key = self._normalize_source_item_key(
            circular.source_item_key or circular.url or circular.circular_id
        )
        now = datetime.now(timezone.utc)

        existing = self._records_by_source_item_key.get(
            self._build_source_item_key(circular.source, source_item_key)
        )
        if existing is None:
            existing = self._records_by_key.get(key)

        if existing is not None:
            updated = replace(
                existing,
                circular_id=circular.circular_id.upper(),
                source_item_key=source_item_key,
                full_reference=circular.full_reference,
                department=circular.department,
                title=circular.title,
                issue_date=circular.issue_date,
                effective_date=circular.effective_date,
                url=circular.url,
                pdf_url=circular.pdf_url,
                detected_at=circular.detected_at,
                updated_at=now,
            )
            self._store_record(updated)
            return updated.id, False

        record = CircularRecord(
            id=uuid4(),
            source=circular.source.upper(),
            circular_id=circular.circular_id.upper(),
            source_item_key=source_item_key,
            full_reference=circular.full_reference,
            department=circular.department,
            title=circular.title,
            issue_date=circular.issue_date,
            effective_date=circular.effective_date,
            url=circular.url,
            pdf_url=circular.pdf_url,
            status="DISCOVERED",
            file_path=None,
            content_hash=None,
            error_message=None,
            detected_at=circular.detected_at,
            created_at=now,
            updated_at=now,
            es_indexed_at=None,
            es_chunk_count=None,
            es_index_name=None,
        )
        self._store_record(record)
        return record.id, True

    def update_file_path(
        self, record_id: UUID, file_path: str, content_hash: str | None = None
    ) -> None:
        record = self._records_by_id[record_id]
        self._store_record(
            replace(
                record,
                file_path=file_path,
                content_hash=content_hash,
                updated_at=datetime.now(timezone.utc),
            )
        )

    def update_status(
        self, record_id: UUID, status: str, error_message: str | None = None
    ) -> None:
        record = self._records_by_id[record_id]
        self._store_record(
            replace(
                record,
                status=status,
                error_message=error_message,
                updated_at=datetime.now(timezone.utc),
            )
        )

    def get_checkpoint(self, source: str) -> date | None:
        return self._checkpoints.get(source.upper())

    def set_checkpoint(self, source: str, run_date: date) -> None:
        self._checkpoints[source.upper()] = run_date

    def get_record(self, source: str, circular_id: str) -> CircularRecord | None:
        return self._records_by_key.get(self._build_key(source, circular_id))

    def get_record_by_id(self, record_id: UUID) -> CircularRecord | None:
        return self._records_by_id.get(record_id)

    def list_records(self) -> list[CircularRecord]:
        return sorted(
            self._records_by_id.values(),
            key=lambda record: (record.source, record.circular_id),
        )

    def list_pending_es_records(self, limit: int = 100) -> list[CircularRecord]:
        pending = [
            record
            for record in self._records_by_id.values()
            if record.status == "FETCHED"
            and record.file_path is not None
            and record.es_indexed_at is None
        ]
        pending.sort(key=lambda record: (record.issue_date, record.created_at, record.id))
        return pending[:limit]

    def mark_es_indexed(
        self, record_id: UUID, chunk_count: int, index_name: str
    ) -> None:
        record = self._records_by_id[record_id]
        self._store_record(
            replace(
                record,
                es_indexed_at=datetime.now(timezone.utc),
                es_chunk_count=chunk_count,
                es_index_name=index_name,
                updated_at=datetime.now(timezone.utc),
            )
        )

    def clear_es_index_state(self, record_id: UUID) -> None:
        record = self._records_by_id[record_id]
        self._store_record(
            replace(
                record,
                es_indexed_at=None,
                es_chunk_count=None,
                es_index_name=None,
                updated_at=datetime.now(timezone.utc),
            )
        )

    def clear_all_es_index_state(self) -> None:
        now = datetime.now(timezone.utc)
        for record in list(self._records_by_id.values()):
            self._store_record(
                replace(
                    record,
                    es_indexed_at=None,
                    es_chunk_count=None,
                    es_index_name=None,
                    updated_at=now,
                )
            )

    def reset_bloom_state(self) -> None:
        return None

    def get_source_counts(
        self, sources: list[str] | tuple[str, ...]
    ) -> dict[str, int]:
        normalized_sources = tuple(source.upper() for source in sources)
        counts = {source: 0 for source in normalized_sources}
        for record in self._records_by_id.values():
            if record.source in counts:
                counts[record.source] += 1
        return counts

    def _build_key(self, source: str, circular_id: str) -> tuple[str, str]:
        return source.upper(), circular_id.upper()

    def _build_source_item_key(
        self, source: str, source_item_key: str
    ) -> tuple[str, str]:
        return source.upper(), self._normalize_source_item_key(source_item_key)

    def _normalize_source_item_key(self, source_item_key: str) -> str:
        return source_item_key.strip()

    def _store_record(self, record: CircularRecord) -> None:
        previous = self._records_by_id.get(record.id)
        if previous is not None:
            self._records_by_key.pop(
                self._build_key(previous.source, previous.circular_id), None
            )
            self._records_by_source_item_key.pop(
                self._build_source_item_key(previous.source, previous.source_item_key),
                None,
            )

        self._records_by_key[self._build_key(record.source, record.circular_id)] = record
        self._records_by_source_item_key[
            self._build_source_item_key(record.source, record.source_item_key)
        ] = record
        self._records_by_id[record.id] = record

