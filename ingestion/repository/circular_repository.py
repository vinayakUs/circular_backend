from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from json import loads as json_loads
import logging
from typing import Any
from uuid import UUID

from ingestion.repository._uuid_utils import _raw_to_uuid, _uuid_to_raw


@dataclass(slots=True)
class CircularRecord:
    id: UUID
    source: str
    circular_id: str
    source_item_key: str
    full_reference: str
    department: str
    title: str
    issue_date: date
    effective_date: date | None
    url: str
    pdf_url: str
    content_hash: str | None
    status: str
    error_message: str | None
    detected_at: datetime
    created_at: datetime
    updated_at: datetime
    es_indexed_at: datetime | None = None
    es_chunk_count: int | None = None
    es_index_name: str | None = None
    applicable_to_nse: bool = False
    signatory: str | None = None


class CircularRepository:

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("CircularRepository requires db_pool")
        self.logger = __import__("logging").getLogger(__name__)
        self.db_pool = db_pool

    def upsert_circular(self, circular: Any) -> tuple[UUID, bool]:
        source = circular.source
        circular_id = circular.circular_id
        source_item_key = circular.source_item_key
        full_reference = circular.full_reference
        department = circular.department
        title = circular.title
        issue_date = circular.issue_date
        url = getattr(circular, 'url', '') or ''
        pdf_url = getattr(circular, 'pdf_url', '') or ''
        content_hash = getattr(circular, 'content_hash', None)
        applicable_to_nse = getattr(circular, 'applicable_to_nse', False)
        detected_at = getattr(circular, 'detected_at', None) or datetime.now(timezone.utc)

        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            source_upper = source.upper()
            circular_id_upper = circular_id.upper()

            cursor.execute(
                "SELECT id FROM circulars WHERE source = :1 AND circular_id = :2",
                (source_upper, circular_id_upper),
            )
            existing = cursor.fetchone()

            if existing:
                cursor.execute(
                    """
                    UPDATE circulars
                    SET source_item_key = :1, full_reference = :2, department = :3, title = :4,
                        issue_date = :5, url = :6, pdf_url = :7,
                        content_hash = :8, status = :9, detected_at = :10, updated_at = SYSTIMESTAMP,
                        applicable_to_nse = :11
                    WHERE id = :12
                    """,
                    (source_item_key, full_reference, department, title, issue_date,
                     url, pdf_url, content_hash, "DISCOVERED", detected_at,
                     1 if applicable_to_nse else 0, existing[0]),
                )
                conn.commit()
                record_id = _raw_to_uuid(existing[0])
                self.logger.info("Circular upserted (update) source=%s circular_id=%s", source_upper, circular_id_upper)
                return record_id, False
            else:
                if source_upper == "NSE":
                    applicable_to_nse = True
                out_id = cursor.var(bytes)
                cursor.execute(
                    """
                    INSERT INTO circulars (
                        source, circular_id, source_item_key, full_reference, department,
                        title, issue_date, url, pdf_url, content_hash, status, detected_at, applicable_to_nse
                    )
                    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)
                    RETURNING id INTO :14
                    """,
                    (source_upper, circular_id_upper, source_item_key, full_reference,
                     department, title, issue_date, url, pdf_url, content_hash,
                     "DISCOVERED", detected_at, 1 if applicable_to_nse else 0, out_id),
                )
                conn.commit()
                record_id = _raw_to_uuid(out_id.getvalue()[0])
                self.logger.info("Circular upserted (insert) source=%s circular_id=%s", source_upper, circular_id_upper)
                return record_id, True

    def get_record(self, source: str, circular_id: str) -> CircularRecord | None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, effective_date, url, pdf_url,
                       content_hash, status, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name, NVL(applicable_to_nse, 0) AS applicable_to_nse
                FROM circulars
                WHERE source = :1 AND circular_id = :2
                """,
                (source.upper(), circular_id.upper()),
            )
            return self._row_to_record(cursor.fetchone())

    def get_record_by_id(self, record_id: UUID) -> CircularRecord | None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, effective_date, url, pdf_url,
                       content_hash, status, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name, NVL(applicable_to_nse, 0) AS applicable_to_nse
                FROM circulars
                WHERE id = :1
                """,
                (_uuid_to_raw(record_id),),
            )
            return self._row_to_record(cursor.fetchone())

    def get_record_by_circular_id(self, circular_id: str, source: str | None = None) -> CircularRecord | None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            if source:
                cursor.execute(
                    """
                    SELECT id, source, circular_id, source_item_key, full_reference,
                           department, title, issue_date, effective_date, url, pdf_url,
                           content_hash, status, error_message, detected_at,
                           created_at, updated_at, es_indexed_at, es_chunk_count,
                           es_index_name, NVL(applicable_to_nse, 0) AS applicable_to_nse
                    FROM circulars
                    WHERE circular_id = :1 AND source = :2
                    ORDER BY issue_date DESC, updated_at DESC, created_at DESC
                    FETCH FIRST 1 ROWS ONLY
                    """,
                    (circular_id.upper(), source.upper()),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, source, circular_id, source_item_key, full_reference,
                           department, title, issue_date, effective_date, url, pdf_url,
                           content_hash, status, error_message, detected_at,
                           created_at, updated_at, es_indexed_at, es_chunk_count,
                           es_index_name, NVL(applicable_to_nse, 0) AS applicable_to_nse
                    FROM circulars
                    WHERE circular_id = :1
                    ORDER BY issue_date DESC, updated_at DESC, created_at DESC
                    FETCH FIRST 1 ROWS ONLY
                    """,
                    (circular_id.upper(),),
                )
            return self._row_to_record(cursor.fetchone())

    def get_record_by_full_reference(self, reference: str) -> CircularRecord | None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, effective_date, url, pdf_url,
                       content_hash, status, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name, NVL(applicable_to_nse, 0) AS applicable_to_nse
                FROM circulars
                WHERE UPPER(full_reference) = :1
                FETCH FIRST 1 ROWS ONLY
                """,
                (reference.upper(),),
            )
            return self._row_to_record(cursor.fetchone())

    def list_records(self, source: str | None = None) -> list[CircularRecord]:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            if source:
                cursor.execute(
                    """
                    SELECT id, source, circular_id, source_item_key, full_reference,
                           department, title, issue_date, effective_date, url, pdf_url,
                           content_hash, status, error_message, detected_at,
                           created_at, updated_at, es_indexed_at, es_chunk_count,
                           es_index_name, NVL(applicable_to_nse, 0) AS applicable_to_nse
                    FROM circulars
                    WHERE source = :1
                    ORDER BY source, circular_id
                    """,
                    (source.upper(),),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, source, circular_id, source_item_key, full_reference,
                           department, title, issue_date, effective_date, url, pdf_url,
                           content_hash, status, error_message, detected_at,
                           created_at, updated_at, es_indexed_at, es_chunk_count,
                           es_index_name, NVL(applicable_to_nse, 0) AS applicable_to_nse
                    FROM circulars
                    ORDER BY source, circular_id
                    """
                )
            return [r for row in cursor.fetchall() if (r := self._row_to_record(row))]

    def list_paginated(
        self,
        limit: int = 20,
        offset: int = 0,
        source: str | None = None,
        from_date: date | None = None,
        to_date: date | None = None,
        applicable_to_nse: bool | None = None,
        signatory: str | None = None,
    ) -> tuple[list[CircularRecord], int]:
        args: list = []
        where: list[str] = []
        idx = 1
        join_sql = ""

        if source:
            where.append(f"c.source = :{idx}")
            args.append(source.upper())
            idx += 1
        if from_date:
            where.append(f"c.issue_date >= :{idx}")
            args.append(from_date)
            idx += 1
        if to_date:
            where.append(f"c.issue_date <= :{idx}")
            args.append(to_date)
            idx += 1
        if applicable_to_nse is not None:
            where.append(f"c.applicable_to_nse = :{idx}")
            args.append(1 if applicable_to_nse else 0)
            idx += 1
        if signatory:
            join_sql = (
                " INNER JOIN circular_signatories cs ON cs.circular_id = c.id"
            )
            where.append(f"cs.signatory_name = :{idx}")
            args.append(signatory)
            idx += 1

        where_sql = " AND ".join(where) if where else "1=1"

        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            count_row = cursor.execute(
                f"SELECT COUNT(DISTINCT c.id) FROM circulars c{join_sql} WHERE {where_sql}",
                args,
            ).fetchone()
            total = count_row[0] if count_row else 0

            cursor.execute(
                f"""
                SELECT * FROM (
                    SELECT c.id, c.source, c.circular_id, c.source_item_key, c.full_reference,
                           c.department, c.title, c.issue_date, c.effective_date, c.url, c.pdf_url,
                           c.content_hash, c.status, c.error_message, c.detected_at,
                           c.created_at, c.updated_at, c.es_indexed_at, c.es_chunk_count,
                           c.es_index_name, NVL(c.applicable_to_nse, 0) AS applicable_to_nse,
                           (SELECT JSON_ARRAYAGG(
                               JSON_OBJECT('name' VALUE cs.signatory_name, 'designation' VALUE cs.signatory_designation)
                               RETURNING VARCHAR2(4000)
                           )
                           FROM circular_signatories cs WHERE cs.circular_id = c.id) AS signatory_json,
                           ROW_NUMBER() OVER (ORDER BY c.issue_date DESC, c.created_at DESC, c.id DESC) AS rn
                    FROM circulars c{join_sql}
                    WHERE {where_sql}
                )
                WHERE rn > :offset AND rn <= :limit
                """,
                [*args, offset, offset + limit],
            )
            rows = cursor.fetchall()
            records = [self._row_to_record(row) for row in rows]

        return records, total

    def update_status(self, record_id: UUID, status: str, error_message: str | None = None) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE circulars
                SET status = :1, error_message = :2, updated_at = SYSTIMESTAMP
                WHERE id = :3
                """,
                (status, error_message, _uuid_to_raw(record_id)),
            )
            conn.commit()
        self.logger.info("Circular status updated record_id=%s status=%s", record_id, status)

    def update_applicable_to_nse(self, record_id: UUID, applicable: bool) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE circulars
                SET applicable_to_nse = :1, updated_at = SYSTIMESTAMP
                WHERE id = :2
                """,
                (1 if applicable else 0, _uuid_to_raw(record_id)),
            )
            conn.commit()
        self.logger.info("Updated applicable_to_nse record_id=%s applicable=%s", record_id, applicable)

    def get_source_counts(self, sources: list[str] | tuple[str, ...]) -> dict[str, int]:
        if not sources:
            return {}
        normalized = [s.upper() for s in sources]
        counts = {s: 0 for s in normalized}
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            placeholders = ",".join([f":{i+1}" for i in range(len(normalized))])
            cursor.execute(
                f"SELECT source, COUNT(*) FROM circulars WHERE source IN ({placeholders}) GROUP BY source",
                normalized,
            )
            for row in cursor.fetchall():
                counts[row[0]] = row[1]
        return counts

    def exists_by_source_and_id(self, source: str, circular_id: str) -> bool:
        return self.get_record(source, circular_id) is not None

    def _row_to_record(self, row: Any) -> CircularRecord | None:
        if row is None:
            return None

        return CircularRecord(
            id=_raw_to_uuid(row[0]),
            source=row[1],
            circular_id=row[2],
            source_item_key=row[3] or "",
            full_reference=row[4],
            department=row[5] or "",
            title=row[6],
            issue_date=row[7],
            effective_date=row[8],
            url=row[9] or "",
            pdf_url=row[10] or "",
            content_hash=row[11],
            status=row[12],
            error_message=row[13],
            detected_at=row[14],
            created_at=row[15],
            updated_at=row[16],
            es_indexed_at=row[17] if len(row) > 17 and row[17] is not None else None,
            es_chunk_count=int(row[18]) if len(row) > 18 and row[18] is not None else None,
            es_index_name=row[19] if len(row) > 19 and row[19] is not None else None,
            applicable_to_nse=bool(row[20]) if len(row) > 20 else False,
            signatory=json_loads(row[21]) if len(row) > 21 and row[21] else [],
        )

    def clear_es_index_state(self, record_id: UUID) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE circulars
                SET es_indexed_at = NULL, es_chunk_count = NULL, es_index_name = NULL, updated_at = SYSTIMESTAMP
                WHERE id = :1
                """,
                (_uuid_to_raw(record_id),),
            )
            conn.commit()
        self.logger.info("Cleared ES metadata for circular record_id=%s", record_id)

    def clear_all_es_index_state(self) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE circulars SET es_indexed_at = NULL, es_chunk_count = NULL, es_index_name = NULL, updated_at = SYSTIMESTAMP"
            )
            conn.commit()
        self.logger.info("Cleared ES metadata for all circular records")

    def list_pending_es_records(self, limit: int = 100) -> list[CircularRecord]:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, effective_date, url, pdf_url,
                       content_hash, status, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name, NVL(applicable_to_nse, 0) AS applicable_to_nse
                FROM circulars
                WHERE status = 'FETCHED' AND es_indexed_at IS NULL
                ORDER BY issue_date ASC, created_at ASC, id ASC
                FETCH FIRST :1 ROWS ONLY
                """,
                (limit,),
            )
            return [r for row in cursor.fetchall() if (r := self._row_to_record(row))]

    def mark_es_indexed(self, record_id: UUID, chunk_count: int, index_name: str) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE circulars
                SET es_indexed_at = SYSTIMESTAMP, es_chunk_count = :1,
                    es_index_name = :2, updated_at = SYSTIMESTAMP
                WHERE id = :3
                """,
                (chunk_count, index_name, _uuid_to_raw(record_id)),
            )
            conn.commit()
        self.logger.info("Circular ES metadata updated record_id=%s chunk_count=%s index_name=%s", record_id, chunk_count, index_name)