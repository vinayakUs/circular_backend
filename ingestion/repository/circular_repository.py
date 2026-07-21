from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from json import loads as json_loads
import logging
from typing import Any
from uuid import UUID


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
        self.logger.info("upsert_circular %s" , circular)
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
                "SELECT id FROM circulars WHERE source = %s AND source_item_key = %s",
                (source_upper, source_item_key),
            )
            existing = cursor.fetchone()

            if existing:
                cursor.execute(
                    """
                    UPDATE circulars
                    SET circular_id = %s, source_item_key = %s, full_reference = %s, department = %s, title = %s,
                        issue_date = %s, url = %s, pdf_url = %s,
                        content_hash = %s, status = %s, detected_at = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (circular_id_upper, source_item_key, full_reference, department, title, issue_date,
                     url, pdf_url, content_hash, "DISCOVERED", detected_at,
                     existing[0]),
                )
                conn.commit()
                record_id = existing[0]
                self.logger.info("Circular upserted (update) source=%s circular_id=%s", source_upper, circular_id_upper)
                return record_id, False
            else:
                # if source_upper == "NSE":
                #     applicable_to_nse = True  # Moved to NSE scraper DTO
                cursor.execute(
                    """
                    INSERT INTO circulars (
                        source, circular_id, source_item_key, full_reference, department,
                        title, issue_date, url, pdf_url, content_hash, status, detected_at, applicable_to_nse
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (source_upper, circular_id_upper, source_item_key, full_reference,
                     department, title, issue_date, url, pdf_url, content_hash,
                     "DISCOVERED", detected_at, applicable_to_nse),
                )
                record_id = cursor.fetchone()[0]
                conn.commit()
                self.logger.info("Circular upserted (insert) source=%s circular_id=%s", source_upper, circular_id_upper)
                return record_id, True

    def get_record(self, source: str, source_item_key: str) -> CircularRecord | None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, effective_date, url, pdf_url,
                       content_hash, status, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name, applicable_to_nse
                FROM circulars
                WHERE source = %s AND source_item_key = %s
                """,
                (source.upper(), source_item_key),
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
                       es_index_name, applicable_to_nse
                FROM circulars
                WHERE id = %s
                """,
                (str(record_id),),
            )
            return self._row_to_record(cursor.fetchone())

    def get_records_by_ids(self, record_ids: list[UUID]) -> list[CircularRecord]:
        """Batch fetch circulars by id. Empty input returns empty list."""
        if not record_ids:
            return []
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, effective_date, url, pdf_url,
                       content_hash, status, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name, applicable_to_nse
                FROM circulars
                WHERE id = ANY(%s::uuid[])
                """,
                ([str(rid) for rid in record_ids],),
            )
            rows = cursor.fetchall()
        return [r for row in rows if (r := self._row_to_record(row))]

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
                           es_index_name, applicable_to_nse
                    FROM circulars
                    WHERE circular_id = %s AND source = %s
                    ORDER BY issue_date DESC, updated_at DESC, created_at DESC
                    LIMIT 1
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
                           es_index_name, applicable_to_nse
                    FROM circulars
                    WHERE circular_id = %s
                    ORDER BY issue_date DESC, updated_at DESC, created_at DESC
                    LIMIT 1
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
                       es_index_name, applicable_to_nse
                FROM circulars
                WHERE UPPER(full_reference) = %s
                LIMIT 1
                """,
                (reference.upper(),),
            )
            return self._row_to_record(cursor.fetchone())

    LOOKUP_FIELDS = ("circular_id", "title", "full_reference")
    # Index in the SELECT list: id=0, circular_id=1, title=2, full_reference=3
    _LOOKUP_COL_IDX = {"circular_id": 1, "title": 2, "full_reference": 3}

    def lookup(
        self,
        query: str,
        field: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """ILIKE substring lookup on a single column.

        `field` must be one of LOOKUP_FIELDS. Only rows with status='FETCHED'
        are considered. The returned dict tags the column that matched
        (always `field`) and the matched value.
        """
        if field not in self.LOOKUP_FIELDS:
            raise ValueError(
                f"Invalid field {field!r}. Must be one of {self.LOOKUP_FIELDS}."
            )

        pattern = f"%{query}%"
        col_idx = self._LOOKUP_COL_IDX[field]

        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT id, circular_id, title, full_reference
                FROM circulars
                WHERE status = 'FETCHED'
                  AND {field} ILIKE %s
                ORDER BY issue_date DESC, id DESC
                LIMIT %s
                """,
                (pattern, limit),
            )
            rows = cursor.fetchall()

        return [
            {
                "id": str(row[0]),
                "matched_field": field,
                "matched_value": row[col_idx] or "",
            }
            for row in rows
        ]

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
                           es_index_name, applicable_to_nse
                    FROM circulars
                    WHERE source = %s
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
                           es_index_name, applicable_to_nse
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
        circular_nos: list[str] | None = None,
        department: str | None = None,
    ) -> tuple[list[CircularRecord], int]:
        args: list = []
        where: list[str] = []
        idx = 1
        join_sql = ""

        if source:
            where.append(f"c.source = %s")
            args.append(source.upper())
            idx += 1
        if from_date:
            where.append(f"c.issue_date >= %s")
            args.append(from_date)
            idx += 1
        if to_date:
            where.append(f"c.issue_date <= %s")
            args.append(to_date)
            idx += 1
        if applicable_to_nse is not None:
            where.append(f"c.applicable_to_nse = %s")
            args.append(applicable_to_nse)
            idx += 1
        if signatory:
            join_sql = " INNER JOIN circular_signatories cs ON cs.circular_id = c.id"
            where.append(f"cs.signatory_name = ANY(%s)")
            args.append(signatory)
            idx += 1
        if circular_nos:
            where.append("c.id = ANY(%s::uuid[])")
            args.append(circular_nos)
            idx += 1
        if department:
            where.append("c.department = %s")
            args.append(department.strip())
            idx += 1

        where_sql = " AND ".join(where) if where else "1=1"

        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT COUNT(DISTINCT c.id) FROM circulars c{join_sql} WHERE {where_sql}
                """,
                args,
            )
            count_row = cursor.fetchone()
            total = count_row[0] if count_row else 0

            cursor.execute(
                f"""
                SELECT c.id, c.source, c.circular_id, c.source_item_key, c.full_reference,
                       c.department, c.title, c.issue_date, c.effective_date, c.url, c.pdf_url,
                       c.content_hash, c.status, c.error_message, c.detected_at,
                       c.created_at, c.updated_at, c.es_indexed_at, c.es_chunk_count,
                       c.es_index_name, c.applicable_to_nse,
                       COALESCE(json_agg(
                           json_build_object('name', cs.signatory_name, 'designation', cs.signatory_designation)
                       ) FILTER (WHERE cs.id IS NOT NULL), '[]') AS signatories
                FROM circulars c
                LEFT JOIN circular_signatories cs ON cs.circular_id = c.id
                WHERE {where_sql}
                GROUP BY c.id
                ORDER BY c.issue_date DESC, c.created_at DESC, c.id DESC
                LIMIT %s OFFSET %s
                """,
                [*args, limit, offset],
            )
            rows = cursor.fetchall()
            records = [self._row_to_record(row) for row in rows]

        return records, total

    def list_distinct_departments(self, source: str | None = None) -> list[str]:
        """Return sorted distinct non-empty department names, optionally scoped to one source.

        Used by the /api/circulars/departments dropdown endpoint so the UI can
        show only departments that actually exist on circulars (and, when
        requested, only those within a given exchange).
        """
        args: list = []
        where = ["department IS NOT NULL", "TRIM(department) <> ''"]
        if source:
            where.append("source = %s")
            args.append(source.upper())
        where_sql = " AND ".join(where)
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT DISTINCT TRIM(department) AS d
                FROM circulars
                WHERE {where_sql}
                ORDER BY d
                """,
                args,
            )
            return [row[0] for row in cursor.fetchall()]

    def _get_signatories_map(self, conn: Any, circular_ids: list[UUID]) -> dict[UUID, list]:
        """Get signatories for multiple circulars using Postgres json_agg."""
        if not circular_ids:
            return {}
        cursor = conn.cursor()
        placeholders = ",".join(["%s"] * len(circular_ids))
        cursor.execute(
            f"""
            SELECT circular_id, json_agg(json_build_object('name', signatory_name, 'designation', signatory_designation))
            FROM circular_signatories
            WHERE circular_id IN ({placeholders})
            GROUP BY circular_id
            """,
            [str(cid) for cid in circular_ids],
        )
        result = {}
        for row in cursor.fetchall():
            circ_id = row[0]
            sigs = row[1] if row[1] else []
            result[circ_id] = sigs
        return result

    def update_status(self, record_id: UUID, status: str, error_message: str | None = None) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE circulars
                SET status = %s, error_message = %s, updated_at = NOW()
                WHERE id = %s
                """,
                (status, error_message, str(record_id)),
            )
            conn.commit()
        self.logger.info("Circular status updated record_id=%s status=%s", record_id, status)

    def update_applicable_to_nse(self, record_id: UUID, applicable: bool) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            self.logger.info("inside update_applicable_to_nse record_id=%s applicable=%s", record_id, applicable)

            cursor.execute(
                """
                UPDATE circulars
                SET applicable_to_nse = %s, updated_at = NOW()
                WHERE id = %s
                """,
                (applicable, str(record_id)),
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
            placeholders = ",".join(["%s"] * len(normalized))
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
            id=row[0],
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
            signatory=row[21] if len(row) > 21 else [],
        )

    def clear_es_index_state(self, record_id: UUID) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE circulars
                SET es_indexed_at = NULL, es_chunk_count = NULL, es_index_name = NULL, updated_at = NOW()
                WHERE id = %s
                """,
                (str(record_id),),
            )
            conn.commit()
        self.logger.info("Cleared ES metadata for circular record_id=%s", record_id)

    def clear_all_es_index_state(self) -> None:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE circulars SET es_indexed_at = NULL, es_chunk_count = NULL, es_index_name = NULL, updated_at = NOW()"
            )
            conn.commit()
        self.logger.info("Cleared ES metadata for all circular records")

    def list_recent_fetched_circulars_for_notification(self, hours: int = 24) -> list[CircularRecord]:
        """List circulars with status FETCHED within the given time window."""
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, effective_date, url, pdf_url,
                       content_hash, status, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name, applicable_to_nse
                FROM circulars
                WHERE status = 'FETCHED'
                  AND issue_date >= CURRENT_DATE - MAKE_INTERVAL(hours => %s)
                ORDER BY issue_date ASC, created_at ASC, id ASC
                """,
                (hours,),
            )
            return [r for row in cursor.fetchall() if (r := self._row_to_record(row))]

    def list_pending_es_records(self, limit: int = 100) -> list[CircularRecord]:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, source, circular_id, source_item_key, full_reference,
                       department, title, issue_date, effective_date, url, pdf_url,
                       content_hash, status, error_message, detected_at,
                       created_at, updated_at, es_indexed_at, es_chunk_count,
                       es_index_name, applicable_to_nse
                FROM circulars
                WHERE status = 'FETCHED' AND es_indexed_at IS NULL
                ORDER BY issue_date ASC, created_at ASC, id ASC
                LIMIT %s
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
                SET es_indexed_at = NOW(), es_chunk_count = %s,
                    es_index_name = %s, updated_at = NOW()
                WHERE id = %s
                """,
                (chunk_count, index_name, str(record_id)),
            )
            conn.commit()
        self.logger.info("Circular ES metadata updated record_id=%s chunk_count=%s index_name=%s", record_id, chunk_count, index_name)