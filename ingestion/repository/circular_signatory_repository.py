from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID


@dataclass(slots=True)
class Signatory:
    """A signatory extracted from a circular."""
    name: str
    designation: str


@dataclass(slots=True)
class CircularSignatoryRecord:
    id: UUID
    circular_id: UUID
    signatory_name: str
    signatory_designation: str
    extracted_at: datetime


class CircularSignatoryRepository:

    def __init__(self, db_pool: Any) -> None:
        if db_pool is None:
            raise ValueError("CircularSignatoryRepository requires db_pool")
        self.logger = __import__("logging").getLogger(__name__)
        self.db_pool = db_pool

    def upsert_signatories(self, circular_id: UUID, signatories: list[Signatory]) -> list[CircularSignatoryRecord]:
        """Replace all signatories for a circular with the given list.

        DELETE + INSERT runs inside an explicit conn.transaction() block —
        if any INSERT fails, the entire operation (including the DELETE)
        rolls back so the circular never ends up half-stripped of its
        prior signatories (M9 defense-in-depth fix).
        """
        with self.db_pool.acquire() as conn:
            with conn.transaction():
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM circular_signatories WHERE circular_id = %s",
                    (str(circular_id),),
                )
                for sig in signatories:
                    cursor.execute(
                        """
                        INSERT INTO circular_signatories (
                            circular_id, signatory_name, signatory_designation
                        )
                        VALUES (%s, %s, %s)
                        """,
                        (str(circular_id), sig.name, sig.designation),
                    )
        self.logger.info(
            "Upserted %d signatories for circular_id=%s",
            len(signatories),
            circular_id,
        )
        return self.get_signatories(circular_id)

    def get_signatories(self, circular_id: UUID) -> list[CircularSignatoryRecord]:
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, circular_id, signatory_name, signatory_designation, extracted_at
                FROM circular_signatories
                WHERE circular_id = %s
                ORDER BY extracted_at ASC
                """,
                (str(circular_id),),
            )
            rows = cursor.fetchall()
        return [r for row in rows if (r := self._row_to_record(row))]

    def list_distinct_names(self) -> list[str]:
        """Return distinct signatory names, sorted alphabetically."""
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT signatory_name FROM circular_signatories ORDER BY signatory_name"
            )
            return [row[0] for row in cursor.fetchall() if row[0]]

    def get_signatories_for_circular_ids(self, circular_ids: list[UUID]) -> dict[UUID, list[CircularSignatoryRecord]]:
        """Batch fetch signatories for multiple circulars. Returns dict mapping circular_id -> signatories."""
        if not circular_ids:
            return {}
        str_ids = [str(uid) for uid in circular_ids]
        placeholders = ",".join(["%s"] * len(str_ids))
        with self.db_pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT id, circular_id, signatory_name, signatory_designation, extracted_at
                FROM circular_signatories
                WHERE circular_id IN ({placeholders})
                ORDER BY circular_id, extracted_at ASC
                """,
                str_ids,
            )
            rows = cursor.fetchall()
        result: dict[UUID, list[CircularSignatoryRecord]] = {uid: [] for uid in circular_ids}
        for row in rows:
            rec = self._row_to_record(row)
            if rec:
                result[rec.circular_id].append(rec)
        return result

    def _row_to_record(self, row: Any) -> CircularSignatoryRecord | None:
        if row is None:
            return None
        return CircularSignatoryRecord(
            id=row[0],
            circular_id=row[1],
            signatory_name=row[2],
            signatory_designation=row[3],
            extracted_at=row[4],
        )