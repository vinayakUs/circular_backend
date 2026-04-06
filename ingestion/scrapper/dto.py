from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
import json


@dataclass(slots=True)
class Circular:
    source: str
    circular_id: str
    full_reference: str
    department: str
    title: str
    issue_date: date
    effective_date: date | None = None
    url: str = ""
    pdf_url: str = ""
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_db_row(self) -> dict:
        return {
            "source": self.source,
            "circular_id": self.circular_id,
            "full_reference": self.full_reference,
            "department": self.department,
            "title": self.title,
            "issue_date": self.issue_date,
            "effective_date": self.effective_date,
            "url": self.url,
            "pdf_url": self.pdf_url,
            "detected_at": self.detected_at,
        }

    def to_json(self) -> str:
        payload = asdict(self)
        payload["issue_date"] = self.issue_date.isoformat()
        payload["effective_date"] = (
            self.effective_date.isoformat() if self.effective_date else None
        )
        payload["detected_at"] = self.detected_at.isoformat()
        return json.dumps(payload)
