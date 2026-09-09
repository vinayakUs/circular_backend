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
    applicable_to_nse: bool = False
    is_active: bool = True
    url: str = ""
    pdf_url: str = ""
    source_item_key: str = ""
    error_message: str | None = None
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_db_row(self) -> dict:
        return {
            "source": self.source,
            "circular_id": self.circular_id,
            "full_reference": self.full_reference,
            "department": self.department,
            "title": self.title,
            "issue_date": self.issue_date,
            "applicable_to_nse": self.applicable_to_nse,
            "is_active": self.is_active,
            "url": self.url,
            "pdf_url": self.pdf_url,
            "source_item_key": self.source_item_key,
            "error_message": self.error_message,
            "detected_at": self.detected_at,
        }

    def to_json(self) -> str:
        payload = asdict(self)
        payload["issue_date"] = self.issue_date.isoformat()
        payload["applicable_to_nse"] = self.applicable_to_nse
        payload["detected_at"] = self.detected_at.isoformat()
        return json.dumps(payload)
