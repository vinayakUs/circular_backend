from __future__ import annotations

from collections.abc import Iterable
from enum import StrEnum


class ExchangeSource(StrEnum):
    """Exchange/regulator identifiers, as stored in `circulars.source`."""

    NSE = "NSE"
    SEBI = "SEBI"
    SEBI_MASTER = "SEBI_MASTER"

    @classmethod
    def parse_list(cls, values: Iterable[str]) -> list[ExchangeSource] | None:
        """Parse raw strings into sources, ignoring case and surrounding whitespace.

        Blank values are skipped. Returns None if no values remain.

        Raises:
            ValueError: If a value is not a valid source.
        """
        sources = []
        for raw in values:
            value = raw.strip().upper()
            if not value:
                continue
            try:
                sources.append(cls(value))
            except ValueError:
                allowed = ", ".join(f"'{member.value}'" for member in cls)
                raise ValueError(f"source must be {allowed}.") from None
        return sources or None