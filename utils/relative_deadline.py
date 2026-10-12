"""Compute relative-deadline labels (presentation-layer helper)."""
from __future__ import annotations

from datetime import date
from typing import Optional, Tuple


def compute_relative_deadline(
    deadline: Optional[date],
) -> Tuple[Optional[int], Optional[str]]:
    """Return (days_until, human_label) for a deadline.

    - days_until: None if no deadline; 0 if today; positive if future; negative if overdue.
    - human_label: None if no deadline; else "today", "tomorrow", "yesterday",
      "Nd ago", "in Nd", "Nw ago", "in Nw", "Nmo ago", "in Nmo".
    """
    if deadline is None:
        return None, None
    today = date.today()
    diff_days = (deadline - today).days
    if diff_days == 0:
        label = "today"
    elif diff_days == 1:
        label = "tomorrow"
    elif diff_days == -1:
        label = "yesterday"
    elif diff_days > 0:
        if diff_days < 7:
            label = f"in {diff_days}d"
        elif diff_days < 60:
            w = diff_days // 7
            label = f"in {w}w"
        else:
            m = diff_days // 30
            label = f"in {m}mo"
    else:  # diff_days < -1
        abs_d = abs(diff_days)
        if abs_d < 7:
            label = f"{abs_d}d ago"
        elif abs_d < 60:
            w = abs_d // 7
            label = f"{w}w ago"
        else:
            m = abs_d // 30
            label = f"{m}mo ago"
    return diff_days, label