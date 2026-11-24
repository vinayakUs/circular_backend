from datetime import date

from app.services.search_engine.dates import resolve_dates

TODAY = date(2026, 11, 24)  # a Tuesday


def test_relative_phrases():
    assert resolve_dates("today", TODAY) == {"from": "2026-11-24", "to": "2026-11-24"}
    assert resolve_dates("yesterday", TODAY) == {"from": "2026-11-23", "to": "2026-11-23"}
    assert resolve_dates("last week", TODAY) == {"from": "2026-11-16", "to": "2026-11-22"}
    assert resolve_dates("this week", TODAY) == {"from": "2026-11-23", "to": "2026-11-29"}
    assert resolve_dates("last month", TODAY) == {"from": "2026-10-01", "to": "2026-10-31"}
    assert resolve_dates("last 7 days", TODAY) == {"from": "2026-11-17", "to": "2026-11-24"}


def test_month_names_pick_most_recent_past_year():
    assert resolve_dates("october", TODAY) == {"from": "2026-10-01", "to": "2026-10-31"}
    assert resolve_dates("december", TODAY) == {"from": "2025-12-01", "to": "2025-12-31"}
    assert resolve_dates("May 2026", TODAY) == {"from": "2026-05-01", "to": "2026-05-31"}


def test_calendar_date_is_day_month_year():
    assert resolve_dates("12/05/2026", TODAY) == {"from": "2026-05-12", "to": "2026-05-12"}
    assert resolve_dates("15 Nov 2026", TODAY) == {"from": "2026-11-15", "to": "2026-11-15"}


def test_unresolvable_and_empty():
    assert resolve_dates(None) is None
    assert resolve_dates("whenever") is None
