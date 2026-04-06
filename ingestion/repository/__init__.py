"""Persistence layer for ingestion workflows."""

from ingestion.repository.circular_repository import CircularRecord, CircularRepository

__all__ = ["CircularRecord", "CircularRepository"]
