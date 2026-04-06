from __future__ import annotations

from typing import Type

from ingestion.scrapper.base import IScraper


class ScraperRegistry:
    """Stores scraper classes and lazily creates singleton instances."""

    _scrapers: dict[str, Type[IScraper]] = {}
    _instances: dict[str, IScraper] = {}

    @classmethod
    def register(cls, scraper_class: Type[IScraper]) -> Type[IScraper]:
        name = scraper_class.source_name.upper()
        cls._scrapers[name] = scraper_class
        return scraper_class

    @classmethod
    def get(cls, name: str) -> IScraper:
        normalized_name = name.upper()
        if normalized_name not in cls._scrapers:
            available = ", ".join(sorted(cls._scrapers)) or "none"
            raise KeyError(
                f"Scraper '{name}' is not registered. Available scrapers: {available}"
            )

        if normalized_name not in cls._instances:
            cls._instances[normalized_name] = cls._scrapers[normalized_name]()
        return cls._instances[normalized_name]

    @classmethod
    def list_all(cls) -> list[IScraper]:
        return [cls.get(name) for name in cls.get_scraper_names()]

    @classmethod
    def get_scraper_names(cls) -> list[str]:
        return sorted(cls._scrapers)
