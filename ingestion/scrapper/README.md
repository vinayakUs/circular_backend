# Scrapper Ingestion

This package contains the scraper scaffold described in `systemdocs/scrapper.md`.

## Structure

- `base.py`: common scraper interface
- `dto.py`: `Circular` transfer object
- `registry.py`: registry + lazy singleton factory
- `orchestrator.py`: ingestion workflow coordinator
- `sources/`: source-specific scraper implementations

## Notes

The current source scrapers are placeholders and return empty results until the
real NSE, SEBI, and BSE parsing logic is added.
