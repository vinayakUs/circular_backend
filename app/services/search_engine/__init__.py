"""Natural-language search over circulars.

Pipeline: parse_query -> run_query -> (summarize | phrase_summarize | agent loop) -> compose.
Entry points: ``SearchEngine`` (graph.py) and the CLI (``python -m app.services.search_engine``).
"""
