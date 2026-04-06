from __future__ import annotations

import logging


DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def configure_logging(level: str = "INFO") -> None:
    root_logger = logging.getLogger()
    normalized_level = getattr(logging, level.upper(), logging.INFO)

    if root_logger.handlers:
        root_logger.setLevel(normalized_level)
        for handler in root_logger.handlers:
            handler.setLevel(normalized_level)
        return

    logging.basicConfig(level=normalized_level, format=DEFAULT_LOG_FORMAT)
