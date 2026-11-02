from __future__ import annotations

import logging

import urllib3


DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)-5s | %(name)s | %(message)s"


def configure_logging(level: str = "INFO") -> None:
    # verify=False is intentional across scrapers (NSE/SEBI CDNs have cert
    # issues); without this every PDF download prints an InsecureRequestWarning.
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    root_logger = logging.getLogger()
    normalized_level = getattr(logging, level.upper(), logging.INFO)

    if root_logger.handlers:
        root_logger.setLevel(normalized_level)
        for handler in root_logger.handlers:
            handler.setLevel(normalized_level)
        return

    logging.basicConfig(
        level=normalized_level,
        format=DEFAULT_LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def truncate(text: str, max_length: int = 80) -> str:
    if len(text) <= max_length:
        return text
    return text[: max_length - 1].rstrip() + "..."


def format_bytes(size: int | None) -> str:
    if size is None:
        return "unknown size"
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size / (1024 * 1024):.1f} MB"


def emit_block(
    logger: logging.Logger,
    level: int,
    header: str,
    fields: list[tuple[str, str]],
) -> None:
    """Emit header + aligned rows as one record; its embedded newlines print
    without the log prefix, keeping each block visually grouped."""
    if not fields:
        logger.log(level, header)
        return
    width = max(len(key) for key, _ in fields)
    lines = [header]
    lines.extend(f"  {key.ljust(width)}  {value}" for key, value in fields)
    logger.log(level, "\n".join(lines))
