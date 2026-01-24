"""
Utility functions for Goszakup tender parser
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

from config import LOG_DIR, LOG_LEVEL, LOG_FORMAT


def setup_logging(name: str = 'goszakup_parser', level: str = LOG_LEVEL) -> logging.Logger:
    """
    Setup logging with console and file handlers

    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR)

    Returns:
        Configured logger
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    # Clear existing handlers
    logger.handlers.clear()

    # Console handler (INFO and above)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(LOG_FORMAT)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler (DEBUG and above)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = LOG_DIR / f"parser_{timestamp}.log"

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(LOG_FORMAT)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    logger.info(f"Logging initialized. Log file: {log_file}")

    return logger


def format_statistics(stats: dict) -> str:
    """
    Format statistics dictionary for display

    Args:
        stats: Statistics dictionary from Database.get_statistics()

    Returns:
        Formatted string
    """
    lines = [
        "=" * 60,
        "SCRAPING STATISTICS",
        "=" * 60,
        f"Total URLs processed: {stats.get('total_urls', 0)}",
        f"Total lot records: {stats.get('total_lots', 0)}",
        "",
        "Parse Status Breakdown:",
    ]

    parse_status = stats.get('parse_status', {})
    for status, count in parse_status.items():
        lines.append(f"  {status or 'unknown'}: {count}")

    lines.append("")
    lines.append("Progress Status Breakdown:")

    progress_status = stats.get('progress_status', {})
    for status, count in progress_status.items():
        lines.append(f"  {status}: {count}")

    lines.append("=" * 60)

    return "\n".join(lines)


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string (e.g., "2h 15m 30s")
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")

    return " ".join(parts)
