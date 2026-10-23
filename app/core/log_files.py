"""Daily-rotating log file handler with working retention."""

import re
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

LOG_BACKUP_COUNT = 5


def make_file_handler(log_file: Path) -> TimedRotatingFileHandler:
    """Rotate at midnight to ``<name>.<YYYY-MM-DD>.log`` and keep the newest few.

    ``extMatch`` must follow ``suffix``: the handler only deletes rotated files
    whose suffix matches it, so the stock pattern would keep them forever.
    """
    handler = TimedRotatingFileHandler(
        filename=log_file,
        when="midnight",
        interval=1,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.suffix = "%Y-%m-%d.log"
    handler.extMatch = re.compile(r"\d{4}-\d{2}-\d{2}\.log")
    return handler
