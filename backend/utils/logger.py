# backend/utils/logger.py

import logging
import sys
import os

# ANSI escape sequences for log level coloring
LOG_COLORS = {
    "DEBUG": "\033[36m",  # Cyan
    "INFO": "\033[32m",  # Green
    "WARNING": "\033[33m",  # Yellow
    "ERROR": "\033[31m",  # Red
    "CRITICAL": "\033[41m",  # Red background
    "RESET": "\033[0m",  # Reset color
}


class ColoredFormatter(logging.Formatter):
    def format(self, record):
        # Get original levelname
        levelname = record.levelname
        color = LOG_COLORS.get(levelname, "")
        reset = LOG_COLORS["RESET"]

        # Calculate proper padding for alignment
        # The longest level name is "CRITICAL" (8 chars)
        max_level_length = 8
        level_length = len(levelname)
        padding = " " * (max_level_length - level_length)

        # Format the level with colon right next to it
        formatted_level = f"{color}{levelname}{reset}:{padding}"

        # Store original values
        orig_levelname = record.levelname
        orig_name = record.name

        # Replace with our custom formatted versions
        record.levelname = formatted_level

        # Add request_id if it exists
        if hasattr(record, "request_id") and record.request_id != "-": # type: ignore
            record.name = f"{color}{record.name} [{record.request_id}]{reset}" # type: ignore
        else:
            record.name = f"{color}{record.name}{reset}"

        # Format the record
        result = super().format(record)

        # Restore original values
        record.levelname = orig_levelname
        record.name = orig_name

        return result


class RequestIdFilter(logging.Filter):
    """Add request_id to log records"""

    def __init__(self, name="", request_id="-"):
        super().__init__(name)
        self.request_id = request_id

    def filter(self, record):
        if not hasattr(record, "request_id"):
            record.request_id = self.request_id
        return True


def get_logger(name: str = "app", request_id: str = "-") -> logging.Logger:
    """Get a logger with optional request ID context"""
    logger = logging.getLogger(name)

    if not logger.handlers:
        level_str = os.getenv("LOG_LEVEL", "DEBUG").upper()
        level = getattr(logging, level_str, logging.DEBUG)
        logger.setLevel(level)

        # We've already added the colon to the level name, so we don't need it in the format string
        formatter = ColoredFormatter("%(levelname)s[%(asctime)s] %(name)s - %(message)s", "%Y-%m-%d %H:%M:%S")

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(level)

        # Add filter to include request_id
        request_filter = RequestIdFilter(request_id=request_id)
        console_handler.addFilter(request_filter)

        logger.addHandler(console_handler)

        # Prevent propagation to avoid duplicate logs
        logger.propagate = False

    return logger
