# backend/app/utils/logger.py
"""
Simple logging utility using Loguru.
Provides structured logging with file rotation and basic context management.
"""

import os
import sys
from pathlib import Path
from loguru import logger


class LoguruConfig:
    """Configuration for Loguru logger"""

    def __init__(self):
        self.log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        self.log_dir = Path(os.getenv("LOG_DIR", "logs"))
        self.log_rotation = os.getenv("LOG_ROTATION", "10 MB")
        self.log_retention = os.getenv("LOG_RETENTION", "30 days")
        self.enable_file_logs = os.getenv("ENABLE_FILE_LOGS", "true").lower() == "true"

        # Create log directory
        if self.enable_file_logs:
            self.log_dir.mkdir(parents=True, exist_ok=True)

    @property
    def console_format(self) -> str:
        """Console log format"""
        return ("<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                "<level>{message}</level>")

    @property
    def file_format(self) -> str:
        """File log format"""
        return ("{time:YYYY-MM-DD HH:mm:ss} | "
                "{level: <8} | "
                "{name}:{function}:{line} | "
                "{message}")


def configure_logger():
    """Configure the global loguru logger"""
    config = LoguruConfig()

    # Remove default handler
    logger.remove()

    # Add console handler
    logger.add(sink=sys.stdout, format=config.console_format, level=config.log_level, colorize=True, backtrace=True, diagnose=True, catch=True)

    if config.enable_file_logs:
        # Add general application log file
        logger.add(sink=config.log_dir / "app.log", format=config.file_format, level="DEBUG", rotation=config.log_rotation, retention=config.log_retention, compression="zip", backtrace=True, diagnose=True, catch=True, enqueue=True)

        # Add error-only log file
        logger.add(sink=config.log_dir / "errors.log", format=config.file_format, level="ERROR", rotation=config.log_rotation, retention=config.log_retention, compression="zip", backtrace=True, diagnose=True, catch=True, enqueue=True)


def get_logger(name: str = None):
    """
    Get a logger instance with optional name binding.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Configured logger instance
    """
    if name:
        return logger.bind(name=name)
    return logger


def log_with_context(level: str, message: str, **context):
    """
    Log with additional context data.
    
    Args:
        level: Log level (debug, info, warning, error, critical)
        message: Log message
        **context: Additional context data
    """
    bound_logger = logger.bind(**context)
    getattr(bound_logger, level.lower())(message)


# Configure logger on import
configure_logger()

# Export commonly used functions
__all__ = ["logger", "get_logger", "log_with_context"]
