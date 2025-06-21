# backend/app/utils/logger.py
"""
Simple logging utility using Loguru.
Provides structured logging with file rotation and basic context management.
"""

import sys
from loguru import logger
from pathlib import Path

from app.core.config import settings


class LoguruConfig:
    """Configuration for Loguru logger"""

    def __init__(self):
        self.log_level = settings.logging.LOG_LEVEL
        self.log_dir = Path(settings.logging.LOG_DIR)
        self.log_rotation = settings.logging.LOG_ROTATION
        self.log_retention = settings.logging.LOG_RETENTION
        self.enable_file_logs = settings.logging.ENABLE_FILE_LOGS
        self.enable_json_logs = settings.logging.ENABLE_JSON_LOGS

        # Create log directory
        if self.enable_file_logs:
            self.log_dir.mkdir(parents=True, exist_ok=True)

    @property
    def console_format(self) -> str:
        """Console log format"""
        return ("<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "{level: <8} | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                "{message}")  #

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
