# backend/utils/logger.py

import os
import sys
from typing import Optional, Dict, Any
from contextvars import ContextVar
from pathlib import Path

from loguru import logger
from rich.console import Console
from rich.traceback import install as install_rich_traceback
from rich.logging import RichHandler

# Install rich traceback handling
install_rich_traceback(show_locals=True, max_frames=10)

# Context variables for request tracking
request_id_context: ContextVar[str] = ContextVar("request_id", default="-")
user_id_context: ContextVar[str] = ContextVar("user_id", default="-")

# Rich console for enhanced output
console = Console(force_terminal=True, width=120)


class LoguruConfig:
    """Configuration for Loguru logger"""

    def __init__(self):
        self.log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        self.log_dir = Path(os.getenv("LOG_DIR", "logs"))
        self.log_rotation = os.getenv("LOG_ROTATION", "500 MB")
        self.log_retention = os.getenv("LOG_RETENTION", "30 days")
        self.log_compression = os.getenv("LOG_COMPRESSION", "gz")
        self.enable_json_logs = os.getenv("ENABLE_JSON_LOGS", "false").lower() == "true"
        self.enable_file_logs = os.getenv("ENABLE_FILE_LOGS", "true").lower() == "true"

        # Create log directory
        if self.enable_file_logs:
            self.log_dir.mkdir(parents=True, exist_ok=True)

    @property
    def console_format(self) -> str:
        """Console log format with rich styling"""
        if self.enable_json_logs:
            return "{time:YYYY-MM-DD HH:mm:ss.SSS} | <level>{level: <8}</level> | <cyan>{name}</cyan> | <cyan>{extra[request_id]}</cyan> | <cyan>{extra[user_id]}</cyan> | {message}"
        return "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | " "<level>{level: <8}</level> | " "<cyan>{name: <20}</cyan> | " "<blue>[{extra[request_id]}]</blue> | " "<magenta>[{extra[user_id]}]</magenta> | " "{message}"

    @property
    def file_format(self) -> str:
        """File log format"""
        if self.enable_json_logs:
            return '{"timestamp": "{time:YYYY-MM-DD HH:mm:ss.SSS}", ' '"level": "{level}", ' '"logger": "{name}", ' '"request_id": "{extra[request_id]}", ' '"user_id": "{extra[user_id]}", ' '"message": "{message}", ' '"module": "{module}", ' '"function": "{function}", ' '"line": {line}}'
        return "{time:YYYY-MM-DD HH:mm:ss.SSS} | " "{level: <8} | " "{name: <20} | " "[{extra[request_id]}] | " "[{extra[user_id]}] | " "{module}:{function}:{line} | " "{message}"


def context_filter(record: Dict[str, Any]) -> Dict[str, Any]:
    """Add context variables to log record"""
    record["extra"]["request_id"] = request_id_context.get("-")
    record["extra"]["user_id"] = user_id_context.get("-")
    return record


def configure_logger():
    """Configure the global loguru logger"""
    config = LoguruConfig()

    # Remove default handler
    logger.remove()

    # Add console handler with Rich
    logger.add(sink=sys.stdout, format=config.console_format, level=config.log_level, filter=context_filter, colorize=True, backtrace=True, diagnose=True, catch=True)

    if config.enable_file_logs:
        # Add general application log file
        logger.add(sink=config.log_dir / "app.log", format=config.file_format, level=config.log_level, filter=context_filter, rotation=config.log_rotation, retention=config.log_retention, compression=config.log_compression, backtrace=True, diagnose=True, catch=True, serialize=config.enable_json_logs)

        # Add error-only log file
        logger.add(sink=config.log_dir / "errors.log", format=config.file_format, level="ERROR", filter=context_filter, rotation=config.log_rotation, retention=config.log_retention, compression=config.log_compression, backtrace=True, diagnose=True, catch=True, serialize=config.enable_json_logs)

        # Add API-specific log file
        logger.add(sink=config.log_dir / "api.log", format=config.file_format, level=config.log_level, filter=lambda record: "api" in record["name"].lower() or "fastapi" in record["name"].lower(), rotation=config.log_rotation, retention=config.log_retention, compression=config.log_compression, serialize=config.enable_json_logs)

        # Add database-specific log file
        logger.add(sink=config.log_dir / "database.log", format=config.file_format, level=config.log_level, filter=lambda record: any(db_term in record["name"].lower() for db_term in ["sqlalchemy", "alembic", "database", "db"]), rotation=config.log_rotation, retention=config.log_retention, compression=config.log_compression, serialize=config.enable_json_logs)


# Configure logger on import
configure_logger()


class QuantPulseLogger:
    """Enhanced logger wrapper with context management"""

    def __init__(self, name: str):
        self.name = name
        self._logger = logger.bind(name=name)

    def bind(self, **kwargs) -> "QuantPulseLogger":
        """Bind additional context to logger"""
        bound_logger = self._logger.bind(**kwargs)
        new_instance = QuantPulseLogger(self.name)
        new_instance._logger = bound_logger
        return new_instance

    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self._logger.debug(message, **kwargs)

    def info(self, message: str, **kwargs):
        """Log info message"""
        self._logger.info(message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self._logger.warning(message, **kwargs)

    def error(self, message: str, **kwargs):
        """Log error message"""
        self._logger.error(message, **kwargs)

    def critical(self, message: str, **kwargs):
        """Log critical message"""
        self._logger.critical(message, **kwargs)

    def exception(self, message: str, **kwargs):
        """Log exception with traceback"""
        self._logger.exception(message, **kwargs)

    def success(self, message: str, **kwargs):
        """Log success message (loguru specific)"""
        self._logger.success(message, **kwargs)


def get_logger(name: str = "quantpulse", **context) -> QuantPulseLogger:
    """
    Get a logger instance with optional context

    Args:
        name: Logger name (usually __name__)
        **context: Additional context to bind to logger

    Returns:
        QuantPulseLogger instance
    """
    logger_instance = QuantPulseLogger(name)

    if context:
        logger_instance = logger_instance.bind(**context)

    return logger_instance


def set_request_context(request_id: str, user_id: Optional[str] = None):
    """Set request context for logging"""
    request_id_context.set(request_id)
    if user_id:
        user_id_context.set(user_id)


def clear_request_context():
    """Clear request context"""
    request_id_context.set("-")
    user_id_context.set("-")


class LoggerContextManager:
    """Context manager for request-scoped logging"""

    def __init__(self, request_id: str, user_id: Optional[str] = None):
        self.request_id = request_id
        self.user_id = user_id
        self.old_request_id = None
        self.old_user_id = None

    def __enter__(self):
        self.old_request_id = request_id_context.get("-")
        self.old_user_id = user_id_context.get("-")
        set_request_context(self.request_id, self.user_id)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        request_id_context.set(self.old_request_id)
        user_id_context.set(self.old_user_id)


def with_request_context(request_id: str, user_id: Optional[str] = None):
    """Decorator for adding request context to functions"""

    def decorator(func):

        def wrapper(*args, **kwargs):
            with LoggerContextManager(request_id, user_id):
                return func(*args, **kwargs)

        return wrapper

    return decorator


# Performance monitoring decorators
def log_execution_time(logger_name: Optional[str] = None):
    """Decorator to log function execution time"""

    def decorator(func):

        def wrapper(*args, **kwargs):
            import time

            start_time = time.time()
            func_logger = get_logger(logger_name or func.__module__)

            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                func_logger.info(f"Function {func.__name__} executed successfully", execution_time=f"{execution_time:.4f}s", function=func.__name__)
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                func_logger.error(f"Function {func.__name__} failed", execution_time=f"{execution_time:.4f}s", function=func.__name__, error=str(e))
                raise

        return wrapper

    return decorator


def log_async_execution_time(logger_name: Optional[str] = None):
    """Decorator to log async function execution time"""

    def decorator(func):

        async def wrapper(*args, **kwargs):
            import time

            start_time = time.time()
            func_logger = get_logger(logger_name or func.__module__)

            try:
                result = await func(*args, **kwargs)
                execution_time = time.time() - start_time
                func_logger.info(f"Async function {func.__name__} executed successfully", execution_time=f"{execution_time:.4f}s", function=func.__name__)
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                func_logger.error(f"Async function {func.__name__} failed", execution_time=f"{execution_time:.4f}s", function=func.__name__, error=str(e))
                raise

        return wrapper

    return decorator


# Structured logging utilities
def log_api_request(method: str, path: str, status_code: int, duration: float, **extra):
    """Log API request with structured data"""
    api_logger = get_logger("quantpulse.api")

    log_data = {"method": method, "path": path, "status_code": status_code, "duration": f"{duration:.4f}s", **extra}

    if status_code >= 500:
        api_logger.error("API request failed", **log_data)
    elif status_code >= 400:
        api_logger.warning("API request error", **log_data)
    else:
        api_logger.info("API request completed", **log_data)


def log_database_operation(operation: str, table: str, duration: float, rows_affected: int = 0, **extra):
    """Log database operation with structured data"""
    db_logger = get_logger("quantpulse.database")

    log_data = {"operation": operation, "table": table, "duration": f"{duration:.4f}s", "rows_affected": rows_affected, **extra}

    db_logger.info("Database operation completed", **log_data)


def log_background_task(task_name: str, status: str, duration: Optional[float] = None, **extra):
    """Log background task execution"""
    task_logger = get_logger("quantpulse.tasks")

    log_data = {"task_name": task_name, "status": status, **extra}

    if duration:
        log_data["duration"] = f"{duration:.4f}s"

    if status == "failed":
        task_logger.error("Background task failed", **log_data)
    elif status == "completed":
        task_logger.success("Background task completed", **log_data)
    else:
        task_logger.info(f"Background task {status}", **log_data)


# Export commonly used functions
__all__ = ["get_logger", "set_request_context", "clear_request_context", "LoggerContextManager", "with_request_context", "log_execution_time", "log_async_execution_time", "log_api_request", "log_database_operation", "log_background_task", "console"]
