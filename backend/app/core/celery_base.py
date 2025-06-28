# backend/app/core/celery_base.py

from typing import Any, Dict, Optional, Union
from celery import Task
from celery.exceptions import Retry, Ignore
from enum import Enum
import traceback
import time
from datetime import datetime

from app.core.celery_app import celery_app
from app.utils.enum import TaskStatus
from app.core.database import get_db
from app.utils.logger import get_logger

logger = get_logger(__name__)


class BaseTask(Task):
    """
    Base task class with common functionality for all QuantPulse tasks.
    Provides error handling, logging, progress tracking.
    """
    autoretry_for = (Exception, )
    retry_kwargs = {'max_retries': 3, 'countdown': 60}
    retry_backoff = True
    retry_backoff_max = 700
    retry_jitter = False

    def __init__(self):
        super().__init__()
        self.logger = get_logger(self.__class__.__name__)

    def on_success(self, retval: Any, task_id: str, args: tuple, kwargs: dict) -> None:
        """Success callback"""
        self.logger.info(f"Task {task_id} completed successfully")
        super().on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc: Exception, task_id: str, args: tuple, kwargs: dict, einfo) -> None:
        """Failure callback"""
        self.logger.error(f"Task {task_id} failed: {str(exc)}")
        self.logger.error(f"Traceback: {traceback.format_exc()}")
        super().on_failure(exc, task_id, args, kwargs, einfo)

    def on_retry(self, exc: Exception, task_id: str, args: tuple, kwargs: dict, einfo) -> None:
        """Retry callback"""
        self.logger.warning(f"Task {task_id} retrying due to: {str(exc)}")
        super().on_retry(exc, task_id, args, kwargs, einfo)

    def update_progress(self, current: int, total: int, message: str = "") -> None:
        """Update task progress"""
        progress = {'current': current, 'total': total, 'percentage': round((current / total) * 100, 2) if total > 0 else 0, 'message': message, 'timestamp': datetime.utcnow().isoformat()}

        self.update_state(state=TaskStatus.PROGRESS, meta=progress)

        self.logger.info(f"Progress: {progress['percentage']}% - {message}")


class DatabaseTask(BaseTask):
    """
    Task with database session management.
    Automatically handles database connections and transactions.
    """

    def __init__(self):
        super().__init__()
        self._db = None

    @property
    def db(self):
        """Get database session"""
        if self._db is None:
            from app.core.database import db_manager
            if db_manager is None:
                raise RuntimeError("Database not initialized")
            self._db = db_manager.SessionLocal()
        return self._db

    def cleanup_db(self):
        """Cleanup database session"""
        if self._db:
            try:
                self._db.close()
            except Exception as e:
                self.logger.error(f"Error closing database session: {e}")
            finally:
                self._db = None

    def on_success(self, retval: Any, task_id: str, args: tuple, kwargs: dict) -> None:
        """Success callback with DB cleanup"""
        self.cleanup_db()
        super().on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc: Exception, task_id: str, args: tuple, kwargs: dict, einfo) -> None:
        """Failure callback with DB cleanup"""
        self.cleanup_db()
        super().on_failure(exc, task_id, args, kwargs, einfo)
