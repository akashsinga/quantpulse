# backend/app/tasks/import_securities.py
"""
Task for importing securities from Dhan.
Processes the details scrip master file and updates the database.
"""

from typing import Dict, Any, List

from app.core.celery_app import celery_app
from app.core.celery_base import DatabaseTask
from app.utils.logger import get_logger

logger = get_logger(__name__)


@celery_app.task(bind=True, base=DatabaseTask, name="import_securities.test_task")
def test_securities_import(self) -> Dict[str, Any]:
    """
    Test task to verify Celery is working.
    TODO: Replace with actual import logic later.
    """
    try:
        self.update_progress(0, 100, "Starting test task...")

        # Simulate some work
        import time
        for i in range(5):
            time.sleep(1)
            self.update_progress((i + 1) * 20, 100, f"Processing step {i + 1}/5...")

        result = {'status': 'success', 'message': 'Test task completed successfully', 'processed_items': 5}

        self.update_progress(100, 100, "Test task completed")
        return result

    except Exception as e:
        logger.error(f"Test task failed: {str(e)}")
        raise
