# backend/app/services/task_service.py

from typing import Dict, Any, Optional, List
from uuid import UUID
from sqlalchemy.orm import Session

from app.repositories.tasks import TaskRunRepository
from app.models.tasks import TaskRun
from app.utils.enum import TaskStatus, TaskType
from app.core.database import get_db
from app.utils.logger import get_logger

logger = get_logger(__name__)


class TaskService:
    """Service for managing task tracking and progress updates"""

    def __init__(self, db: Session = None):
        self.db = db or next(get_db())
        self.task_run_repo = TaskRunRepository(self.db)

    def create_task_run(self, celery_task_id: str, task_name: str, task_type: TaskType, title: str, description: str = None, user_id: UUID = None, input_parameters: Dict[str, Any] = None) -> TaskRun:
        """Create a new task run with tracking"""
        return self.task_run_repo.create_task_run(celery_task_id=celery_task_id, task_name=task_name, task_type=task_type, title=title, description=description, user_id=user_id, input_parameters=input_parameters)

    def get_task_status(self, task_id: UUID) -> Optional[Dict[str, Any]]:
        """Get comprehensive task status"""
        task_run = self.task_run_repo.get_by_id(task_id)
        if not task_run:
            return None
        return task_run.to_dict()

    def get_user_tasks(self, user_id: UUID, **filters) -> List[Dict[str, Any]]:
        """Get tasks for a user"""
        tasks = self.task_run_repo.get_user_tasks(user_id, **filters)
        return [task.to_dict() for task in tasks]
