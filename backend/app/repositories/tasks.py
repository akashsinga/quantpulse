# backend/app/repositories/tasks.py

from typing import List, Optional, Dict, Any
from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.repositories.base import BaseRepository
from app.models.tasks import TaskRun, TaskStep, TaskLog
from app.utils.enum import TaskStatus, TaskType
from app.utils.logger import get_logger

logger = get_logger(__name__)


class TaskRunRepository(BaseRepository[TaskRun]):
    """Repository for TaskRun operations"""

    def __init__(self, db: Session):
        super().__init__(db, TaskRun)

    def get_by_celery_id(self, celery_task_id: str) -> Optional[TaskRun]:
        """Get task run by Celery task ID"""
        return self.db.query(TaskRun).filter(TaskRun.celery_task_id == celery_task_id, TaskRun.is_deleted == False).first()

    def create_task_run(self, celery_task_id: str, task_name: str, task_type: TaskType, title: str, description: str = None, user_id: UUID = None, input_parameters: Dict[str, Any] = None) -> TaskRun:
        """Create a new task run"""
        task_run = TaskRun(celery_task_id=celery_task_id, task_name=task_name, task_type=task_type, title=title, description=description, user_id=user_id, input_parameters=input_parameters or {})
        return self.create(task_run)

    def get_user_tasks(self, user_id: UUID, skip: int = 0, limit: int = 100) -> List[TaskRun]:
        """Get tasks for a specific user"""
        return self.db.query(TaskRun).filter(TaskRun.user_id == user_id, TaskRun.is_deleted == False).order_by(desc(TaskRun.created_at)).offset(skip).limit(limit).all()

    def get_running_tasks(self, task_type: TaskType = None) -> List[TaskRun]:
        """Get all currently running tasks"""
        running_statuses = [TaskStatus.RECEIVED, TaskStatus.STARTED, TaskStatus.PROGRESS]

        query = self.db.query(TaskRun).filter(TaskRun.status.in_(running_statuses), TaskRun.is_deleted == False)

        if task_type:
            query = query.filter(TaskRun.task_type == task_type)

        return query.order_by(desc(TaskRun.started_at)).all()
