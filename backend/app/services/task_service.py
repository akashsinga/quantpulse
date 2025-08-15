# backend/app/services/task_service.py

from typing import Dict, Any, Optional, List, Tuple
from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc, asc
from datetime import datetime, timedelta

from app.repositories.tasks import TaskRunRepository
from app.models.tasks import TaskRun, TaskLog, TaskStep
from app.utils.enum import TaskStatus, TaskType
from app.core.database import get_db
from app.utils.logger import get_logger
from app.schemas.tasks import TaskFilters
from app.core.exceptions import ValidationError, NotFoundError
from celery import current_app as celery_app

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

    def get_task_by_id(self, task_id: UUID) -> Optional[Dict[str, Any]]:
        task_run = self.task_run_repo.get_by_id(task_id)
        if not task_run:
            return None
        return task_run.to_dict()

    def retry_task(self, task_id: UUID, user_id: UUID, reason: Optional[str] = None) -> TaskRun:
        # Get the original task
        original_task = self.task_run_repo.get_by_id(task_id)
        if not original_task:
            raise NotFoundError("Task", str(task_id))

        # Check if task can be retried
        if original_task.status not in [TaskStatus.FAILURE, TaskStatus.CANCELLED, TaskStatus.REVOKED]:
            raise ValidationError(f"Task with status '{original_task.status}' cannot be retried. "
                                  f"Only FAILURE, CANCELLED, or REVOKED tasks can be retried.")

        try:
            # Create new task run with same parameters
            new_task = self.task_run_repo.create_task_run(celery_task_id="", task_name=original_task.task_name, task_type=original_task.task_type, title=f"RETRY: {original_task.title}", description=f"Retried task. Original task ID: {task_id}. Reason: {reason or 'Manual retry'}", user_id=user_id, input_parameters=original_task.input_parameters or {})

            # Start the appropriate Celery task based on task type
            celery_task = self._start_celery_task(original_task.task_type, original_task.input_parameters)

            # Update with Celery task ID
            self.task_run_repo.update(new_task, {"celery_task_id": celery_task.id})

            logger.info(f"Task {task_id} retried successfully. New task ID: {new_task.id}")
            return new_task

        except Exception as e:
            logger.error(f"Error retrying task {task_id}: {e}")
            raise ValidationError(f"Failed to retry task: {str(e)}")

    def cancel_task(self, task_id: UUID, user_id: UUID, reason: Optional[str] = None) -> TaskRun:
        # Get the task
        task = self.task_run_repo.get_by_id(task_id)
        if not task:
            raise NotFoundError("Task", str(task_id))

        # Check if task can be cancelled
        if task.status not in [TaskStatus.PENDING, TaskStatus.RECEIVED, TaskStatus.STARTED, TaskStatus.PROGRESS]:
            raise ValidationError(f"Task with status '{task.status}' cannot be cancelled. "
                                  f"Only running or pending tasks can be cancelled.")

        try:
            # Revoke the Celery task
            if task.celery_task_id:
                celery_app.control.revoke(task.celery_task_id, terminate=True)
                logger.info(f"Revoked Celery task {task.celery_task_id}")

            # Update task status
            update_data = {"status": TaskStatus.CANCELLED, "completed_at": datetime.now(), "error_message": f"Cancelled by user {user_id}. Reason: {reason or 'Manual cancellation'}"}

            updated_task = self.task_run_repo.update(task, update_data)

            logger.info(f"Task {task_id} cancelled successfully")
            return updated_task

        except Exception as e:
            logger.error(f"Error cancelling task {task_id}: {e}")
            raise ValidationError(f"Failed to cancel task: {str(e)}")

    def delete_task(self, task_id: UUID, user_id: UUID, force: bool = False) -> bool:
        """
        Delete a task record.
        
        Args:
            task_id: ID of the task to delete
            user_id: ID of the user initiating the deletion
            force: Whether to force delete even if task is running
            
        Returns:
            True if deleted successfully
            
        Raises:
            NotFoundError: If task doesn't exist
            ValidationError: If task cannot be deleted
        """
        # Get the task
        task = self.task_run_repo.get_by_id(task_id)
        if not task:
            raise NotFoundError("Task", str(task_id))

        # Check if task can be deleted
        if not force and task.status in [TaskStatus.PENDING, TaskStatus.RECEIVED, TaskStatus.STARTED, TaskStatus.PROGRESS]:
            raise ValidationError(f"Cannot delete running task with status '{task.status}'. "
                                  f"Use force=True to force delete or cancel the task first.")

        try:
            # If forcing deletion of a running task, try to cancel it first
            if force and task.status in [TaskStatus.PENDING, TaskStatus.RECEIVED, TaskStatus.STARTED, TaskStatus.PROGRESS]:
                try:
                    self.cancel_task(task_id, user_id, "Cancelled before deletion")
                except Exception as e:
                    logger.warning(f"Failed to cancel task before deletion: {e}")

            # Soft delete the task
            self.task_run_repo.delete(task, soft_delete=True)

            logger.info(f"Task {task_id} deleted successfully by user {user_id}")
            return True

        except Exception as e:
            logger.error(f"Error deleting task {task_id}: {e}")
            raise ValidationError(f"Failed to delete task: {str(e)}")

    def _start_celery_task(self, task_type: TaskType, input_parameters: Dict[str, Any]):
        if task_type == TaskType.SECURITIES_IMPORT:
            from app.tasks.import_securities import import_securities_from_dhan
            return import_securities_from_dhan.delay()

        # Add other task types as they are implemented
        else:
            raise ValidationError(f"Unknown task type: {task_type}")

    def get_tasks_paginated(self, skip: int = 0, limit: int = 50, filters: Optional[TaskFilters] = None) -> Tuple[List[TaskRun], int]:
        try:
            query = self.db.query(TaskRun).filter(TaskRun.is_deleted == False)

            if filters:
                if filters.status:
                    query = query.filter(TaskRun.status == filters.status)

                if filters.task_type:
                    query = query.filter(TaskRun.task_type == filters.task_type)

                if filters.user_id:
                    query = query.filter(TaskRun.user_id == filters.user_id)

                if filters.task_name:
                    query = query.filter(TaskRun.task_name.ilike(f"%{filters.task_name}%"))

                # Date range filtering
                if filters.created_after:
                    created_after = datetime.fromisoformat(filters.created_after.replace('Z', '+00:00'))
                    query = query.filter(TaskRun.created_at >= created_after)

                if filters.created_before:
                    created_before = datetime.fromisoformat(filters.created_before.replace('Z', '+00:00'))
                    query = query.filter(TaskRun.created_at <= created_before)

            # Get total count before applying pagination
            total_count = query.count()

            # Apply sorting
            if filters and filters.sort_by:
                sort_field = getattr(TaskRun, filters.sort_by, TaskRun.created_at)
                if filters.sort_order == 'desc':
                    query = query.order_by(desc(sort_field))
                else:
                    query = query.order_by(asc(sort_field))
            else:
                query = query.order_by(desc(TaskRun.created_at))

            tasks = query.offset(skip).limit(limit).all()

            return tasks, total_count

        except Exception as e:
            logger.error(f"Error getting paginated tasks: {e}")
            raise

    def get_task_details(self, task_id: UUID) -> Optional[TaskRun]:
        try:
            task = self.task_run_repo.get_by_id(task_id)

            if not task:
                return None

            _ = task.logs.all()  # This will load logs
            _ = task.steps.all()  # This will load steps

            return task
        except Exception as e:
            logger.error(f"Error getting task details for {task_id}: {e}")
            raise

    def get_task_logs(self, task_id: UUID, skip: int = 0, limit: int = 100, log_level: Optional[str] = None) -> List[TaskLog]:
        try:
            query = self.db.query(TaskLog).filter(TaskLog.task_run_id == task_id, TaskLog.is_deleted == False)

            if log_level:
                query = query.filter(TaskLog.level.ilike(log_level))

            logs = query.order_by(desc(TaskLog.created_at)).offset(skip).limit(limit).all()
            return logs
        except Exception as e:
            logger.error(f"Error getting task logs for {task_id}: {e}")
            raise

    def get_task_steps(self, task_id: UUID) -> List[TaskStep]:
        try:
            steps = self.db.query(TaskStep).filter(TaskStep.task_run_id == task_id, TaskStep.is_deleted == False).order_by(TaskStep.step_order).all()

            return steps

        except Exception as e:
            logger.error(f"Error getting task steps for {task_id}: {e}")
            raise

    def get_running_tasks(self, task_type: Optional[TaskType] = None, limit: int = 50) -> List[TaskRun]:
        """Get currently running tasks"""
        return self.task_run_repo.get_running_tasks(task_type=task_type)[:limit]

    def get_recent_tasks(self, hours: int = 24, limit: int = 50) -> List[TaskRun]:
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)

            tasks = self.db.query(TaskRun).filter(TaskRun.created_at >= cutoff_time, TaskRun.is_deleted == False).order_by(desc(TaskRun.created_at)).limit(limit).all()

            return tasks

        except Exception as e:
            logger.error(f"Error getting recent tasks: {e}")
            raise

    def get_failed_tasks(self, days: int = 7, limit: int = 50) -> List[TaskRun]:
        try:
            cutoff_time = datetime.now() - timedelta(days=days)

            tasks = self.db.query(TaskRun).filter(TaskRun.status == TaskStatus.FAILURE, TaskRun.created_at >= cutoff_time, TaskRun.is_deleted == False).order_by(desc(TaskRun.created_at)).limit(limit).all()

            return tasks

        except Exception as e:
            logger.error(f"Error getting failed tasks: {e}")
            raise

    def get_task_statistics(self) -> Dict[str, Any]:
        try:
            # Base query for non-deleted tasks
            base_query = self.db.query(TaskRun).filter(TaskRun.is_deleted == False)

            # Total tasks
            total_tasks = base_query.count()

            # Running tasks
            running_statuses = [TaskStatus.PENDING, TaskStatus.RECEIVED, TaskStatus.STARTED, TaskStatus.PROGRESS]
            running_tasks = base_query.filter(TaskRun.status.in_(running_statuses)).count()

            # Recent tasks (last 24 hours)
            yesterday = datetime.now() - timedelta(days=1)
            recent_tasks_24h = base_query.filter(TaskRun.created_at >= yesterday).count()

            # Recent failures (last 7 days)
            week_ago = datetime.now() - timedelta(days=7)
            recent_failures_7d = base_query.filter(TaskRun.status == TaskStatus.FAILURE, TaskRun.created_at >= week_ago).count()

            # Status breakdown
            status_breakdown = {}
            for status in TaskStatus:
                count = base_query.filter(TaskRun.status == status).count()
                if count > 0:  # Only include statuses that have tasks
                    status_breakdown[status.value] = count

            # Type breakdown
            type_breakdown = {}
            for task_type in TaskType:
                count = base_query.filter(TaskRun.task_type == task_type).count()
                if count > 0:  # Only include types that have tasks
                    type_breakdown[task_type.value] = count

            # Success rate calculation
            completed_statuses = [TaskStatus.SUCCESS, TaskStatus.FAILURE]
            completed_tasks = base_query.filter(TaskRun.status.in_(completed_statuses)).count()
            successful_tasks = base_query.filter(TaskRun.status == TaskStatus.SUCCESS).count()

            success_rate = 0.0
            if completed_tasks > 0:
                success_rate = round((successful_tasks / completed_tasks) * 100, 2)

            # Currently running task details (limited to 10)
            running_task_details = []
            running_tasks_query = base_query.filter(TaskRun.status.in_(running_statuses)).order_by(desc(TaskRun.started_at)).limit(10)

            for task in running_tasks_query:
                running_task_details.append({"id": str(task.id), "title": task.title, "status": task.status.value, "progress_percentage": task.progress_percentage, "current_message": task.current_message, "started_at": task.started_at.isoformat() if task.started_at else None})

            return {"total_tasks": total_tasks, "running_tasks": running_tasks, "recent_tasks_24h": recent_tasks_24h, "recent_failures_7d": recent_failures_7d, "success_rate_percentage": success_rate, "status_breakdown": status_breakdown, "type_breakdown": type_breakdown, "running_task_details": running_task_details}

        except Exception as e:
            logger.error(f"Error calculating task statistics: {e}")
            raise

    def cleanup_old_tasks(self, days: int = 30, dry_run: bool = True) -> Dict[str, int]:
        try:
            cutoff_date = datetime.now() - timedelta(days=days)

            # Find old completed tasks
            old_tasks_query = self.db.query(TaskRun).filter(TaskRun.completed_at < cutoff_date, TaskRun.status.in_([TaskStatus.SUCCESS, TaskStatus.FAILURE, TaskStatus.CANCELLED, TaskStatus.REVOKED]), TaskRun.is_deleted == False)

            tasks_to_cleanup = old_tasks_query.count()

            if not dry_run:
                # Soft delete old tasks
                old_tasks_query.update({"is_deleted": True, "deleted_at": datetime.now()})
                self.db.commit()
                logger.info(f"Cleaned up {tasks_to_cleanup} old tasks")

            return {"tasks_identified": tasks_to_cleanup, "tasks_deleted": tasks_to_cleanup if not dry_run else 0, "cutoff_date": cutoff_date.isoformat(), "dry_run": dry_run}

        except Exception as e:
            logger.error(f"Error during task cleanup: {e}")
            self.db.rollback()
            raise
