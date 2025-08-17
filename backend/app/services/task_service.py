# backend/app/services/task_service.py

from typing import Dict, Any, Optional, List, Tuple
from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc, asc, func
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
    """Service for managing task tracking and progress updates with enhanced step and log management"""

    def __init__(self, db: Session = None):
        self.db = db or next(get_db())
        self.task_run_repo = TaskRunRepository(self.db)

    def create_task_run(self, celery_task_id: str, task_name: str, task_type: TaskType, title: str, description: str = None, user_id: UUID = None, input_parameters: Dict[str, Any] = None) -> TaskRun:
        """Create a new task run with tracking"""
        return self.task_run_repo.create_task_run(celery_task_id=celery_task_id, task_name=task_name, task_type=task_type, title=title, description=description, user_id=user_id, input_parameters=input_parameters)

    def get_task_status(self, task_id: UUID) -> Optional[Dict[str, Any]]:
        """Get comprehensive task status including steps and recent logs"""
        task_run = self.task_run_repo.get_by_id(task_id)
        if not task_run:
            return None

        # Get basic task info
        task_data = task_run.to_dict()

        # Add step summary
        steps = self.get_task_steps(task_id)
        task_data['steps_summary'] = {'total_steps': len(steps), 'completed_steps': len([s for s in steps if s.status == TaskStatus.SUCCESS]), 'failed_steps': len([s for s in steps if s.status == TaskStatus.FAILURE]), 'current_step': next((s.title for s in steps if s.status == TaskStatus.PROGRESS), None)}

        # Add recent logs count
        recent_logs = self.get_task_logs(task_id, limit=10)
        task_data['recent_logs_count'] = len(recent_logs)
        task_data['has_errors'] = any(log.level in ['ERROR', 'CRITICAL'] for log in recent_logs)

        return task_data

    def get_task_details(self, task_id: UUID) -> Optional[TaskRun]:
        """Get comprehensive task details with all related data"""
        try:
            task = self.task_run_repo.get_by_id(task_id)
            if not task:
                return None

            # Eagerly load related data
            _ = task.logs.all()  # This will load logs
            _ = task.steps.all()  # This will load steps

            return task
        except Exception as e:
            logger.error(f"Error getting task details for {task_id}: {e}")
            raise

    def get_task_steps(self, task_id: UUID) -> List[TaskStep]:
        """Get execution steps for a specific task ordered by step_order"""
        try:
            steps = self.db.query(TaskStep).filter(TaskStep.task_run_id == task_id, TaskStep.is_deleted == False).order_by(TaskStep.step_order).all()

            return steps

        except Exception as e:
            logger.error(f"Error getting task steps for {task_id}: {e}")
            raise

    def get_task_logs(self, task_id: UUID, skip: int = 0, limit: int = 100, log_level: Optional[str] = None) -> List[TaskLog]:
        """Get logs for a specific task with optional filtering"""
        try:
            query = self.db.query(TaskLog).filter(TaskLog.task_run_id == task_id, TaskLog.is_deleted == False)

            if log_level:
                query = query.filter(TaskLog.level.ilike(log_level))

            logs = query.order_by(desc(TaskLog.created_at)).offset(skip).limit(limit).all()
            return logs
        except Exception as e:
            logger.error(f"Error getting task logs for {task_id}: {e}")
            raise

    def get_task_step_details(self, task_id: UUID, step_name: str) -> Optional[TaskStep]:
        """Get details for a specific task step"""
        try:
            step = self.db.query(TaskStep).filter(TaskStep.task_run_id == task_id, TaskStep.step_name == step_name, TaskStep.is_deleted == False).first()

            return step
        except Exception as e:
            logger.error(f"Error getting step details for {task_id}.{step_name}: {e}")
            raise

    def get_step_logs(self, task_id: UUID, step_name: str, limit: int = 50) -> List[TaskLog]:
        """Get logs related to a specific step by filtering log messages"""
        try:
            # Get step details first
            step = self.get_task_step_details(task_id, step_name)
            if not step:
                return []

            # Get logs created around the step timeframe
            step_start = step.created_at
            step_end = step.updated_at if step.status in [TaskStatus.SUCCESS, TaskStatus.FAILURE] else datetime.now()

            logs = self.db.query(TaskLog).filter(TaskLog.task_run_id == task_id, TaskLog.created_at >= step_start, TaskLog.created_at <= step_end, TaskLog.is_deleted == False).order_by(TaskLog.created_at).limit(limit).all()

            return logs
        except Exception as e:
            logger.error(f"Error getting step logs for {task_id}.{step_name}: {e}")
            raise

    def get_task_progress_timeline(self, task_id: UUID) -> Dict[str, Any]:
        """Get a comprehensive timeline of task progress including steps and key logs"""
        try:
            task = self.task_run_repo.get_by_id(task_id)
            if not task:
                raise NotFoundError("Task", str(task_id))

            steps = self.get_task_steps(task_id)
            logs = self.get_task_logs(task_id, limit=200)  # Get more logs for timeline

            # Build timeline events
            timeline = []

            # Add task start event
            timeline.append({'timestamp': task.created_at, 'type': 'task_created', 'title': 'Task Created', 'description': task.title, 'status': 'info'})

            if task.started_at:
                timeline.append({'timestamp': task.started_at, 'type': 'task_started', 'title': 'Task Started', 'description': 'Task execution began', 'status': 'info'})

            # Add step events
            for step in steps:
                timeline.append({'timestamp': step.created_at, 'type': 'step_started', 'title': f'Step: {step.title}', 'description': f'Started step {step.step_order}', 'status': 'info', 'step_name': step.step_name, 'step_order': step.step_order})

                if step.status in [TaskStatus.SUCCESS, TaskStatus.FAILURE] and step.updated_at != step.created_at:
                    timeline.append({'timestamp': step.updated_at, 'type': 'step_completed', 'title': f'Step: {step.title}', 'description': f'Completed with status: {step.status.value}', 'status': 'success' if step.status == TaskStatus.SUCCESS else 'error', 'step_name': step.step_name, 'step_order': step.step_order, 'result_data': step.result_data})

            # Add significant log events (errors and major progress updates)
            for log in logs:
                if log.level in ['ERROR', 'CRITICAL']:
                    timeline.append({'timestamp': log.created_at, 'type': 'error_log', 'title': 'Error Occurred', 'description': log.message, 'status': 'error', 'log_level': log.level, 'extra_data': log.extra_data})

            # Add task completion event
            if task.completed_at:
                timeline.append({'timestamp': task.completed_at, 'type': 'task_completed', 'title': 'Task Completed', 'description': f'Task finished with status: {task.status.value}', 'status': 'success' if task.status == TaskStatus.SUCCESS else 'error'})

            # Sort timeline by timestamp
            timeline.sort(key=lambda x: x['timestamp'])

            return {'task_id': task_id, 'timeline': timeline, 'summary': {'total_events': len(timeline), 'total_steps': len(steps), 'error_count': len([event for event in timeline if event['status'] == 'error']), 'duration_seconds': ((task.completed_at - task.started_at).total_seconds() if task.completed_at and task.started_at else None)}}

        except Exception as e:
            logger.error(f"Error getting task progress timeline for {task_id}: {e}")
            raise

    def get_task_performance_metrics(self, task_id: UUID) -> Dict[str, Any]:
        """Get performance metrics for a completed task"""
        try:
            task = self.task_run_repo.get_by_id(task_id)
            if not task:
                raise NotFoundError("Task", str(task_id))

            steps = self.get_task_steps(task_id)
            logs = self.get_task_logs(task_id, limit=1000)

            metrics = {'task_id': task_id, 'execution_time_seconds': task.execution_time_seconds, 'total_steps': len(steps), 'completed_steps': len([s for s in steps if s.status == TaskStatus.SUCCESS]), 'failed_steps': len([s for s in steps if s.status == TaskStatus.FAILURE]), 'total_logs': len(logs), 'error_logs': len([l for l in logs if l.level in ['ERROR', 'CRITICAL']]), 'warning_logs': len([l for l in logs if l.level == 'WARNING']), 'retry_count': task.retry_count, 'success_rate': 0.0}

            # Calculate success rate
            total_completed = metrics['completed_steps'] + metrics['failed_steps']
            if total_completed > 0:
                metrics['success_rate'] = round((metrics['completed_steps'] / total_completed) * 100, 2)

            # Calculate step durations if we have timestamps
            step_durations = []
            for i, step in enumerate(steps):
                if step.updated_at and step.created_at:
                    duration = (step.updated_at - step.created_at).total_seconds()
                    step_durations.append({'step_name': step.step_name, 'step_title': step.title, 'duration_seconds': duration, 'step_order': step.step_order})

            metrics['step_durations'] = step_durations
            metrics['average_step_duration'] = (sum(s['duration_seconds'] for s in step_durations) / len(step_durations) if step_durations else 0)

            # Find bottleneck step (longest duration)
            if step_durations:
                bottleneck = max(step_durations, key=lambda x: x['duration_seconds'])
                metrics['bottleneck_step'] = bottleneck

            return metrics

        except Exception as e:
            logger.error(f"Error getting task performance metrics for {task_id}: {e}")
            raise

    def search_task_logs(self, task_id: UUID, search_term: str, log_level: Optional[str] = None, limit: int = 100) -> List[TaskLog]:
        """Search task logs by message content"""
        try:
            query = self.db.query(TaskLog).filter(TaskLog.task_run_id == task_id, TaskLog.message.ilike(f"%{search_term}%"), TaskLog.is_deleted == False)

            if log_level:
                query = query.filter(TaskLog.level.ilike(log_level))

            logs = query.order_by(desc(TaskLog.created_at)).limit(limit).all()
            return logs

        except Exception as e:
            logger.error(f"Error searching task logs for {task_id}: {e}")
            raise

    def export_task_data(self, task_id: UUID) -> Dict[str, Any]:
        """Export comprehensive task data for debugging or analysis"""
        try:
            task = self.get_task_details(task_id)
            if not task:
                raise NotFoundError("Task", str(task_id))

            # Convert to exportable format
            export_data = {
                'task_info': {
                    'id': str(task.id),
                    'celery_task_id': task.celery_task_id,
                    'task_name': task.task_name,
                    'task_type': task.task_type.value,
                    'title': task.title,
                    'description': task.description,
                    'status': task.status.value,
                    'created_at': task.created_at.isoformat(),
                    'started_at': task.started_at.isoformat() if task.started_at else None,
                    'completed_at': task.completed_at.isoformat() if task.completed_at else None,
                    'execution_time_seconds': task.execution_time_seconds,
                    'progress_percentage': task.progress_percentage,
                    'current_message': task.current_message,
                    'error_message': task.error_message,
                    'retry_count': task.retry_count,
                    'input_parameters': task.input_parameters,
                    'result_data': task.result_data
                },
                'steps': [{
                    'step_name': step.step_name,
                    'step_order': step.step_order,
                    'title': step.title,
                    'status': step.status.value,
                    'created_at': step.created_at.isoformat(),
                    'updated_at': step.updated_at.isoformat(),
                    'result_data': step.result_data
                } for step in task.steps],
                'logs': [{
                    'level': log.level,
                    'message': log.message,
                    'created_at': log.created_at.isoformat(),
                    'extra_data': log.extra_data
                } for log in task.logs],
                'exported_at': datetime.now().isoformat()
            }

            return export_data

        except Exception as e:
            logger.error(f"Error exporting task data for {task_id}: {e}")
            raise

    # Keep existing methods from the original implementation
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
        """Delete a task record (admin only - use with caution)"""
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

            # Soft delete the task (this will cascade to steps and logs)
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
        else:
            raise ValidationError(f"Unknown task type: {task_type}")

    # Include other existing methods from the original implementation...
    def get_tasks_paginated(self, skip: int = 0, limit: int = 50, filters: Optional[TaskFilters] = None) -> Tuple[List[TaskRun], int]:
        # Implementation remains the same as original
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
                if filters.created_after:
                    created_after = datetime.fromisoformat(filters.created_after.replace('Z', '+00:00'))
                    query = query.filter(TaskRun.created_at >= created_after)
                if filters.created_before:
                    created_before = datetime.fromisoformat(filters.created_before.replace('Z', '+00:00'))
                    query = query.filter(TaskRun.created_at <= created_before)

            total_count = query.count()

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

    def get_task_statistics(self) -> Dict[str, Any]:
        # Implementation remains the same as original with additional step/log stats
        try:
            base_query = self.db.query(TaskRun).filter(TaskRun.is_deleted == False)

            total_tasks = base_query.count()

            running_statuses = [TaskStatus.PENDING, TaskStatus.RECEIVED, TaskStatus.STARTED, TaskStatus.PROGRESS]
            running_tasks = base_query.filter(TaskRun.status.in_(running_statuses)).count()

            yesterday = datetime.now() - timedelta(days=1)
            recent_tasks_24h = base_query.filter(TaskRun.created_at >= yesterday).count()

            week_ago = datetime.now() - timedelta(days=7)
            recent_failures_7d = base_query.filter(TaskRun.status == TaskStatus.FAILURE, TaskRun.created_at >= week_ago).count()

            # Enhanced statistics with step and log data
            total_steps = self.db.query(TaskStep).filter(TaskStep.is_deleted == False).count()
            total_logs = self.db.query(TaskLog).filter(TaskLog.is_deleted == False).count()
            error_logs = self.db.query(TaskLog).filter(TaskLog.level.in_(['ERROR', 'CRITICAL']), TaskLog.is_deleted == False).count()

            # Status breakdown
            status_breakdown = {}
            for status in TaskStatus:
                count = base_query.filter(TaskRun.status == status).count()
                if count > 0:
                    status_breakdown[status.value] = count

            # Type breakdown
            type_breakdown = {}
            for task_type in TaskType:
                count = base_query.filter(TaskRun.task_type == task_type).count()
                if count > 0:
                    type_breakdown[task_type.value] = count

            # Success rate calculation
            completed_statuses = [TaskStatus.SUCCESS, TaskStatus.FAILURE]
            completed_tasks = base_query.filter(TaskRun.status.in_(completed_statuses)).count()
            successful_tasks = base_query.filter(TaskRun.status == TaskStatus.SUCCESS).count()

            success_rate = 0.0
            if completed_tasks > 0:
                success_rate = round((successful_tasks / completed_tasks) * 100, 2)

            # Currently running task details
            running_task_details = []
            running_tasks_query = base_query.filter(TaskRun.status.in_(running_statuses)).order_by(desc(TaskRun.started_at)).limit(10)

            for task in running_tasks_query:
                running_task_details.append({"id": str(task.id), "title": task.title, "status": task.status.value, "progress_percentage": task.progress_percentage, "current_message": task.current_message, "started_at": task.started_at.isoformat() if task.started_at else None})

            return {
                "total_tasks": total_tasks,
                "running_tasks": running_tasks,
                "recent_tasks_24h": recent_tasks_24h,
                "recent_failures_7d": recent_failures_7d,
                "success_rate_percentage": success_rate,
                "status_breakdown": status_breakdown,
                "type_breakdown": type_breakdown,
                "running_task_details": running_task_details,
                # Enhanced statistics
                "total_steps": total_steps,
                "total_logs": total_logs,
                "error_logs": error_logs,
                "average_steps_per_task": round(total_steps / total_tasks, 2) if total_tasks > 0 else 0,
                "average_logs_per_task": round(total_logs / total_tasks, 2) if total_tasks > 0 else 0
            }

        except Exception as e:
            logger.error(f"Error calculating task statistics: {e}")
            raise
