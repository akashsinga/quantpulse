# backend/app/core/celery_base.py

from typing import Any, Optional, Dict
from celery import Task
import traceback
from datetime import datetime, timezone

from app.utils.enum import TaskStatus
from app.core.database import init_database
from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class BaseTask(Task):
    """
    Base task class with common functionality for all QuantPulse tasks.
    Provides error handling, logging, progress tracking.
    """
    autoretry_for = ()
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
    Task with database session management and comprehensive progress tracking.
    Automatically handles database connections, transactions, and task tracking.
    """

    def __init__(self):
        super().__init__()
        self._db = None
        self._db_manager = None
        self._task_run = None
        self._current_step = None
        self._step_order = 0

    @property
    def db(self):
        """Get database session"""
        if self._db is None:
            # Initialize database manager if not already done
            if self._db_manager is None:
                self._db_manager = init_database(settings.database.DB_URL)
            self._db = self._db_manager.SessionLocal()
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

    def get_task_run(self):
        """Get or find the TaskRun record for this task"""
        if self._task_run is None:
            try:
                from app.services.task_service import TaskService
                task_id = getattr(self.request, 'id', None)

                if task_id:
                    task_service = TaskService(self.db)
                    self._task_run = task_service.task_run_repo.get_by_celery_id(task_id)
            except Exception as e:
                self.logger.warning(f"Failed to get TaskRun record: {e}")

        return self._task_run

    def create_step(self, step_name: str, title: str, status: TaskStatus = TaskStatus.PENDING, result_data: Optional[Dict] = None) -> bool:
        """
        Create a new task step
        
        Args:
            step_name: Unique identifier for the step
            title: Human-readable title for the step
            status: Initial status of the step
            result_data: Optional result data for the step
            
        Returns:
            bool: True if step was created successfully
        """
        try:
            from app.models.tasks import TaskStep

            task_run = self.get_task_run()
            if not task_run:
                self.logger.warning("No TaskRun found, cannot create step")
                return False

            self._step_order += 1

            step = TaskStep(task_run_id=task_run.id, step_name=step_name, step_order=self._step_order, title=title, status=status, result_data=result_data or {})

            self.db.add(step)
            self.db.commit()
            self.db.refresh(step)

            self._current_step = step
            self.logger.info(f"Created step: {step_name} - {title}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create step {step_name}: {e}")
            try:
                self.db.rollback()
            except:
                pass
            return False

    def update_step_status(self, step_name: str, status: TaskStatus, result_data: Optional[Dict] = None) -> bool:
        """
        Update the status of a specific step
        
        Args:
            step_name: Name of the step to update
            status: New status for the step
            result_data: Optional result data to store
            
        Returns:
            bool: True if step was updated successfully
        """
        try:
            from app.models.tasks import TaskStep

            task_run = self.get_task_run()
            if not task_run:
                return False

            step = self.db.query(TaskStep).filter(TaskStep.task_run_id == task_run.id, TaskStep.step_name == step_name).first()

            if step:
                step.status = status
                if result_data:
                    step.result_data = result_data

                self.db.commit()
                self.logger.info(f"Updated step {step_name} to status {status.value}")

                # Update current step reference if this is the current step
                if self._current_step and self._current_step.step_name == step_name:
                    self._current_step = step

                return True
            else:
                self.logger.warning(f"Step {step_name} not found for update")
                return False

        except Exception as e:
            self.logger.error(f"Failed to update step {step_name}: {e}")
            try:
                self.db.rollback()
            except:
                pass
            return False

    def log_message(self, level: str, message: str, extra_data: Optional[Dict] = None) -> bool:
        """
        Add a detailed log entry for the task
        
        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            message: Log message
            extra_data: Optional additional data to store
            
        Returns:
            bool: True if log was created successfully
        """
        try:
            from app.models.tasks import TaskLog

            task_run = self.get_task_run()
            if not task_run:
                return False

            log_entry = TaskLog(task_run_id=task_run.id, level=level.upper(), message=message, extra_data=extra_data or {})

            self.db.add(log_entry)
            self.db.commit()

            # Also log to application logger
            log_method = getattr(self.logger, level.lower(), self.logger.info)
            log_method(f"[Task Log] {message}")

            return True

        except Exception as e:
            self.logger.error(f"Failed to create log entry: {e}")
            try:
                self.db.rollback()
            except:
                pass
            return False

    def _update_progress(self, current: int, message: str, total: int = 100):
        """
        Update comprehensive progress including TaskRun, current TaskStep, and TaskLog
        
        Args:
            current: Current progress value (0-100 if total=100)
            message: Progress message
            total: Total progress value (default 100 for percentage)
        """
        try:
            # Update Celery state
            self.update_state(state='PROGRESS', meta={'current': current, 'total': total, 'message': message, 'timestamp': datetime.utcnow().isoformat()})

            # Update TaskRun record
            task_run = self.get_task_run()
            if task_run:
                from app.services.task_service import TaskService

                progress_percentage = current if total == 100 else round((current / total) * 100, 2)

                task_service = TaskService(self.db)
                task_service.task_run_repo.update(task_run, {'status': TaskStatus.PROGRESS, 'progress_percentage': progress_percentage, 'current_message': message, 'current_step': current, 'total_steps': total})

                # Update current step status if available
                if self._current_step:
                    self.update_step_status(self._current_step.step_name, TaskStatus.PROGRESS, {'progress_message': message, 'progress_percentage': progress_percentage})

                # Add log entry for significant progress updates
                if current % 10 == 0 or current == total:  # Log every 10% or at completion
                    self.log_message('INFO', f"Progress: {progress_percentage}% - {message}", {'progress_percentage': progress_percentage, 'current_step': current, 'total_steps': total})

        except Exception as e:
            self.logger.warning(f"Failed to update comprehensive progress: {e}")

    def _update_task_status(self, status: TaskStatus, **update_data):
        """
        Update TaskRun status and related fields
        
        Args:
            status: New task status
            **update_data: Additional fields to update
        """
        try:
            task_run = self.get_task_run()
            if not task_run:
                return

            from app.services.task_service import TaskService

            update_fields = {'status': status, **update_data}

            # Set completion time for terminal statuses
            if status in [TaskStatus.SUCCESS, TaskStatus.FAILURE, TaskStatus.CANCELLED, TaskStatus.REVOKED]:
                update_fields['completed_at'] = datetime.now(tz=settings.INDIA_TZ)

                # Calculate execution time if started
                if task_run.started_at:
                    execution_time = (datetime.now(tz=settings.INDIA_TZ) - task_run.started_at).total_seconds()
                    update_fields['execution_time_seconds'] = int(execution_time)

            task_service = TaskService(self.db)
            task_service.task_run_repo.update(task_run, update_fields)

            # Complete current step if task is completing
            if self._current_step and status in [TaskStatus.SUCCESS, TaskStatus.FAILURE]:
                step_status = TaskStatus.SUCCESS if status == TaskStatus.SUCCESS else TaskStatus.FAILURE
                self.update_step_status(self._current_step.step_name, step_status)

            # Add final log entry
            self.log_message('INFO' if status == TaskStatus.SUCCESS else 'ERROR', f"Task completed with status: {status.value}", {'final_status': status.value, 'update_data': update_data})

        except Exception as e:
            self.logger.warning(f"Failed to update task status: {e}")

    def start_step(self, step_name: str, title: str, message: str = "") -> bool:
        """
        Convenience method to start a new step with progress update
        
        Args:
            step_name: Unique identifier for the step
            title: Human-readable title
            message: Optional progress message
            
        Returns:
            bool: True if step was started successfully
        """
        success = self.create_step(step_name, title, TaskStatus.STARTED)
        if success and message:
            self.log_message('INFO', f"Started step: {title} - {message}")
        return success

    def complete_step(self, step_name: str, message: str = "", result_data: Optional[Dict] = None) -> bool:
        """
        Convenience method to complete a step with success status
        
        Args:
            step_name: Name of the step to complete
            message: Optional completion message
            result_data: Optional result data
            
        Returns:
            bool: True if step was completed successfully
        """
        success = self.update_step_status(step_name, TaskStatus.SUCCESS, result_data)
        if success:
            log_msg = f"Completed step: {step_name}"
            if message:
                log_msg += f" - {message}"
            self.log_message('INFO', log_msg, result_data)
        return success

    def fail_step(self, step_name: str, error_message: str, error_data: Optional[Dict] = None) -> bool:
        """
        Convenience method to mark a step as failed
        
        Args:
            step_name: Name of the step that failed
            error_message: Error description
            error_data: Optional error data
            
        Returns:
            bool: True if step was marked as failed successfully
        """
        error_result = {'error_message': error_message}
        if error_data:
            error_result.update(error_data)

        success = self.update_step_status(step_name, TaskStatus.FAILURE, error_result)
        if success:
            self.log_message('ERROR', f"Step failed: {step_name} - {error_message}", error_result)
        return success

    def on_success(self, retval: Any, task_id: str, args: tuple, kwargs: dict) -> None:
        """Success callback with DB cleanup"""
        self._update_task_status(TaskStatus.SUCCESS, result_data=retval)
        self.cleanup_db()
        super().on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc: Exception, task_id: str, args: tuple, kwargs: dict, einfo) -> None:
        """Failure callback with DB cleanup"""
        error_data = {'error_message': str(exc), 'error_traceback': traceback.format_exc()}
        self._update_task_status(TaskStatus.FAILURE, **error_data)

        # Mark current step as failed if exists
        if self._current_step:
            self.fail_step(self._current_step.step_name, str(exc), {'traceback': traceback.format_exc()})

        self.cleanup_db()
        super().on_failure(exc, task_id, args, kwargs, einfo)
