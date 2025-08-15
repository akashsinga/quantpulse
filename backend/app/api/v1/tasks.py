# backend/app/api/v1/tasks.py

from typing import List, Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.api.dependencies import get_current_superuser, get_pagination_params
from app.services.task_service import TaskService
from app.schemas.base import APIResponse, PaginatedResponse
from app.schemas.tasks import TaskRunResponse, TaskRunDetailResponse, TaskLogResponse, TaskStepResponse, TaskRetryRequest, TaskCancelRequest, TaskFilters
from app.utils.enum import TaskStatus, TaskType
from app.utils.logger import get_logger
from app.core.exceptions import NotFoundError, to_http_exception

router = APIRouter()
logger = get_logger(__name__)


@router.get("", response_model=PaginatedResponse[TaskRunResponse])
async def get_tasks(
    skip: int = 0,
    limit: int = 50,
    # Filtering options
    status: Optional[TaskStatus] = Query(None, description="Filter by task status"),
    task_type: Optional[TaskType] = Query(None, description="Filter by task type"),
    user_id: Optional[UUID] = Query(None, description="Filter by user ID"),
    task_name: Optional[str] = Query(None, description="Filter by task name"),
    # Date range filtering
    created_after: Optional[str] = Query(None, description="Filter tasks created after this date (ISO format)"),
    created_before: Optional[str] = Query(None, description="Filter tasks created before this date (ISO format)"),
    # Sorting
    sort_by: str = Query("created_at", description="Sort field"),
    sort_order: str = Query("desc", description="Sort order (asc/desc)"),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_superuser)):
    """Get paginated list of tasks with filtering and sorting options."""
    try:
        task_service = TaskService(db)

        # Build filters
        filters = TaskFilters(status=status, task_type=task_type, user_id=user_id, task_name=task_name, created_after=created_after, created_before=created_before, sort_by=sort_by, sort_order=sort_order)

        # Get tasks with pagination
        tasks, total_count = task_service.get_tasks_paginated(skip=skip, limit=limit, filters=filters)

        # Convert to response format
        task_responses = [TaskRunResponse.model_validate(task) for task in tasks]

        # Create pagination metadata
        from app.schemas.base import PaginationMeta
        pagination = PaginationMeta.create(total=total_count, page=(skip // limit) + 1, per_page=limit)

        return PaginatedResponse(data=task_responses, pagination=pagination, message=f"Retrieved {len(task_responses)} tasks")

    except Exception as e:
        logger.error(f"Error retrieving tasks: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve tasks")


@router.get("/{task_id}", response_model=APIResponse[TaskRunDetailResponse])
async def get_task_details(task_id: UUID, db: Session = Depends(get_db), current_user=Depends(get_current_superuser)):
    """Get detailed information about a specific task including steps and logs."""
    try:
        task_service = TaskService(db)

        # Get task details
        task_details = task_service.get_task_details(task_id)

        if not task_details:
            raise NotFoundError("Task", str(task_id))

        return APIResponse(data=TaskRunDetailResponse.model_validate(task_details), message="Task details retrieved successfully")

    except NotFoundError as e:
        raise to_http_exception(e)
    except Exception as e:
        logger.error(f"Error retrieving task details {task_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve task details")


@router.get("/{task_id}/logs", response_model=APIResponse[List[TaskLogResponse]])
async def get_task_logs(task_id: UUID, skip: int = 0, limit: int = 100, log_level: Optional[str] = Query(None, description="Filter by log level"), db: Session = Depends(get_db), current_user=Depends(get_current_superuser)):
    """Get logs for a specific task."""
    try:
        task_service = TaskService(db)

        # Verify task exists
        task = task_service.get_task_by_id(task_id)
        if not task:
            raise NotFoundError("Task", str(task_id))

        # Get task logs
        logs = task_service.get_task_logs(task_id=task_id, skip=skip, limit=limit, log_level=log_level)

        log_responses = [TaskLogResponse.from_orm(log) for log in logs]

        return APIResponse(data=log_responses, message=f"Retrieved {len(log_responses)} log entries")

    except NotFoundError as e:
        raise to_http_exception(e)
    except Exception as e:
        logger.error(f"Error retrieving task logs {task_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve task logs")


@router.get("/{task_id}/steps", response_model=APIResponse[List[TaskStepResponse]])
async def get_task_steps(task_id: UUID, db: Session = Depends(get_db), current_user=Depends(get_current_superuser)):
    """Get execution steps for a specific task."""
    try:
        task_service = TaskService(db)

        # Verify task exists
        task = task_service.get_task_by_id(task_id)
        if not task:
            raise NotFoundError("Task", str(task_id))

        # Get task steps
        steps = task_service.get_task_steps(task_id)

        step_responses = [TaskStepResponse.model_validate(step) for step in steps]

        return APIResponse(data=step_responses, message=f"Retrieved {len(step_responses)} task steps")

    except NotFoundError as e:
        raise to_http_exception(e)
    except Exception as e:
        logger.error(f"Error retrieving task steps {task_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve task steps")


@router.post("/{task_id}/retry", response_model=APIResponse[TaskRunResponse])
async def retry_task(task_id: UUID, retry_request: TaskRetryRequest, db: Session = Depends(get_db), current_user=Depends(get_current_superuser)):
    """Retry a failed or cancelled task."""
    try:
        task_service = task_service(db)

        # Retry the task
        new_task = task_service.retry_task(task_id=task_id, user_id=current_user.id, reason=retry_request.reason)

        return APIResponse(data=TaskRunResponse.model_validate(new_task), message="Task retry initiated successfully")

    except NotFoundError as e:
        raise to_http_exception(e)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Error retrying task {task_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retry task")


@router.post("/{task_id}/cancel", response_model=APIResponse[TaskRunResponse])
async def cancel_task(task_id: UUID, cancel_request: TaskCancelRequest, db: Session = Depends(get_db), current_user=Depends(get_current_superuser)):
    """Cancel a running task."""
    try:
        task_service = TaskService(db)

        # Cancel the task
        cancelled_task = task_service.cancel_task(task_id=task_id, user_id=current_user.id, reason=cancel_request.reason)

        return APIResponse(data=TaskRunResponse.model_validate(cancelled_task), message="Task cancelled successfully")

    except NotFoundError as e:
        raise to_http_exception(e)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Error cancelling task {task_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to cancel task")


@router.get("/stats/overview", response_model=APIResponse[dict])
async def get_task_stats(db: Session = Depends(get_db), current_user=Depends(get_current_superuser)):
    """Get task statistics overview."""
    try:
        task_service = TaskService(db)

        stats = task_service.get_task_statistics()

        return APIResponse(data=stats, message="Task statistics retrieved successfully")

    except Exception as e:
        logger.error(f"Error retrieving task statistics: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve task statistics")


@router.delete("/{task_id}", response_model=APIResponse[dict])
async def delete_task(task_id: UUID, force: bool = Query(False, description="Force delete even if task is running"), db: Session = Depends(get_db), current_user=Depends(get_current_superuser)):
    """Delete a task record (admin only - use with caution)."""
    try:
        task_service = TaskService(db)

        # Delete the task
        result = task_service.delete_task(task_id=task_id, user_id=current_user.id, force=force)

        return APIResponse(data={"deleted": True, "task_id": str(task_id)}, message="Task deleted successfully")

    except NotFoundError as e:
        raise to_http_exception(e)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Error deleting task {task_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to delete task")
