# backend/app/api/v1/securities.py

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime

from app.tasks.securities_import import import_securities_task
from app.api.deps import get_current_superadmin
from app.db.models.user import User
from app.core.celery_app import celery_app
from app.utils.logger import get_logger

router = APIRouter()
logger = get_logger(__name__)


class ImportResponse(BaseModel):
    task_id: str
    status: str
    message: str
    started_at: str


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    progress: int
    message: str
    result: Dict[str, Any] = None
    error: str = None


@router.post("/import", response_model=ImportResponse)
async def start_securities_import(current_user: User = Depends(get_current_superadmin)):
    """
    Start securities import from Dhan API (Admin only)
    This runs as a background task and returns immediately
    """
    logger.info(f"Securities import requested by admin user {current_user.email}")

    try:
        # Start the Celery task
        task = import_securities_task.delay()

        response = ImportResponse(task_id=task.id, status="PENDING", message="Securities import started successfully", started_at=datetime.now().isoformat())

        logger.info(f"Started securities import task {task.id}")
        return response

    except Exception as e:
        logger.error(f"Failed to start securities import: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start import task: {str(e)}")


@router.get("/import/status/{task_id}", response_model=TaskStatusResponse)
async def get_import_status(task_id: str, current_user: User = Depends(get_current_superadmin)):
    """
    Get status of securities import task (Admin only)
    """
    try:
        # Get task result from Celery
        task_result = celery_app.AsyncResult(task_id)

        if task_result.state == 'PENDING':
            response = TaskStatusResponse(task_id=task_id, status='PENDING', progress=0, message='Task is waiting to be processed')

        elif task_result.state == 'PROGRESS':
            meta = task_result.info
            response = TaskStatusResponse(task_id=task_id, status='PROGRESS', progress=meta.get('progress', 0), message=meta.get('status', 'Processing...'))

        elif task_result.state == 'SUCCESS':
            result = task_result.result
            response = TaskStatusResponse(task_id=task_id, status='SUCCESS', progress=100, message='Import completed successfully', result=result)

        elif task_result.state == 'FAILURE':
            error_info = task_result.info
            response = TaskStatusResponse(task_id=task_id, status='FAILURE', progress=0, message='Import failed', error=str(error_info) if error_info else 'Unknown error')

        else:
            response = TaskStatusResponse(task_id=task_id, status=task_result.state, progress=0, message=f'Task state: {task_result.state}')

        return response

    except Exception as e:
        logger.error(f"Error getting task status for {task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get task status: {str(e)}")


@router.delete("/import/{task_id}")
async def cancel_import_task(task_id: str, current_user: User = Depends(get_current_superadmin)):
    """
    Cancel a running securities import task (Admin only)
    """
    try:
        celery_app.control.revoke(task_id, terminate=True)
        logger.info(f"Cancelled securities import task {task_id}")

        return {"task_id": task_id, "status": "cancelled", "message": "Task has been cancelled"}

    except Exception as e:
        logger.error(f"Error cancelling task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to cancel task: {str(e)}")
