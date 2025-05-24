# app/api/v1/ohlcv.py - UPDATED with new OHLCV endpoints

from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException, Query
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime, date
import uuid

from app.tasks.ohlcv_import import (fetch_historical_ohlcv_task, fetch_daily_ohlcv_task, generate_weekly_ohlcv_task, cleanup_failed_ohlcv_fetches_task, daily_ohlcv_automation_task, full_ohlcv_import_task)
from app.services.data_fetchers.ohlcv_fetcher import create_ohlcv_fetcher
from app.services.weekly_aggregator import WeeklyDataAggregator
from app.api.deps import get_current_user, get_current_superadmin
from app.db.models.user import User
from app.db.models.security import Security
from app.utils.logger import get_logger

# Initialize router
router = APIRouter()

# Initialize logger
logger = get_logger(__name__)


# Pydantic models for request/response
class HistoricalOHLCVRequest(BaseModel):
    security_ids: Optional[List[str]] = Field(None, description="List of security UUIDs")
    from_date: str = Field("2000-01-01", description="Start date (YYYY-MM-DD)")
    to_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD)")
    batch_size: int = Field(50, description="Batch size for processing")
    background: bool = Field(True, description="Run in background")


class DailyOHLCVRequest(BaseModel):
    security_ids: Optional[List[str]] = Field(None, description="List of security UUIDs")
    background: bool = Field(True, description="Run in background")


class WeeklyOHLCVRequest(BaseModel):
    security_ids: Optional[List[str]] = Field(None, description="List of security UUIDs")
    weeks_back: int = Field(4, description="Number of weeks back to regenerate")
    background: bool = Field(True, description="Run in background")


class TaskResponse(BaseModel):
    task_id: str = Field(..., description="Background task ID")
    status: str = Field(..., description="Task status")
    message: str = Field(..., description="Task description")
    started_at: datetime = Field(..., description="Task start time")


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    progress: Optional[int] = None
    message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


# OHLCV Data Fetching Endpoints
@router.post("/historical", response_model=TaskResponse, summary="Start historical OHLCV data fetch")
async def start_historical_fetch(request: HistoricalOHLCVRequest, background_tasks: BackgroundTasks, current_user: User = Depends(get_current_user)):
    """
    Start historical OHLCV data fetch for securities.
    
    This endpoint fetches complete historical data from 2000-01-01 to present.
    By default, processes all active stocks and indices if no security_ids specified.
    
    **Features:**
    - Handles duplicates automatically
    - Resume capability for interrupted imports
    - Rate limiting (20 req/sec)
    - Progress tracking
    - Error handling with retries
    """
    logger.info(f"Historical OHLCV fetch request from user {current_user.email}")

    if not request.background:
        # Synchronous execution (not recommended for large datasets)
        try:
            fetcher = create_ohlcv_fetcher()

            # Get securities to process
            if request.security_ids:
                security_uuids = [uuid.UUID(sid) for sid in request.security_ids]
                with fetcher.db.session() as db:
                    securities = db.query(Security).filter(Security.id.in_(security_uuids)).all()
            else:
                securities = fetcher.get_pending_securities('historical')

            # Process securities
            results = []
            for security in securities[:5]:  # Limit to 5 for sync execution
                result = fetcher.fetch_historical_data_for_security(security, request.from_date, request.to_date)
                results.append(result)

            return {'status': 'COMPLETED', 'results': results, 'processed': len(results)}

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Historical fetch failed: {str(e)}")

    # Background execution (recommended)
    try:
        task = fetch_historical_ohlcv_task.delay(security_ids=request.security_ids, from_date=request.from_date, to_date=request.to_date, batch_size=request.batch_size)

        return TaskResponse(task_id=task.id, status="PENDING", message=f"Historical OHLCV fetch started for {len(request.security_ids) if request.security_ids else 'all'} securities", started_at=datetime.now())

    except Exception as e:
        logger.error(f"Failed to start historical fetch task: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start task: {str(e)}")


@router.post("/daily", response_model=TaskResponse, summary="Start daily OHLCV data fetch")
async def start_daily_fetch(request: DailyOHLCVRequest, background_tasks: BackgroundTasks, current_user: User = Depends(get_current_user)):
    """
    Start daily OHLCV data fetch (today's data).
    
    Fetches current day's OHLC data for active securities.
    Uses batched API calls for efficiency.
    """
    logger.info(f"Daily OHLCV fetch request from user {current_user.email}")

    if not request.background:
        # Synchronous execution
        try:
            fetcher = create_ohlcv_fetcher()

            if request.security_ids:
                security_uuids = [uuid.UUID(sid) for sid in request.security_ids]
                with fetcher.db.session() as db:
                    securities = db.query(Security).filter(Security.id.in_(security_uuids)).all()
            else:
                securities = fetcher.get_pending_securities('daily')

            result = fetcher.fetch_today_eod_data(securities)
            return result

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Daily fetch failed: {str(e)}")

    # Background execution
    try:
        task = fetch_daily_ohlcv_task.delay(security_ids=request.security_ids)

        return TaskResponse(task_id=task.id, status="PENDING", message=f"Daily OHLCV fetch started for {len(request.security_ids) if request.security_ids else 'all pending'} securities", started_at=datetime.now())

    except Exception as e:
        logger.error(f"Failed to start daily fetch task: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start task: {str(e)}")


@router.post("/weekly/generate", response_model=TaskResponse, summary="Generate weekly OHLCV data")
async def generate_weekly_data(request: WeeklyOHLCVRequest, background_tasks: BackgroundTasks, current_user: User = Depends(get_current_user)):
    """
    Generate weekly OHLCV data from daily data.
    
    Aggregates daily OHLCV records into weekly timeframe:
    - Open: First day's open of the week
    - High: Highest high of the week  
    - Low: Lowest low of the week
    - Close: Last day's close of the week
    - Volume: Sum of daily volumes
    """
    logger.info(f"Weekly OHLCV generation request from user {current_user.email}")

    if not request.background:
        # Synchronous execution
        try:
            aggregator = WeeklyDataAggregator()
            security_uuids = [uuid.UUID(sid) for sid in request.security_ids] if request.security_ids else None

            result = aggregator.generate_weekly_data(security_ids=security_uuids, weeks_back=request.weeks_back)
            return result

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Weekly generation failed: {str(e)}")

    # Background execution
    try:
        task = generate_weekly_ohlcv_task.delay(security_ids=request.security_ids, weeks_back=request.weeks_back)

        return TaskResponse(task_id=task.id, status="PENDING", message=f"Weekly OHLCV generation started for {request.weeks_back} weeks back", started_at=datetime.now())

    except Exception as e:
        logger.error(f"Failed to start weekly generation task: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start task: {str(e)}")


# Task Management Endpoints
@router.get("/tasks/{task_id}/status", response_model=TaskStatusResponse, summary="Get task status")
async def get_task_status(task_id: str, current_user: User = Depends(get_current_user)):
    """Get the status of an OHLCV background task."""
    try:
        from app.core.celery_app import celery_app

        task_result = celery_app.AsyncResult(task_id)

        if task_result.state == 'PENDING':
            response = TaskStatusResponse(task_id=task_id, status='PENDING', progress=0, message='Task is waiting to be processed')
        elif task_result.state == 'PROGRESS':
            meta = task_result.info or {}
            response = TaskStatusResponse(task_id=task_id, status='PROGRESS', progress=meta.get('progress', 0), message=meta.get('status', 'Processing...'))
        elif task_result.state == 'SUCCESS':
            result = task_result.result
            response = TaskStatusResponse(task_id=task_id, status='SUCCESS', progress=100, message='Task completed successfully', result=result)
        elif task_result.state == 'FAILURE':
            error_info = task_result.info
            response = TaskStatusResponse(task_id=task_id, status='FAILURE', progress=0, message='Task failed', error=str(error_info) if error_info else 'Unknown error')
        else:
            response = TaskStatusResponse(task_id=task_id, status=task_result.state, progress=0, message=f'Task state: {task_result.state}')

        return response

    except Exception as e:
        logger.error(f"Error getting task status for {task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get task status: {str(e)}")


@router.delete("/tasks/{task_id}", summary="Cancel task")
async def cancel_task(task_id: str, current_user: User = Depends(get_current_superadmin)):
    """Cancel a running OHLCV task (Admin only)."""
    try:
        from app.core.celery_app import celery_app
        celery_app.control.revoke(task_id, terminate=True)

        logger.info(f"Cancelled OHLCV task {task_id}")
        return {"task_id": task_id, "status": "cancelled", "message": "Task has been cancelled"}

    except Exception as e:
        logger.error(f"Error cancelling task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to cancel task: {str(e)}")


# Automation & Workflows
@router.post("/automation/daily", summary="Start daily automation workflow")
async def start_daily_automation(current_user: User = Depends(get_current_user)):
    """
    Start the daily OHLCV automation workflow.
    
    This workflow:
    1. Fetches today's OHLCV data for all securities
    2. Generates/updates weekly data
    3. Sends completion notifications
    """
    try:
        task = daily_ohlcv_automation_task.delay()

        return TaskResponse(task_id=task.id, status="PENDING", message="Daily OHLCV automation workflow started", started_at=datetime.now())

    except Exception as e:
        logger.error(f"Failed to start daily automation: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start automation: {str(e)}")


@router.post("/automation/full-import", summary="Start full OHLCV import workflow")
async def start_full_import(from_date: str = Query("2000-01-01", description="Start date for historical import"), current_user: User = Depends(get_current_superadmin)):
    """
    Start the complete OHLCV import workflow (Admin only).
    
    This workflow:
    1. Fetches complete historical data from specified date
    2. Generates weekly data from daily data
    3. Sets up daily automation
    
    **Warning: This is a long-running operation that may take hours to complete.**
    """
    try:
        task = full_ohlcv_import_task.delay(from_date=from_date)

        return TaskResponse(task_id=task.id, status="PENDING", message=f"Full OHLCV import workflow started from {from_date}", started_at=datetime.now())

    except Exception as e:
        logger.error(f"Failed to start full import: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start import: {str(e)}")


# Maintenance & Monitoring
@router.post("/maintenance/cleanup-failed", summary="Cleanup and retry failed fetches")
async def cleanup_failed_fetches(max_retries: int = Query(3, description="Maximum retry attempts"), current_user: User = Depends(get_current_superadmin)):
    """Cleanup and retry failed OHLCV fetches (Admin only)."""
    try:
        task = cleanup_failed_ohlcv_fetches_task.delay(max_retries=max_retries)

        return TaskResponse(task_id=task.id, status="PENDING", message=f"Cleanup of failed fetches started (max retries: {max_retries})", started_at=datetime.now())

    except Exception as e:
        logger.error(f"Failed to start cleanup task: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start cleanup: {str(e)}")


@router.get("/status/progress", summary="Get overall OHLCV import progress")
async def get_progress_status(current_user: User = Depends(get_current_user)):
    """Get overall progress of OHLCV data import."""
    try:
        fetcher = create_ohlcv_fetcher()
        progress = fetcher.get_progress_summary()

        # Add weekly data statistics
        aggregator = WeeklyDataAggregator()
        weekly_stats = aggregator.get_weekly_data_statistics()

        return {**progress, 'weekly_data': weekly_stats, 'last_updated': datetime.now()}

    except Exception as e:
        logger.error(f"Error getting progress status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get status: {str(e)}")


@router.get("/status/api", summary="Test Dhan API connection")
async def test_api_connection(current_user: User = Depends(get_current_user)):
    """Test connection to Dhan API."""
    try:
        fetcher = create_ohlcv_fetcher()
        connection_ok = fetcher.test_api_connection()

        if connection_ok:
            return {"status": "SUCCESS", "message": "Dhan API connection successful", "timestamp": datetime.now()}
        else:
            return {"status": "FAILED", "message": "Dhan API connection failed", "timestamp": datetime.now()}

    except Exception as e:
        logger.error(f"API connection test error: {e}")
        raise HTTPException(status_code=500, detail=f"Connection test failed: {str(e)}")


@router.get("/statistics", summary="Get OHLCV data statistics")
async def get_ohlcv_statistics(current_user: User = Depends(get_current_user)):
    """Get comprehensive OHLCV data statistics."""
    try:
        from app.db.session import get_db_session
        from app.db.models.ohlcv_daily import OHLCVDaily
        from app.db.models.ohlcv_weekly import OHLCVWeekly
        from sqlalchemy import func

        with get_db_session() as db:
            # Daily data statistics
            daily_stats = db.query(func.count(OHLCVDaily.time).label('total_records'), func.count(func.distinct(OHLCVDaily.security_id)).label('securities_count'), func.min(OHLCVDaily.time).label('earliest_date'), func.max(OHLCVDaily.time).label('latest_date')).first()

            # Weekly data statistics
            weekly_stats = db.query(func.count(OHLCVWeekly.time).label('total_records'), func.count(func.distinct(OHLCVWeekly.security_id)).label('securities_count'), func.min(OHLCVWeekly.time).label('earliest_date'), func.max(OHLCVWeekly.time).label('latest_date')).first()

            return {
                'daily_data': {
                    'total_records': daily_stats.total_records or 0,
                    'securities_covered': daily_stats.securities_count or 0,
                    'date_range': {
                        'start': daily_stats.earliest_date.date() if daily_stats.earliest_date else None,
                        'end': daily_stats.latest_date.date() if daily_stats.latest_date else None
                    }
                },
                'weekly_data': {
                    'total_records': weekly_stats.total_records or 0,
                    'securities_covered': weekly_stats.securities_count or 0,
                    'date_range': {
                        'start': weekly_stats.earliest_date.date() if weekly_stats.earliest_date else None,
                        'end': weekly_stats.latest_date.date() if weekly_stats.latest_date else None
                    }
                },
                'generated_at': datetime.now()
            }

    except Exception as e:
        logger.error(f"Error getting OHLCV statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get statistics: {str(e)}")
