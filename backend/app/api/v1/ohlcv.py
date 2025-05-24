# app/api/v1/endpoints/data_fetching.py

from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException, Query
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime, date
import uuid

from app.services.data_fetchers import create_ohlcv_fetcher
from app.api.deps import get_current_user, get_current_superadmin
from app.db.models.user import User
from app.utils.logger import get_logger

# Initialize router
router = APIRouter()

# Initialize logger
logger = get_logger(__name__)


# Pydantic models for request/response
class HistoricalDataRequest(BaseModel):
    security_ids: Optional[List[str]] = Field(None, description="List of security UUIDs")
    exchanges: Optional[List[str]] = Field(None, description="List of exchange codes")
    segments: Optional[List[str]] = Field(None, description="List of segment types")
    start_date: Optional[str] = Field(None, description="Start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD)")
    workers: int = Field(8, description="Number of worker threads")
    batch_size: int = Field(50, description="Batch size for processing")
    background: bool = Field(False, description="Run in background")


class TodayDataRequest(BaseModel):
    security_ids: Optional[List[str]] = Field(None, description="List of security UUIDs")
    exchanges: Optional[List[str]] = Field(None, description="List of exchange codes")
    segments: Optional[List[str]] = Field(None, description="List of segment types")
    mode: str = Field("regular", description="Processing mode: 'regular' or 'eod'")
    workers: int = Field(4, description="Number of worker threads")
    batch_size: int = Field(200, description="Batch size for processing")
    background: bool = Field(False, description="Run in background")


class UpdateAllRequest(BaseModel):
    security_ids: Optional[List[str]] = Field(None, description="List of security UUIDs")
    exchanges: Optional[List[str]] = Field(None, description="List of exchange codes")
    segments: Optional[List[str]] = Field(None, description="List of segment types")
    days_back: int = Field(7, description="Number of days back to check for gaps")
    include_today: bool = Field(True, description="Include today's data")
    workers: int = Field(8, description="Number of worker threads")
    batch_size: int = Field(50, description="Batch size for processing")
    notification_email: Optional[str] = Field(None, description="Email for completion notification")


class BackgroundJobResponse(BaseModel):
    job_id: str = Field(..., description="Background job ID")
    status: str = Field(..., description="Job status")
    started_at: datetime = Field(..., description="Job start time")
    estimated_completion: Optional[datetime] = Field(None, description="Estimated completion time")


# Global job storage (in a real implementation, use Redis or a database)
background_jobs = {}


# Background job execution functions
def run_historical_data_job(job_id: str, request: HistoricalDataRequest, user_id: str):
    """Run historical data fetch job in background."""
    try:
        # Create fetcher
        fetcher = create_ohlcv_fetcher()

        # Update job status
        background_jobs[job_id]["status"] = "running"

        # Execute operation
        result = fetcher.fetch_historical_data(security_ids=request.security_ids, exchanges=request.exchanges, segments=request.segments, start_date=request.start_date, end_date=request.end_date, workers=request.workers, batch_size=request.batch_size, verbose=False)

        # Update job status and result
        background_jobs[job_id]["status"] = "completed"
        background_jobs[job_id]["completed_at"] = datetime.now()
        background_jobs[job_id]["result"] = result

        logger.info(f"Completed background job {job_id}")

    except Exception as e:
        # Update job status with error
        background_jobs[job_id]["status"] = "error"
        background_jobs[job_id]["error"] = str(e)
        background_jobs[job_id]["completed_at"] = datetime.now()

        logger.error(f"Error in background job {job_id}: {str(e)}")


def run_today_data_job(job_id: str, request: TodayDataRequest, user_id: str):
    """Run today's data fetch job in background."""
    try:
        # Create fetcher
        fetcher = create_ohlcv_fetcher()

        # Update job status
        background_jobs[job_id]["status"] = "running"

        # Execute operation
        result = fetcher.fetch_current_day_data(security_ids=request.security_ids, exchanges=request.exchanges, segments=request.segments, is_eod=request.mode == "eod", workers=request.workers, batch_size=request.batch_size, verbose=False)

        # Update job status and result
        background_jobs[job_id]["status"] = "completed"
        background_jobs[job_id]["completed_at"] = datetime.now()
        background_jobs[job_id]["result"] = result

        logger.info(f"Completed background job {job_id}")

    except Exception as e:
        # Update job status with error
        background_jobs[job_id]["status"] = "error"
        background_jobs[job_id]["error"] = str(e)
        background_jobs[job_id]["completed_at"] = datetime.now()

        logger.error(f"Error in background job {job_id}: {str(e)}")


def run_update_all_job(job_id: str, request: UpdateAllRequest, user_id: str):
    """Run update all job in background."""
    try:
        # Create fetcher
        fetcher = create_ohlcv_fetcher()

        # Update job status
        background_jobs[job_id]["status"] = "running"

        # Execute operation
        result = fetcher.update_all_data(security_ids=request.security_ids, exchanges=request.exchanges, segments=request.segments, days_back=request.days_back, include_today=request.include_today, workers=request.workers, batch_size=request.batch_size, verbose=False)

        # Update job status and result
        background_jobs[job_id]["status"] = "completed"
        background_jobs[job_id]["completed_at"] = datetime.now()
        background_jobs[job_id]["result"] = result

        logger.info(f"Completed background job {job_id}")

        # Send notification if requested (simplified - in reality would use email service)
        if request.notification_email:
            logger.info(f"Would send notification to {request.notification_email}")

    except Exception as e:
        # Update job status with error
        background_jobs[job_id]["status"] = "error"
        background_jobs[job_id]["error"] = str(e)
        background_jobs[job_id]["completed_at"] = datetime.now()

        logger.error(f"Error in background job {job_id}: {str(e)}")


# API Endpoints
@router.post("/historical", summary="Fetch historical OHLCV data")
async def fetch_historical_data(request: HistoricalDataRequest, background_tasks: BackgroundTasks, current_user: User = Depends(get_current_user)):
    """Fetch historical OHLCV data for securities.

    By default, processes all active securities if none are specified.
    """
    logger.info(f"Historical data fetch request received from user {current_user.email}")

    # Run in background if requested
    if request.background:
        job_id = str(uuid.uuid4())

        # Register job
        background_jobs[job_id] = {"id": job_id, "type": "historical", "status": "pending", "user_id": str(current_user.id), "request": request.dict(), "started_at": datetime.now(), "estimated_completion": None}  # Would calculate this in a real implementation

        # Add background task
        background_tasks.add_task(run_historical_data_job, job_id, request, str(current_user.id))

        # Return job info
        return BackgroundJobResponse(job_id=job_id, status="pending", started_at=background_jobs[job_id]["started_at"], estimated_completion=background_jobs[job_id]["estimated_completion"])

    # Run synchronously
    fetcher = create_ohlcv_fetcher()

    result = fetcher.fetch_historical_data(security_ids=request.security_ids, exchanges=request.exchanges, segments=request.segments, start_date=request.start_date, end_date=request.end_date, workers=request.workers, batch_size=request.batch_size, verbose=False)

    return result


@router.post("/today", summary="Fetch today's OHLCV data")
async def fetch_today_data(request: TodayDataRequest, background_tasks: BackgroundTasks, current_user: User = Depends(get_current_user)):
    """Fetch today's OHLCV data for securities.

    By default, processes all active securities if none are specified.
    """
    logger.info(f"Today's data fetch request received from user {current_user.email}")

    # Run in background if requested
    if request.background:
        job_id = str(uuid.uuid4())

        # Register job
        background_jobs[job_id] = {"id": job_id, "type": "today", "status": "pending", "user_id": str(current_user.id), "request": request.dict(), "started_at": datetime.now(), "estimated_completion": None}

        # Add background task
        background_tasks.add_task(run_today_data_job, job_id, request, str(current_user.id))

        # Return job info
        return BackgroundJobResponse(job_id=job_id, status="pending", started_at=background_jobs[job_id]["started_at"], estimated_completion=background_jobs[job_id]["estimated_completion"])

    # Run synchronously
    fetcher = create_ohlcv_fetcher()

    result = fetcher.fetch_current_day_data(security_ids=request.security_ids, exchanges=request.exchanges, segments=request.segments, is_eod=request.mode == "eod", workers=request.workers, batch_size=request.batch_size, verbose=False)

    return result


@router.post("/update-all", summary="Update both historical and today's data")
async def update_all_data(request: UpdateAllRequest, background_tasks: BackgroundTasks, current_user: User = Depends(get_current_user)):
    """Update both historical and today's data for securities.

    This comprehensive update always runs as a background task.
    """
    logger.info(f"Update all request received from user {current_user.email}")

    job_id = str(uuid.uuid4())

    # Register job
    background_jobs[job_id] = {"id": job_id, "type": "update-all", "status": "pending", "user_id": str(current_user.id), "request": request.dict(), "started_at": datetime.now(), "estimated_completion": None}

    # Add background task
    background_tasks.add_task(run_update_all_job, job_id, request, str(current_user.id))

    # Return job info
    return BackgroundJobResponse(job_id=job_id, status="pending", started_at=background_jobs[job_id]["started_at"], estimated_completion=background_jobs[job_id]["estimated_completion"])


@router.get("/status/{job_id}", summary="Get status of a background job")
async def get_job_status(job_id: str, current_user: User = Depends(get_current_user)):
    """Get the status of a background data fetching job."""
    # Check if job exists
    if job_id not in background_jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    # Check if user has access to this job
    job = background_jobs[job_id]
    if str(current_user.id) != job["user_id"] and not current_user.is_superuser:
        raise HTTPException(status_code=403, detail="Not authorized to access this job")

    # Return job status
    return {"job_id": job_id, "type": job["type"], "status": job["status"], "started_at": job["started_at"], "completed_at": job.get("completed_at"), "error": job.get("error")}


@router.get("/report/{job_id}", summary="Get detailed report for a completed job")
async def get_job_report(job_id: str, current_user: User = Depends(get_current_user)):
    """Get detailed report for a completed data fetching job."""
    # Check if job exists
    if job_id not in background_jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    # Check if user has access to this job
    job = background_jobs[job_id]
    if str(current_user.id) != job["user_id"] and not current_user.is_superuser:
        raise HTTPException(status_code=403, detail="Not authorized to access this job")

    # Check if job is completed
    if job["status"] not in ["completed", "error"]:
        raise HTTPException(status_code=400, detail="Job is still running")

    # Return job result
    return {"job_id": job_id, "type": job["type"], "status": job["status"], "started_at": job["started_at"], "completed_at": job.get("completed_at"), "error": job.get("error"), "result": job.get("result")}


@router.delete("/jobs/{job_id}", summary="Delete a job")
async def delete_job(job_id: str, current_user: User = Depends(get_current_superadmin)):
    """Delete a job (admin only)."""
    # Check if job exists
    if job_id not in background_jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    # Delete job
    del background_jobs[job_id]

    return {"status": "success", "message": f"Job {job_id} deleted"}


@router.get("/jobs", summary="List all jobs")
async def list_jobs(status: Optional[str] = Query(None, description="Filter by status"), type: Optional[str] = Query(None, description="Filter by job type"), limit: int = Query(100, description="Max number of jobs to return"), current_user: User = Depends(get_current_superadmin)):
    """List all jobs (admin only)."""
    # Filter jobs
    filtered_jobs = []
    for job_id, job in background_jobs.items():
        if status and job["status"] != status:
            continue
        if type and job["type"] != type:
            continue

        # Add to filtered list
        filtered_jobs.append({"job_id": job_id, "type": job["type"], "status": job["status"], "user_id": job["user_id"], "started_at": job["started_at"], "completed_at": job.get("completed_at")})

        # Apply limit
        if len(filtered_jobs) >= limit:
            break

    return {"total": len(filtered_jobs), "jobs": filtered_jobs}
