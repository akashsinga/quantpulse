# backend/app/api/v1/securities.py
"""Securities Router"""

from typing import List, Optional, Dict, Any
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.api.dependencies import get_current_active_user
from app.services.task_service import TaskService
from app.repositories.securities import SecurityRepository, ExchangeRepository, FutureRepository
from app.repositories.tasks import TaskRunRepository
from app.schemas.base import APIResponse, PaginatedResponse, PaginationMeta
from app.schemas.security import SecurityResponse, ExchangeResponse, FutureResponse, ImportRequest, ImportStatusResponse
from app.tasks.import_securities import import_securities_from_dhan
from app.utils.enum import TaskType
from app.utils.logger import get_logger

router = APIRouter()
logger = get_logger(__name__)


@router.get("/", response_model=PaginatedResponse[SecurityResponse])
async def get_securities(skip: int = 0, limit: int = 100, exchange_id: Optional[UUID] = None, security_type: Optional[str] = None, segment: Optional[str] = None, sector: Optional[str] = None, active_only: bool = True, db: Session = Depends(get_db), current_user=Depends(get_current_active_user)):
    """Get list of securities with filtering and pagination."""
    try:
        security_repo = SecurityRepository(db)

        # Build filters
        filters = {}
        if exchange_id:
            filters["exchange_id"] = exchange_id
        if security_type:
            filters["security_type"] = security_type
        if segment:
            filters["segment"] = segment
        if sector:
            filters["sector"] = sector
        if active_only is not None:
            filters["is_active"] = active_only

        # Get securities with filters
        if filters:
            securities = security_repo.get_many_by_field("is_active", True, skip, limit)
            total = security_repo.count()
        else:
            securities = security_repo.get_all(skip, limit)
            total = security_repo.count()

        # Calculate pagination
        pagination = PaginationMeta.create(total=total, page=(skip // limit) + 1, per_page=limit)

        # Convert to response format
        security_responses = [SecurityResponse.from_orm(security) for security in securities]

        return PaginatedResponse(data=security_responses, pagination=pagination, message="Securities retrieved successfully")

    except Exception as e:
        logger.error(f"Error getting securities: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve securities")


@router.get("/search", response_model=PaginatedResponse[SecurityResponse])
async def search_securities(q: str = Query(..., description="Search term for symbol or name"), skip: int = 0, limit: int = 50, security_type: Optional[str] = None, segment: Optional[str] = None, exchange_id: Optional[UUID] = None, db: Session = Depends(get_db), current_user=Depends(get_current_active_user)):
    """Search securities by symbol or name."""
    try:
        security_repo = SecurityRepository(db)

        # Build filters
        filters = {}
        if security_type:
            filters["security_type"] = security_type
        if segment:
            filters["segment"] = segment
        if exchange_id:
            filters["exchange_id"] = exchange_id

        # Perform search
        securities, total = security_repo.search_securities(search_term=q, skip=skip, limit=limit, filters=filters)

        # Calculate pagination
        pagination = PaginationMeta.create(total=total, page=(skip // limit) + 1, per_page=limit)

        # Convert to response format
        security_responses = [SecurityResponse.from_orm(security) for security in securities]

        return PaginatedResponse(data=security_responses, pagination=pagination, message=f"Found {total} securities matching '{q}'")

    except Exception as e:
        logger.error(f"Error searching securities: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to search securities")


@router.get("/{security_id}", response_model=APIResponse[SecurityResponse])
async def get_security(security_id: UUID, db: Session = Depends(get_db), current_user=Depends(get_current_active_user)):
    """Get a specific security by ID."""
    try:
        security_repo = SecurityRepository(db)
        security = security_repo.get_by_id_or_raise(security_id)

        return APIResponse(data=SecurityResponse.from_orm(security), message="Security retrieved successfully")

    except Exception as e:
        logger.error(f"Error getting security {security_id}: {e}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Security not found")


@router.get("/exchanges/", response_model=APIResponse[List[ExchangeResponse]])
async def get_exchanges(active_only: bool = True, db: Session = Depends(get_db), current_user=Depends(get_current_active_user)):
    """Get list of all exchanges."""
    try:
        exchange_repo = ExchangeRepository(db)

        if active_only:
            exchanges = exchange_repo.get_many_by_field("is_active", True)
        else:
            exchanges = exchange_repo.get_all()

        exchange_responses = [ExchangeResponse.from_orm(exchange) for exchange in exchanges]

        return APIResponse(data=exchange_responses, message="Exchanges retrieved successfully")

    except Exception as e:
        logger.error(f"Error getting exchanges: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve exchanges")


@router.get("/futures/", response_model=PaginatedResponse[FutureResponse])
async def get_futures(skip: int = 0, limit: int = 100, underlying_id: Optional[UUID] = None, contract_month: Optional[str] = None, active_only: bool = True, db: Session = Depends(get_db), current_user=Depends(get_current_active_user)):
    """Get list of futures contracts."""
    try:
        future_repo = FutureRepository(db)

        if underlying_id:
            futures = future_repo.get_by_underlying(underlying_id, skip, limit, active_only)
            total = len(futures)  # This is approximate - you might want to implement a count method
        elif contract_month:
            futures = future_repo.get_by_contract_month(contract_month, skip, limit, active_only)
            total = len(futures)
        else:
            futures = future_repo.get_active_futures(skip, limit) if active_only else future_repo.get_all(skip, limit)
            total = future_repo.count()

        # Calculate pagination
        pagination = PaginationMeta.create(total=total, page=(skip // limit) + 1, per_page=limit)

        # Convert to response format
        future_responses = [FutureResponse.from_orm(future) for future in futures]

        return PaginatedResponse(data=future_responses, pagination=pagination, message="Futures retrieved successfully")

    except Exception as e:
        logger.error(f"Error getting futures: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve futures")


@router.post("/import/", response_model=APIResponse[ImportStatusResponse])
async def import_securities(request: ImportRequest, background_tasks: BackgroundTasks, db: Session = Depends(get_db), current_user=Depends(get_current_active_user)):
    """
    Start securities import from Dhan CSV.
    This will run as a background task and return a task ID for monitoring.
    """
    try:
        task_service = TaskService(db)

        # Create task run record
        task_run = task_service.create_task_run(
            celery_task_id="",  # Will be updated when task starts
            task_name="import_securities",
            task_type=TaskType.SECURITIES_IMPORT,
            title="Import Securities from Dhan CSV",
            description="Import all securities data from Dhan API scrip master CSV",
            user_id=current_user.id,
            input_parameters={
                "force_refresh": request.force_refresh,
                "source": "dhan_csv"
            })

        # Start the Celery task
        celery_task = import_securities.delay(force_refresh=request.force_refresh)

        # Update task run with Celery task ID
        task_service.task_run_repo.update(task_run, {"celery_task_id": celery_task.id})

        response_data = ImportStatusResponse(task_id=task_run.id, celery_task_id=celery_task.id, status="PENDING", message="Securities import started successfully", created_at=task_run.created_at)

        return APIResponse(data=response_data, message="Securities import started")

    except Exception as e:
        logger.error(f"Error starting securities import: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to start securities import")


@router.get("/import/status/{task_id}", response_model=APIResponse[ImportStatusResponse])
async def get_import_status(task_id: UUID, db: Session = Depends(get_db), current_user=Depends(get_current_active_user)):
    """Get the status of a securities import task."""
    try:
        task_service = TaskService(db)
        task_status = task_service.get_task_status(task_id)

        if not task_status:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Import task not found")

        response_data = ImportStatusResponse(task_id=task_id, celery_task_id=task_status.get("celery_task_id"), status=task_status.get("status"), message=task_status.get("current_message", ""), progress_percentage=task_status.get("progress_percentage", 0), created_at=task_status.get("created_at"), result_data=task_status.get("result_data"))

        return APIResponse(data=response_data, message="Import status retrieved")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting import status: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve import status")
