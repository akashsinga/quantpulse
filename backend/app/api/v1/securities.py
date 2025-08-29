# backend/app/api/v1/securities.py
"""Securities Router"""

from typing import Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.api.dependencies import get_current_active_user, get_current_superuser
from app.services.task_service import TaskService
from app.repositories.securities import SecurityRepository, FutureRepository
from app.schemas.base import APIResponse, PaginatedResponse, PaginationMeta
from app.schemas.security import SecurityResponse, ImportStatusResponse, SecurityStatsResponse
from app.tasks.import_securities import import_securities_from_dhan
from app.utils.enum import TaskType, SecuritySegment
from app.utils.logger import get_logger

router = APIRouter()
logger = get_logger(__name__)


@router.get("", response_model=PaginatedResponse[SecurityResponse])
async def get_securities(
    skip: int = 0,
    limit: int = 100,
    # Search functionality
    q: Optional[str] = Query(None, description="Search term for symbol or name"),

    # Filters
    exchange_id: Optional[UUID] = None,
    security_type: Optional[str] = None,
    segment: Optional[str] = SecuritySegment.EQUITY.value,
    sector: Optional[str] = None,
    active_only: bool = True,

    # Futures-specific filters
    underlying_id: Optional[UUID] = Query(None, description="Filter futures by underlying security"),
    contract_month: Optional[str] = Query(None, description="Filter futures by contract month"),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_user)):
    """
    Get securities with optional search, filtering, and pagination.
    Handles both regular securities and futures contracts in one endpoint.
    """
    try:
        security_repo = SecurityRepository(db)

        # If searching for futures specifically
        if underlying_id or contract_month or (security_type and security_type.startswith("FUT")):
            future_repo = FutureRepository(db)

            # Build futures filters
            filters = {}
            if underlying_id:
                filters["underlying_id"] = underlying_id
            if contract_month:
                filters["contract_month"] = contract_month
            if security_type:
                filters["security_type"] = security_type
            if active_only is not None:
                filters["is_active"] = active_only

            # Search futures
            if q:
                futures, total = future_repo.search_futures(search_term=q, skip=skip, limit=limit, filters=filters)
            else:
                if underlying_id:
                    futures = future_repo.get_by_underlying(underlying_id, skip, limit, active_only)
                    total = len(futures)  # Could implement proper count
                elif contract_month:
                    futures = future_repo.get_by_contract_month(contract_month, skip, limit, active_only)
                    total = len(futures)
                else:
                    futures = future_repo.get_active_futures(skip, limit) if active_only else future_repo.get_all(skip, limit)
                    total = future_repo.count()

            # Convert futures to security responses (or create unified response)
            pagination = PaginationMeta.create(total=total, page=(skip // limit) + 1, per_page=limit)

            # You might want to create a unified response or convert futures to securities
            # For now, let's convert to security responses
            security_responses = []
            for future in futures:
                if future.security:
                    security_responses.append(SecurityResponse.model_validate(future.security))

            return PaginatedResponse(data=security_responses, pagination=pagination, message=f"Found {total} futures matching criteria")

        # Regular securities handling
        else:
            # Build filters for regular securities
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

            # Use search method for both search and filtering
            # If no search term, use empty string (search method handles filters regardless)
            search_term = q or ""
            securities, total = security_repo.search_securities(search_term=search_term, skip=skip, limit=limit, filters=filters)

            # Calculate pagination
            pagination = PaginationMeta.create(total=total, page=(skip // limit) + 1, per_page=limit)

            # Convert to response format
            security_responses = [SecurityResponse.model_validate(security) for security in securities]

            message = f"Found {total} securities"
            if q:
                message += f" matching '{q}'"

            return PaginatedResponse(data=security_responses, pagination=pagination, message=message)

    except Exception as e:
        logger.error(f"Error getting securities: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve securities")


@router.get('/stats', response_model=APIResponse[SecurityStatsResponse])
async def get_security_stats(db=Depends(get_db), current_user=Depends(get_current_superuser)):
    """Get securities statistics"""
    try:
        security_repo = SecurityRepository(db)

        total_securities = security_repo.get_all(limit=None)
        total = len(total_securities)
        active_securities = security_repo.get_many_by_field('is_active', True, limit=None)
        active = len(active_securities)
        futures_securities = security_repo.get_securities_by_segment(SecuritySegment.DERIVATIVE.value, limit=None)
        futures = len(futures_securities)
        derivatives_eligible = security_repo.get_many_by_field('is_derivatives_eligible', True, limit=None)
        derivatives = len(derivatives_eligible)

        stats_response = SecurityStatsResponse(total=total, active=active, futures=futures, derivatives=derivatives)

        return APIResponse(success=True, message="Securities stats fetched successfully", data=stats_response)
    except Exception as e:
        logger.error(f"Error preparing securities stats: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to fetch securities stats")


@router.post("/enrich-sectors", response_model=APIResponse[ImportStatusResponse])
async def enrich_sectors(force_refresh: bool = Query(False, description="Force refresh all securities even if they have sector data"), db: Session = Depends(get_db), current_user=Depends(get_current_superuser)):
    """
    Start sector enrichment from Dhan API.
    This will run as a background task and return a task ID for monitoring.
    Only processes EQUITY securities with ISIN that are missing sector data.
    """
    try:
        task_service = TaskService(db)

        # Create task run record
        task_run = task_service.create_task_run(celery_task_id="", task_name="enrich_sectors", task_type=TaskType.SECTOR_ENRICHMENT, title="Enrich Securities with Sector Data", description=f"Enrich equity securities with sector and industry information from Dhan API (force_refresh={force_refresh})", user_id=current_user.id, input_parameters={"force_refresh": force_refresh})

        # Start the Celery task
        from app.tasks.enrich_sectors import enrich_sectors_from_dhan
        celery_task = enrich_sectors_from_dhan.delay(force_refresh=force_refresh)

        # Update task run with Celery task ID
        task_service.task_run_repo.update(task_run, {"celery_task_id": celery_task.id})

        response_data = ImportStatusResponse(task_id=task_run.id, celery_task_id=celery_task.id, status="PENDING", message="Sector enrichment started successfully", created_at=task_run.created_at)

        return APIResponse(data=response_data, message="Sector enrichment started")

    except Exception as e:
        logger.error(f"Error starting sector enrichment: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to start sector enrichment")


@router.get("/{security_id}", response_model=APIResponse[SecurityResponse])
async def get_security(security_id: UUID, db: Session = Depends(get_db), current_user=Depends(get_current_active_user)):
    """Get a specific security by ID."""
    try:
        security_repo = SecurityRepository(db)
        security = security_repo.get_by_id_or_raise(security_id)

        return APIResponse(data=SecurityResponse.model_validate(security), message="Security retrieved successfully")

    except Exception as e:
        logger.error(f"Error getting security {security_id}: {e}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Security not found")


@router.post("/import", response_model=APIResponse[ImportStatusResponse])
async def import_securities(db: Session = Depends(get_db), current_user=Depends(get_current_superuser)):
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
            input_parameters={})

        # Start the Celery task
        celery_task = import_securities_from_dhan.delay()

        # Update task run with Celery task ID
        task_service.task_run_repo.update(task_run, {"celery_task_id": celery_task.id})

        response_data = ImportStatusResponse(task_id=task_run.id, celery_task_id=celery_task.id, status="PENDING", message="Securities import started successfully", created_at=task_run.created_at)

        return APIResponse(data=response_data, message="Securities import started")

    except Exception as e:
        logger.error(f"Error starting securities import: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to start securities import")


@router.get("/import/status/{task_id}", response_model=APIResponse[ImportStatusResponse])
async def get_import_status(task_id: UUID, db: Session = Depends(get_db), current_user=Depends(get_current_superuser)):
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
