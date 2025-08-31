# backend/app/api/v1/ohlcv.py
"""
OHLCV (Market Data) Router for QuantPulse API
Handles OHLCV data operations, imports, and queries.
"""

from typing import Optional, List
from uuid import UUID
from datetime import date, datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.api.dependencies import get_current_active_user, get_current_superuser
from app.services.ohlcv_service import OHLCVService
from app.services.task_service import TaskService
from app.schemas.base import APIResponse, PaginatedResponse, PaginationMeta
from app.schemas.ohlcv import (OHLCVResponse, OHLCVImportRequest, OHLCVImportStatusResponse, DataCoverageResponse, OHLCVStatsResponse, OHLCVBulkResponse)
from app.utils.enum import TaskType, Timeframe
from app.utils.logger import get_logger
from app.core.exceptions import NotFoundError, ValidationError, to_http_exception

router = APIRouter()
logger = get_logger(__name__)


@router.get("/{security_id}", response_model=APIResponse[List[OHLCVResponse]])
async def get_ohlcv_data(security_id: UUID, date_from: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"), date_to: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"), timeframe: str = Query(Timeframe.DAILY.value, description="Data timeframe"), limit: int = Query(1000, le=5000, description="Maximum records to return"), db: Session = Depends(get_db), current_user=Depends(get_current_active_user)):
    """Get OHLCV data for a specific security within date range."""
    try:
        ohlcv_service = OHLCVService(db)

        # Set default dates if not provided
        if not date_to:
            date_to = date.today()
        if not date_from:
            date_from = date_to - timedelta(days=90)  # Default 90 days

        # Validate date range
        if date_from > date_to:
            raise ValidationError("date_from cannot be greater than date_to")

        # Get OHLCV data
        ohlcv_data = ohlcv_service.get_ohlcv_data(str(security_id), date_from, date_to, timeframe, limit)

        # Convert to response format
        ohlcv_responses = [OHLCVResponse.model_validate(data) for data in ohlcv_data]

        message = f"Retrieved {len(ohlcv_responses)} OHLCV records"
        if len(ohlcv_responses) == limit:
            message += f" (limited to {limit})"

        return APIResponse(data=ohlcv_responses, message=message)

    except (NotFoundError, ValidationError) as e:
        raise to_http_exception(e)
    except Exception as e:
        logger.error(f"Error getting OHLCV data for security {security_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve OHLCV data")


@router.get("/{security_id}/latest", response_model=APIResponse[OHLCVResponse])
async def get_latest_ohlcv_data(security_id: UUID, timeframe: str = Query(Timeframe.DAILY.value, description="Data timeframe"), db: Session = Depends(get_db), current_user=Depends(get_current_active_user)):
    """Get the latest OHLCV data for a security."""
    try:
        ohlcv_service = OHLCVService(db)

        # Get last 5 days of data to ensure we get the latest available
        date_to = date.today()
        date_from = date_to - timedelta(days=5)

        ohlcv_data = ohlcv_service.get_ohlcv_data(str(security_id), date_from, date_to, timeframe, limit=5)

        if not ohlcv_data:
            raise NotFoundError("OHLCV data", f"security_id={security_id}")

        # Get the most recent record
        latest_data = max(ohlcv_data, key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'))

        return APIResponse(data=OHLCVResponse.model_validate(latest_data), message="Latest OHLCV data retrieved successfully")

    except NotFoundError as e:
        raise to_http_exception(e)
    except Exception as e:
        logger.error(f"Error getting latest OHLCV data for security {security_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve latest OHLCV data")


@router.get("/{security_id}/stats", response_model=APIResponse[OHLCVStatsResponse])
async def get_ohlcv_statistics(security_id: UUID, date_from: Optional[date] = Query(None, description="Start date for statistics"), date_to: Optional[date] = Query(None, description="End date for statistics"), timeframe: str = Query(Timeframe.DAILY.value, description="Data timeframe"), db: Session = Depends(get_db), current_user=Depends(get_current_active_user)):
    """Get statistical analysis of OHLCV data for a security."""
    try:
        ohlcv_service = OHLCVService(db)

        # Set default dates if not provided
        if not date_to:
            date_to = date.today()
        if not date_from:
            date_from = date_to - timedelta(days=90)  # Default 90 days

        # Get OHLCV data
        ohlcv_data = ohlcv_service.get_ohlcv_data(str(security_id), date_from, date_to, timeframe, limit=1000)

        if not ohlcv_data:
            raise NotFoundError("OHLCV data", f"security_id={security_id}")

        # Calculate statistics using DhanService
        from app.services.dhan_service import DhanService
        dhan_service = DhanService()
        stats = dhan_service.get_ohlcv_statistics(ohlcv_data)

        return APIResponse(data=OHLCVStatsResponse.model_validate(stats), message=f"Statistics calculated for {len(ohlcv_data)} records")

    except NotFoundError as e:
        raise to_http_exception(e)
    except Exception as e:
        logger.error(f"Error calculating OHLCV statistics for security {security_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to calculate OHLCV statistics")


@router.post("/import", response_model=APIResponse[OHLCVImportStatusResponse])
async def import_ohlcv_data(import_request: OHLCVImportRequest, background_tasks: BackgroundTasks, db: Session = Depends(get_db), current_user=Depends(get_current_superuser)):
    """
    Start OHLCV data import from Dhan API.
    This will run as a background task and return a task ID for monitoring.
    """
    try:
        task_service = TaskService(db)

        # Prepare task parameters
        task_params = {'security_id': str(import_request.security_id) if import_request.security_id else None, 'date_from': import_request.date_from.isoformat() if import_request.date_from else None, 'date_to': import_request.date_to.isoformat() if import_request.date_to else None, 'timeframe': import_request.timeframe, 'import_type': import_request.import_type, 'force_update': import_request.force_update}

        # Create task run record
        task_run = task_service.create_task_run(
            celery_task_id="",  # Will be updated when task starts
            task_name="import_ohlcv_data",
            task_type=TaskType.DATA_ENRICHMENT,
            title="Import OHLCV Data from Dhan API",
            description=f"Import OHLCV data ({import_request.import_type}) from {import_request.date_from} to {import_request.date_to}",
            user_id=current_user.id,
            input_parameters=task_params)

        # Start the background task
        from app.tasks.import_ohlcv import import_ohlcv_from_dhan
        celery_task = import_ohlcv_from_dhan.delay(**task_params)

        # Update task run with Celery task ID
        task_service.task_run_repo.update(task_run, {"celery_task_id": celery_task.id})

        response_data = OHLCVImportStatusResponse(task_id=task_run.id, celery_task_id=celery_task.id, status="PENDING", message="OHLCV import started successfully", created_at=task_run.created_at)

        return APIResponse(data=response_data, message="OHLCV import task started")

    except Exception as e:
        logger.error(f"Error starting OHLCV import: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to start OHLCV import")


@router.get("/import/status/{task_id}", response_model=APIResponse[OHLCVImportStatusResponse])
async def get_ohlcv_import_status(task_id: UUID, db: Session = Depends(get_db), current_user=Depends(get_current_superuser)):
    """Get the status of an OHLCV import task."""
    try:
        task_service = TaskService(db)
        task_status = task_service.get_task_status(task_id)

        if not task_status:
            raise NotFoundError("Import task", str(task_id))

        response_data = OHLCVImportStatusResponse(task_id=task_id, celery_task_id=task_status.get("celery_task_id"), status=task_status.get("status"), message=task_status.get("current_message", ""), progress_percentage=task_status.get("progress_percentage", 0), created_at=task_status.get("created_at"), result_data=task_status.get("result_data"))

        return APIResponse(data=response_data, message="Import status retrieved")

    except NotFoundError as e:
        raise to_http_exception(e)
    except Exception as e:
        logger.error(f"Error getting OHLCV import status: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve import status")


@router.get("/coverage/summary", response_model=APIResponse[DataCoverageResponse])
async def get_data_coverage_summary(security_ids: Optional[List[UUID]] = Query(None, description="Specific security IDs"), db: Session = Depends(get_db), current_user=Depends(get_current_active_user)):
    """Get data coverage summary for securities."""
    try:
        ohlcv_service = OHLCVService(db)

        security_ids_str = [str(sid) for sid in security_ids] if security_ids else None
        coverage_summary = ohlcv_service.get_data_coverage_summary(security_ids_str)

        return APIResponse(data=DataCoverageResponse.model_validate(coverage_summary), message="Data coverage summary retrieved successfully")

    except Exception as e:
        logger.error(f"Error getting data coverage summary: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve data coverage summary")


@router.get("/bulk", response_model=APIResponse[OHLCVBulkResponse])
async def get_bulk_ohlcv_data(security_ids: List[UUID] = Query(..., description="List of security IDs"),
                              date_from: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
                              date_to: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
                              timeframe: str = Query(Timeframe.DAILY.value, description="Data timeframe"),
                              limit_per_security: int = Query(100, le=1000, description="Max records per security"),
                              db: Session = Depends(get_db),
                              current_user=Depends(get_current_active_user)):
    """Get OHLCV data for multiple securities."""
    try:
        if len(security_ids) > 50:
            raise ValidationError("Cannot fetch data for more than 50 securities at once")

        ohlcv_service = OHLCVService(db)

        # Set default dates if not provided
        if not date_to:
            date_to = date.today()
        if not date_from:
            date_from = date_to - timedelta(days=30)  # Default 30 days for bulk

        bulk_data = {}
        successful_count = 0
        failed_securities = []

        for security_id in security_ids:
            try:
                ohlcv_data = ohlcv_service.get_ohlcv_data(str(security_id), date_from, date_to, timeframe, limit_per_security)

                if ohlcv_data:
                    bulk_data[str(security_id)] = [OHLCVResponse.model_validate(data) for data in ohlcv_data]
                    successful_count += 1
                else:
                    failed_securities.append(str(security_id))

            except Exception as e:
                logger.warning(f"Failed to get OHLCV data for security {security_id}: {e}")
                failed_securities.append(str(security_id))

        bulk_response = OHLCVBulkResponse(data=bulk_data, summary={'requested_securities': len(security_ids), 'successful_securities': successful_count, 'failed_securities': len(failed_securities), 'failed_security_ids': failed_securities, 'date_range': {'from': date_from.isoformat(), 'to': date_to.isoformat()}})

        return APIResponse(data=bulk_response, message=f"Retrieved OHLCV data for {successful_count}/{len(security_ids)} securities")

    except ValidationError as e:
        raise to_http_exception(e)
    except Exception as e:
        logger.error(f"Error getting bulk OHLCV data: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve bulk OHLCV data")


@router.delete("/{security_id}", response_model=APIResponse[dict])
async def delete_ohlcv_data(security_id: UUID, date_from: date = Query(..., description="Start date for deletion"), date_to: date = Query(..., description="End date for deletion"), timeframe: str = Query(Timeframe.DAILY.value, description="Data timeframe"), hard_delete: bool = Query(False, description="Perform hard delete (permanent)"), db: Session = Depends(get_db), current_user=Depends(get_current_superuser)):
    """Delete OHLCV data for a security within date range (Admin only)."""
    try:
        from app.repositories.market_data import OHLCVRepository

        # Validate date range
        if date_from > date_to:
            raise ValidationError("date_from cannot be greater than date_to")

        # Limit deletion range to prevent accidental mass deletion
        max_days = 365  # Maximum 1 year at a time
        if (date_to - date_from).days > max_days:
            raise ValidationError(f"Cannot delete more than {max_days} days of data at once")

        ohlcv_repo = OHLCVRepository(db)
        deleted_count = ohlcv_repo.delete_data_by_date_range(security_id, date_from, date_to, timeframe, hard_delete)

        delete_type = "hard" if hard_delete else "soft"

        return APIResponse(data={'deleted_records': deleted_count, 'security_id': str(security_id), 'date_from': date_from.isoformat(), 'date_to': date_to.isoformat(), 'delete_type': delete_type}, message=f"Successfully {delete_type} deleted {deleted_count} OHLCV records")

    except ValidationError as e:
        raise to_http_exception(e)
    except Exception as e:
        logger.error(f"Error deleting OHLCV data for security {security_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to delete OHLCV data")


@router.get("/{security_id}/gaps", response_model=APIResponse[dict])
async def get_data_gaps(security_id: UUID, date_from: Optional[date] = Query(None, description="Start date for gap analysis"), date_to: Optional[date] = Query(None, description="End date for gap analysis"), timeframe: str = Query(Timeframe.DAILY.value, description="Data timeframe"), db: Session = Depends(get_db), current_user=Depends(get_current_active_user)):
    """Identify gaps in OHLCV data for a security."""
    try:
        from app.repositories.market_data import OHLCVRepository
        import pandas as pd

        ohlcv_repo = OHLCVRepository(db)

        # Set default dates if not provided
        if not date_to:
            date_to = date.today()
        if not date_from:
            date_from = date_to - timedelta(days=90)

        # Get existing data
        ohlcv_data = ohlcv_repo.get_by_security_date_range(security_id, date_from, date_to, timeframe, limit=10000)

        if not ohlcv_data:
            return APIResponse(data={'gaps': [], 'total_gaps': 0, 'missing_days': 0, 'coverage_percentage': 0.0, 'analysis_period': {'from': date_from.isoformat(), 'to': date_to.isoformat(), 'total_days': (date_to - date_from).days + 1}}, message="No OHLCV data found for the specified period")

        # Convert to date set for gap analysis
        existing_dates = {data.date for data in ohlcv_data}

        # Generate expected trading days (excluding weekends)
        expected_dates = set()
        current_date = date_from
        while current_date <= date_to:
            # Skip weekends (Saturday=5, Sunday=6)
            if current_date.weekday() < 5:
                expected_dates.add(current_date)
            current_date += timedelta(days=1)

        # Find gaps
        missing_dates = expected_dates - existing_dates
        missing_dates_list = sorted(list(missing_dates))

        # Group consecutive missing dates into ranges
        gaps = []
        if missing_dates_list:
            gap_start = missing_dates_list[0]
            gap_end = missing_dates_list[0]

            for i in range(1, len(missing_dates_list)):
                current_missing_date = missing_dates_list[i]
                if (current_missing_date - gap_end).days == 1:
                    # Consecutive date, extend current gap
                    gap_end = current_missing_date
                else:
                    # Non-consecutive, close current gap and start new one
                    gaps.append({'start_date': gap_start.isoformat(), 'end_date': gap_end.isoformat(), 'missing_days': (gap_end - gap_start).days + 1})
                    gap_start = current_missing_date
                    gap_end = current_missing_date

            # Add the last gap
            gaps.append({'start_date': gap_start.isoformat(), 'end_date': gap_end.isoformat(), 'missing_days': (gap_end - gap_start).days + 1})

        # Calculate coverage
        total_expected_days = len(expected_dates)
        total_missing_days = len(missing_dates_list)
        coverage_percentage = ((total_expected_days - total_missing_days) / total_expected_days * 100) if total_expected_days > 0 else 0

        return APIResponse(data={'gaps': gaps, 'total_gaps': len(gaps), 'missing_days': total_missing_days, 'coverage_percentage': round(coverage_percentage, 2), 'analysis_period': {'from': date_from.isoformat(), 'to': date_to.isoformat(), 'expected_trading_days': total_expected_days, 'actual_data_days': len(existing_dates)}}, message=f"Gap analysis completed: {len(gaps)} gaps found with {total_missing_days} missing days")

    except Exception as e:
        logger.error(f"Error analyzing data gaps for security {security_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to analyze data gaps")


@router.post("/{security_id}/validate", response_model=APIResponse[dict])
async def validate_ohlcv_data(security_id: UUID, date_from: Optional[date] = Query(None, description="Start date for validation"), date_to: Optional[date] = Query(None, description="End date for validation"), timeframe: str = Query(Timeframe.DAILY.value, description="Data timeframe"), db: Session = Depends(get_db), current_user=Depends(get_current_active_user)):
    """Validate OHLCV data integrity for a security."""
    try:
        ohlcv_service = OHLCVService(db)

        # Set default dates if not provided
        if not date_to:
            date_to = date.today()
        if not date_from:
            date_from = date_to - timedelta(days=30)

        # Get OHLCV data
        ohlcv_data = ohlcv_service.get_ohlcv_data(str(security_id), date_from, date_to, timeframe, limit=5000)

        if not ohlcv_data:
            return APIResponse(data={'validation_result': 'NO_DATA', 'total_records': 0, 'valid_records': 0, 'invalid_records': 0, 'data_quality_score': 0.0, 'issues': ['No OHLCV data found for the specified period']}, message="No data available for validation")

        # Validate data using DhanService
        from app.services.dhan_service import DhanService
        dhan_service = DhanService()
        validation_result = dhan_service.validate_ohlcv_data_integrity(ohlcv_data)

        # Determine validation result status
        if validation_result['data_quality_score'] >= 95:
            validation_status = 'EXCELLENT'
        elif validation_result['data_quality_score'] >= 85:
            validation_status = 'GOOD'
        elif validation_result['data_quality_score'] >= 70:
            validation_status = 'FAIR'
        else:
            validation_status = 'POOR'

        validation_result['validation_result'] = validation_status

        return APIResponse(data=validation_result, message=f"Data validation completed: {validation_status} quality score")

    except Exception as e:
        logger.error(f"Error validating OHLCV data for security {security_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to validate OHLCV data")
