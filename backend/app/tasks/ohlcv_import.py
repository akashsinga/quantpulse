# app/tasks/ohlcv_import.py - FIXED to remove problematic task

from typing import Dict, List, Any, Optional
from datetime import datetime, date, timedelta
from celery import group, chain, chord
import uuid
import math

from app.core.celery_app import celery_app
from app.services.data_fetchers.ohlcv_fetcher import create_ohlcv_fetcher
from app.services.weekly_aggregator import WeeklyDataAggregator
from app.utils.logger import get_logger
from app.db.models.security import Security
from app.db.session import get_db
from sqlalchemy import and_
from app.config import settings

logger = get_logger(__name__)


@celery_app.task(bind=True, name="fetch_historical_ohlcv")
def fetch_historical_ohlcv_task(self, security_ids: Optional[List[str]] = None, from_date: str = "2000-01-01", to_date: Optional[str] = None, batch_size: int = 500) -> Dict[str, Any]:
    """
    HIGH-PERFORMANCE Celery task to fetch historical OHLCV data
    Uses distributed rate limiting to prevent API issues
    """
    task_id = self.request.id
    start_time = datetime.now()

    logger.info(f"Starting distributed historical OHLCV fetch task {task_id}")

    # Update task state
    self.update_state(state='PROGRESS', meta={'status': 'Initializing distributed fetcher...', 'progress': 0})

    try:
        # Create distributed fetcher
        fetcher = create_ohlcv_fetcher()

        # Test API connection first
        self.update_state(state='PROGRESS', meta={'status': 'Testing API connection...', 'progress': 5})
        if not fetcher.test_api_connection():
            raise Exception("Failed to connect to Dhan API")

        # Get securities to process
        self.update_state(state='PROGRESS', meta={'status': 'Getting securities list...', 'progress': 10})

        if security_ids:
            # Process specific securities
            security_uuids = [uuid.UUID(sid) for sid in security_ids]
            with get_db() as db:
                securities = db.query(Security).filter(Security.id.in_(security_uuids)).all()
        else:
            # Get all pending securities
            securities = fetcher.get_pending_securities('historical')

        if not securities:
            return {'status': 'SUCCESS', 'message': 'No securities to process', 'processed': 0, 'successful': 0, 'failed': 0}

        total_securities = len(securities)
        logger.info(f"Processing {total_securities} securities with distributed parallel fetcher")

        # Progress callback to update task state
        def update_progress(progress_pct):
            self.update_state(
                state='PROGRESS',
                meta={
                    'status': f'Distributed parallel fetch... {progress_pct}%',
                    'progress': 15 + int(progress_pct * 0.8),  # 15% to 95%
                    'stats': fetcher.get_performance_stats()
                })

        # Use the distributed parallel fetcher
        self.update_state(state='PROGRESS', meta={'status': 'Starting distributed parallel processing...', 'progress': 15})

        result = fetcher.fetch_historical_data_parallel(securities=securities, from_date=from_date, to_date=to_date, progress_callback=update_progress)

        # Final progress update
        duration = (datetime.now() - start_time).total_seconds()

        final_result = {'status': 'SUCCESS', 'task_id': task_id, 'duration_seconds': round(duration, 2), 'duration_minutes': round(duration / 60, 2), **result, 'method': 'distributed_parallel_fetch'}

        logger.info(f"Distributed historical OHLCV fetch completed: {final_result}")
        return final_result

    except Exception as e:
        error_msg = f"Distributed historical OHLCV fetch failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        self.update_state(state='FAILURE', meta={'error': error_msg})
        raise


@celery_app.task(bind=True, name="fetch_daily_ohlcv")
def fetch_daily_ohlcv_task(self, security_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    HIGH-PERFORMANCE Celery task to fetch daily OHLCV data (today's data)
    """
    task_id = self.request.id
    start_time = datetime.now()

    logger.info(f"Starting distributed daily OHLCV fetch task {task_id}")

    self.update_state(state='PROGRESS', meta={'status': 'Initializing distributed daily fetch...', 'progress': 0})

    try:
        fetcher = create_ohlcv_fetcher()

        # Test API connection
        self.update_state(state='PROGRESS', meta={'status': 'Testing API connection...', 'progress': 10})
        if not fetcher.test_api_connection():
            raise Exception("Failed to connect to Dhan API")

        # Get securities to process
        self.update_state(state='PROGRESS', meta={'status': 'Getting securities list...', 'progress': 20})

        if security_ids:
            security_uuids = [uuid.UUID(sid) for sid in security_ids]
            with get_db() as db:
                securities = db.query(Security).filter(Security.id.in_(security_uuids)).all()
        else:
            securities = fetcher.get_pending_securities('daily')

        if not securities:
            return {'status': 'SUCCESS', 'message': 'No securities need daily updates', 'processed': 0, 'inserted': 0}

        logger.info(f"Processing {len(securities)} securities for daily data with distributed fetcher")

        # Use distributed EOD data fetching
        self.update_state(state='PROGRESS', meta={'status': 'Fetching today\'s data with distributed coordination...', 'progress': 50})
        result = fetcher.fetch_today_eod_data_optimized(securities)

        # Update progress based on results
        self.update_state(state='PROGRESS', meta={'status': 'Updating progress...', 'progress': 80})

        # Bulk update progress tracking
        if result.get('inserted', 0) > 0:
            # Mark successful securities
            success_ids = [s.id for s in securities]
            fetcher._bulk_update_progress([{'security_id': sid, 'status': 'success'} for sid in success_ids])
        else:
            # Mark as failed if no data inserted
            error_msg = result.get('error', 'No data received')
            failed_results = [{'security_id': s.id, 'status': 'failed', 'error': error_msg} for s in securities]
            fetcher._bulk_update_progress(failed_results)

        duration = (datetime.now() - start_time).total_seconds()

        final_result = {'status': 'SUCCESS', 'task_id': task_id, 'duration_seconds': round(duration, 2), 'total_securities': len(securities), **result, 'method': 'distributed_eod'}

        logger.info(f"Distributed daily OHLCV fetch completed: {final_result}")
        return final_result

    except Exception as e:
        error_msg = f"Distributed daily OHLCV fetch failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        self.update_state(state='FAILURE', meta={'error': error_msg})
        raise


@celery_app.task(bind=True, name="generate_weekly_ohlcv")
def generate_weekly_ohlcv_task(self, security_ids: Optional[List[str]] = None, weeks_back: int = 4) -> Dict[str, Any]:
    """
    Optimized Celery task to generate weekly OHLCV data from daily data
    """
    task_id = self.request.id
    start_time = datetime.now()

    logger.info(f"Starting optimized weekly OHLCV generation task {task_id}")

    self.update_state(state='PROGRESS', meta={'status': 'Initializing weekly aggregation...', 'progress': 0})

    try:
        # Use optimized aggregator
        aggregator = WeeklyDataAggregator(batch_size=settings.WEEKLY_AGGREGATION_BATCH_SIZE, max_workers=settings.WEEKLY_AGGREGATION_MAX_WORKERS)

        # Get securities to process
        self.update_state(state='PROGRESS', meta={'status': 'Getting securities list...', 'progress': 10})

        if security_ids:
            try:
                if isinstance(security_ids, dict):
                    security_uuids = None
                else:
                    security_uuids = [uuid.UUID(str(sid)) for sid in security_ids if sid]
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid security_ids format: {security_ids}, processing all securities")
                security_uuids = None
        else:
            security_uuids = None

        # Progress callback to update task state
        def update_progress(progress_pct):
            self.update_state(state='PROGRESS', meta={'status': f'Generating weekly data... {progress_pct}%', 'progress': 30 + int(progress_pct * 0.6)})

        # Generate weekly data with optimized processing
        self.update_state(state='PROGRESS', meta={'status': 'Starting weekly data generation...', 'progress': 30})

        result = aggregator.generate_weekly_data(security_ids=security_uuids, weeks_back=weeks_back, progress_callback=update_progress)

        duration = (datetime.now() - start_time).total_seconds()

        final_result = {'status': 'SUCCESS', 'task_id': task_id, 'duration_seconds': round(duration, 2), **result}

        logger.info(f"Weekly OHLCV generation completed: {final_result}")
        return final_result

    except Exception as e:
        error_msg = f"Weekly OHLCV generation failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        self.update_state(state='FAILURE', meta={'error': error_msg})
        raise


@celery_app.task(bind=True, name="cleanup_failed_ohlcv_fetches")
def cleanup_failed_ohlcv_fetches_task(self, max_retries: int = 3) -> Dict[str, Any]:
    """
    Cleanup and retry failed OHLCV fetches
    """
    task_id = self.request.id
    logger.info(f"Starting OHLCV cleanup task {task_id}")

    try:
        from app.db.models.ohlcv_progress import OHLCVProgress

        with get_db() as db:
            # Get failed records that haven't exceeded max retries
            failed_records = db.query(OHLCVProgress).filter(and_(OHLCVProgress.status == 'failed', OHLCVProgress.retry_count < max_retries)).all()

            if not failed_records:
                return {'status': 'SUCCESS', 'message': 'No failed records to retry', 'retried': 0}

            logger.info(f"Found {len(failed_records)} failed records to retry")

            # Reset failed records for retry in batches
            security_ids = []
            batch_size = 100

            for i in range(0, len(failed_records), batch_size):
                batch = failed_records[i:i + batch_size]
                batch_security_ids = []

                for record in batch:
                    record.status = 'pending'
                    batch_security_ids.append(str(record.security_id))

                security_ids.extend(batch_security_ids)
                db.commit()

            # Start a new historical fetch task for the failed securities
            retry_task = fetch_historical_ohlcv_task.delay(security_ids=security_ids)

            return {'status': 'SUCCESS', 'retried_count': len(security_ids), 'retry_task_id': retry_task.id, 'method': 'distributed_retry', 'message': f'Queued {len(security_ids)} securities for retry'}

    except Exception as e:
        error_msg = f"OHLCV cleanup failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise


@celery_app.task(name="daily_ohlcv_automation")
def daily_ohlcv_automation_task() -> Dict[str, Any]:
    """
    Daily automation task for OHLCV data updates
    """
    logger.info("Starting daily OHLCV automation")

    try:
        # Chain: Daily fetch -> Incremental weekly aggregation (only recent weeks)
        workflow = chain(
            fetch_daily_ohlcv_task.s(),
            generate_weekly_ohlcv_task.si(security_ids=None, weeks_back=2)  # Only recent 2 weeks
        )

        result = workflow.apply_async()

        return {'status': 'SUCCESS', 'workflow_id': result.id, 'message': 'Daily automation workflow started'}

    except Exception as e:
        error_msg = f"Daily automation failed: {str(e)}"
        logger.error(error_msg)
        return {'status': 'FAILED', 'error': error_msg}


@celery_app.task(name="full_ohlcv_import")
def full_ohlcv_import_task(from_date: str = "2000-01-01") -> Dict[str, Any]:
    """
    Complete OHLCV import workflow - SIMPLIFIED to avoid Celery deadlocks
    """
    logger.info("Starting full OHLCV import workflow")

    try:
        # Sequential workflow to avoid deadlocks
        workflow = chain(
            fetch_historical_ohlcv_task.s(from_date=from_date),
            generate_weekly_ohlcv_task.si(security_ids=None, weeks_back=1300),  # ~25 years
            setup_continuous_aggregates_task.s())

        result = workflow.apply_async()

        return {'status': 'SUCCESS', 'workflow_id': result.id, 'message': 'Full import workflow started - sequential processing to avoid deadlocks', 'estimated_completion_time': '20-30 minutes for 25 years of data'}

    except Exception as e:
        error_msg = f"Full import workflow failed: {str(e)}"
        logger.error(error_msg)
        return {'status': 'FAILED', 'error': error_msg}


@celery_app.task(name="setup_continuous_aggregates")
def setup_continuous_aggregates_task(previous_result: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Setup TimescaleDB continuous aggregates for automated weekly data generation
    """
    logger.info("Setting up TimescaleDB continuous aggregates")

    try:
        aggregator = WeeklyDataAggregator()
        result = aggregator.create_continuous_aggregate()

        logger.info(f"Continuous aggregate setup result: {result}")

        # Combine with previous result if available
        if previous_result:
            result['previous_task_result'] = previous_result

        return result

    except Exception as e:
        error_msg = f"Continuous aggregate setup failed: {str(e)}"
        logger.error(error_msg)
        return {'status': 'FAILED', 'error': error_msg}


# Benchmark task for testing performance
@celery_app.task(bind=True, name="benchmark_ohlcv_import")
def benchmark_ohlcv_import_task(self, num_securities: int = 100, from_date: str = "2020-01-01") -> Dict[str, Any]:
    """
    Benchmark task to test the distributed performance
    """
    task_id = self.request.id
    start_time = datetime.now()

    logger.info(f"Starting OHLCV import benchmark task {task_id}")
    logger.info(f"Benchmark: {num_securities} securities from {from_date}")

    try:
        fetcher = create_ohlcv_fetcher()

        # Get sample securities for benchmark
        with get_db() as db:
            securities = db.query(Security).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']))).limit(num_securities).all()

        if not securities:
            return {'status': 'FAILED', 'error': 'No securities found for benchmark'}

        logger.info(f"Benchmarking with {len(securities)} securities")

        # Run the distributed import
        result = fetcher.fetch_historical_data_parallel(securities=securities, from_date=from_date, to_date=None, progress_callback=lambda p: self.update_state(state='PROGRESS', meta={'status': f'Benchmarking... {p}%', 'progress': p}))

        duration = (datetime.now() - start_time).total_seconds()

        # Calculate performance metrics
        securities_per_minute = (len(securities) / duration) * 60
        records_per_second = result.get('total_records_inserted', 0) / duration

        benchmark_result = {
            'status': 'SUCCESS',
            'task_id': task_id,
            'benchmark_securities': len(securities),
            'date_range': f"{from_date} to present",
            'duration_seconds': round(duration, 2),
            'duration_minutes': round(duration / 60, 2),
            **result, 'performance_metrics': {
                'securities_per_minute': round(securities_per_minute, 2),
                'records_per_second': round(records_per_second, 2),
                'estimated_time_for_3000_securities': f"{(3000 / securities_per_minute):.1f} minutes",
                'method': 'distributed_coordinated'
            }
        }

        logger.info(f"🚀 BENCHMARK RESULTS: {benchmark_result}")
        return benchmark_result

    except Exception as e:
        error_msg = f"Benchmark failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        self.update_state(state='FAILURE', meta={'error': error_msg})
        raise
