# app/tasks/ohlcv_import.py - FIXED to use sequential processing

from typing import Dict, List, Any, Optional
from datetime import datetime, date, timedelta
from celery import group, chain, chord
import uuid
import math
import time

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
    FIXED Celery task to fetch historical OHLCV data SEQUENTIALLY
    No more concurrency issues or rate limiting problems
    """
    task_id = self.request.id
    start_time = datetime.now()

    logger.info(f"Starting SEQUENTIAL historical OHLCV fetch task {task_id}")

    # Update task state
    self.update_state(state='PROGRESS', meta={'status': 'Initializing sequential fetcher...', 'progress': 0})

    try:
        # Create SEQUENTIAL fetcher
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
        logger.info(f"Processing {total_securities} securities with SEQUENTIAL fetcher")

        # Progress callback to update task state
        def update_progress(progress_pct):
            self.update_state(
                state='PROGRESS',
                meta={
                    'status': f'Sequential processing... {progress_pct}%',
                    'progress': 15 + int(progress_pct * 0.8),  # 15% to 95%
                    'current_securities': total_securities,
                    'method': 'sequential_safe'
                })

        # Use the SEQUENTIAL fetcher (no concurrency issues)
        self.update_state(state='PROGRESS', meta={'status': 'Starting sequential processing...', 'progress': 15})

        result = fetcher.fetch_historical_data_sequential(securities=securities, from_date=from_date, to_date=to_date, progress_callback=update_progress)

        # Final progress update
        duration = (datetime.now() - start_time).total_seconds()

        final_result = {'status': 'SUCCESS', 'task_id': task_id, 'duration_seconds': round(duration, 2), 'duration_minutes': round(duration / 60, 2), **result, 'method': 'sequential_safe'}

        logger.info(f"Sequential historical OHLCV fetch completed: {final_result}")
        return final_result

    except Exception as e:
        error_msg = f"Sequential historical OHLCV fetch failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        self.update_state(state='FAILURE', meta={'error': error_msg})
        raise


@celery_app.task(bind=True, name="fetch_daily_ohlcv")
def fetch_daily_ohlcv_task(self, security_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    FIXED Celery task to fetch daily OHLCV data SEQUENTIALLY
    """
    task_id = self.request.id
    start_time = datetime.now()

    logger.info(f"Starting SEQUENTIAL daily OHLCV fetch task {task_id}")

    self.update_state(state='PROGRESS', meta={'status': 'Initializing sequential daily fetch...', 'progress': 0})

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

        logger.info(f"Processing {len(securities)} securities for daily data with SEQUENTIAL fetcher")

        # Use sequential EOD data fetching
        self.update_state(state='PROGRESS', meta={'status': 'Fetching today\'s data sequentially...', 'progress': 50})
        result = fetcher.fetch_today_eod_data_sequential(securities)

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

        final_result = {'status': 'SUCCESS', 'task_id': task_id, 'duration_seconds': round(duration, 2), 'total_securities': len(securities), **result, 'method': 'sequential_safe'}

        logger.info(f"Sequential daily OHLCV fetch completed: {final_result}")
        return final_result

    except Exception as e:
        error_msg = f"Sequential daily OHLCV fetch failed: {str(e)}"
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
    Cleanup and retry failed OHLCV fetches using SEQUENTIAL processing
    """
    task_id = self.request.id
    logger.info(f"Starting SEQUENTIAL OHLCV cleanup task {task_id}")

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

            # Start a new SEQUENTIAL historical fetch task for the failed securities
            retry_task = fetch_historical_ohlcv_task.delay(security_ids=security_ids)

            return {'status': 'SUCCESS', 'retried_count': len(security_ids), 'retry_task_id': retry_task.id, 'method': 'sequential_retry', 'message': f'Queued {len(security_ids)} securities for sequential retry'}

    except Exception as e:
        error_msg = f"OHLCV cleanup failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise


@celery_app.task(name="daily_ohlcv_automation")
def daily_ohlcv_automation_task() -> Dict[str, Any]:
    """
    Daily automation task for OHLCV data updates using SEQUENTIAL processing
    """
    logger.info("Starting daily OHLCV automation with SEQUENTIAL processing")

    try:
        # Chain: Daily fetch -> Incremental weekly aggregation (only recent weeks)
        workflow = chain(
            fetch_daily_ohlcv_task.s(),
            generate_weekly_ohlcv_task.si(security_ids=None, weeks_back=2)  # Only recent 2 weeks
        )

        result = workflow.apply_async()

        return {'status': 'SUCCESS', 'workflow_id': result.id, 'message': 'Daily automation workflow started with SEQUENTIAL processing'}

    except Exception as e:
        error_msg = f"Daily automation failed: {str(e)}"
        logger.error(error_msg)
        return {'status': 'FAILED', 'error': error_msg}


@celery_app.task(name="full_ohlcv_import")
def full_ohlcv_import_task(from_date: str = "2000-01-01") -> Dict[str, Any]:
    """
    Complete OHLCV import workflow using SEQUENTIAL processing
    """
    logger.info("Starting full OHLCV import workflow with SEQUENTIAL processing")

    try:
        # Sequential workflow to avoid deadlocks
        workflow = chain(
            fetch_historical_ohlcv_task.s(from_date=from_date),
            generate_weekly_ohlcv_task.si(security_ids=None, weeks_back=1300),  # ~25 years
            setup_continuous_aggregates_task.s())

        result = workflow.apply_async()

        return {'status': 'SUCCESS', 'workflow_id': result.id, 'message': 'Full import workflow started - SEQUENTIAL processing for reliability', 'estimated_completion_time': '45-60 minutes for 25 years of data (sequential processing)', 'method': 'sequential_safe'}

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


# Test task for debugging rate limiting
@celery_app.task(bind=True, name="test_rate_limiting")
def test_rate_limiting_task(self, num_requests: int = 10) -> Dict[str, Any]:
    """
    Test task to verify rate limiting is working correctly
    """
    task_id = self.request.id
    start_time = datetime.now()

    logger.info(f"Starting rate limiting test task {task_id} with {num_requests} requests")

    try:
        fetcher = create_ohlcv_fetcher()

        # Test rate limiter status
        status = fetcher.get_rate_limit_status()
        logger.info(f"Rate limiter status: {status}")

        # Test API connection multiple times
        results = []
        for i in range(num_requests):
            self.update_state(state='PROGRESS', meta={'status': f'Testing request {i+1}/{num_requests}', 'progress': int((i / num_requests) * 100), 'rate_limiter_status': status})

            request_start = time.time()
            success = fetcher.test_api_connection()
            request_duration = time.time() - request_start

            results.append({'request_number': i + 1, 'success': success, 'duration_seconds': round(request_duration, 3), 'timestamp': datetime.now().isoformat()})

            logger.info(f"Request {i+1}: {'SUCCESS' if success else 'FAILED'} in {request_duration:.3f}s")

        duration = (datetime.now() - start_time).total_seconds()

        final_result = {'status': 'SUCCESS', 'task_id': task_id, 'total_requests': num_requests, 'successful_requests': sum(1 for r in results if r['success']), 'failed_requests': sum(1 for r in results if not r['success']), 'duration_seconds': round(duration, 2), 'average_request_duration': round(sum(r['duration_seconds'] for r in results) / len(results), 3), 'requests': results, 'rate_limiter_final_status': fetcher.get_rate_limit_status(), 'method': 'sequential_test'}

        logger.info(f"Rate limiting test completed: {final_result}")
        return final_result

    except Exception as e:
        error_msg = f"Rate limiting test failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        self.update_state(state='FAILURE', meta={'error': error_msg})
        raise


# Benchmark task for testing performance
@celery_app.task(bind=True, name="benchmark_ohlcv_import")
def benchmark_ohlcv_import_task(self, num_securities: int = 10, from_date: str = "2024-01-01") -> Dict[str, Any]:
    """
    Benchmark task to test the SEQUENTIAL performance
    """
    task_id = self.request.id
    start_time = datetime.now()

    logger.info(f"Starting SEQUENTIAL OHLCV import benchmark task {task_id}")
    logger.info(f"Benchmark: {num_securities} securities from {from_date}")

    try:
        fetcher = create_ohlcv_fetcher()

        # Get sample securities for benchmark
        with get_db() as db:
            securities = db.query(Security).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']))).limit(num_securities).all()

        if not securities:
            return {'status': 'FAILED', 'error': 'No securities found for benchmark'}

        logger.info(f"Benchmarking with {len(securities)} securities using SEQUENTIAL processing")

        # Run the sequential import
        result = fetcher.fetch_historical_data_sequential(securities=securities, from_date=from_date, to_date=None, progress_callback=lambda p: self.update_state(state='PROGRESS', meta={'status': f'Benchmarking... {p}%', 'progress': p, 'method': 'sequential_benchmark'}))

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
                'method': 'sequential_coordinated'
            }
        }

        logger.info(f"🚀 SEQUENTIAL BENCHMARK RESULTS: {benchmark_result}")
        return benchmark_result

    except Exception as e:
        error_msg = f"Sequential benchmark failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        self.update_state(state='FAILURE', meta={'error': error_msg})
        raise
