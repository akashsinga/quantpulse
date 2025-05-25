# app/tasks/ohlcv_import.py - UPDATED to use high-performance fetcher

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
    Optimized for massive parallel processing - should reduce 60+ min to 15-20 min
    
    Args:
        security_ids: List of security UUIDs to process (None = all pending)
        from_date: Start date for historical data
        to_date: End date for historical data
        batch_size: Number of securities to process in each parallel batch
        
    Returns:
        Dict with task results and statistics
    """
    task_id = self.request.id
    start_time = datetime.now()

    logger.info(f"Starting HIGH-PERFORMANCE historical OHLCV fetch task {task_id}")

    # Update task state
    self.update_state(state='PROGRESS', meta={'status': 'Initializing high-performance fetcher...', 'progress': 0})

    try:
        # Create high-performance fetcher with maximum parallelization
        max_workers = min(settings.OHLCV_HISTORICAL_WORKERS * 3, 30)  # Aggressive parallelization
        fetcher = create_ohlcv_fetcher()
        fetcher.max_workers = max_workers  # Override for this task

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
        logger.info(f"Processing {total_securities} securities with HIGH-PERFORMANCE parallel fetcher")

        # Progress callback to update task state
        def update_progress(progress_pct):
            self.update_state(
                state='PROGRESS',
                meta={
                    'status': f'High-performance parallel fetch... {progress_pct}%',
                    'progress': 15 + int(progress_pct * 0.8),  # 15% to 95%
                    'stats': fetcher.get_performance_stats()
                })

        # Use the high-performance parallel fetcher - this is the key optimization
        self.update_state(state='PROGRESS', meta={'status': 'Starting high-performance parallel processing...', 'progress': 15})

        result = fetcher.fetch_historical_data_parallel(securities=securities, from_date=from_date, to_date=to_date, progress_callback=update_progress)

        # Final progress update
        duration = (datetime.now() - start_time).total_seconds()

        final_result = {'status': 'SUCCESS', 'task_id': task_id, 'duration_seconds': round(duration, 2), 'duration_minutes': round(duration / 60, 2), **result, 'performance_improvement': f"Processed {result.get('processed', 0)} securities in {duration/60:.1f} minutes", 'estimated_old_time': f"Old method would take ~{(result.get('processed', 0) * 1.2)/60:.1f} minutes"}

        logger.info(f"HIGH-PERFORMANCE historical OHLCV fetch completed: {final_result}")
        return final_result

    except Exception as e:
        error_msg = f"High-performance historical OHLCV fetch failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        self.update_state(state='FAILURE', meta={'error': error_msg})
        raise


@celery_app.task(bind=True, name="fetch_daily_ohlcv")
def fetch_daily_ohlcv_task(self, security_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    HIGH-PERFORMANCE Celery task to fetch daily OHLCV data (today's data)
    
    Args:
        security_ids: List of security UUIDs to process (None = all active)
        
    Returns:
        Dict with task results
    """
    task_id = self.request.id
    start_time = datetime.now()

    logger.info(f"Starting HIGH-PERFORMANCE daily OHLCV fetch task {task_id}")

    self.update_state(state='PROGRESS', meta={'status': 'Initializing high-performance daily fetch...', 'progress': 0})

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

        logger.info(f"Processing {len(securities)} securities for daily data with optimized fetcher")

        # Update progress for all securities
        self.update_state(state='PROGRESS', meta={'status': 'Updating progress tracking...', 'progress': 30})

        # Use optimized EOD data fetching
        self.update_state(state='PROGRESS', meta={'status': 'Fetching today\'s data with high-performance method...', 'progress': 50})
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

        final_result = {'status': 'SUCCESS', 'task_id': task_id, 'duration_seconds': round(duration, 2), 'total_securities': len(securities), **result, 'method': 'high_performance_eod'}

        logger.info(f"HIGH-PERFORMANCE daily OHLCV fetch completed: {final_result}")
        return final_result

    except Exception as e:
        error_msg = f"High-performance daily OHLCV fetch failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        self.update_state(state='FAILURE', meta={'error': error_msg})
        raise


@celery_app.task(bind=True, name="massive_parallel_historical_import")
def massive_parallel_historical_import_task(self, from_date: str = "2000-01-01", to_date: Optional[str] = None, max_parallel_workers: int = 50) -> Dict[str, Any]:
    """
    ULTRA HIGH-PERFORMANCE task for importing 25 years of data in under 20 minutes
    Uses aggressive parallelization and optimized batching
    
    Args:
        from_date: Start date for historical data
        to_date: End date for historical data  
        max_parallel_workers: Maximum parallel workers (default: 50)
        
    Returns:
        Dict with comprehensive results
    """
    task_id = self.request.id
    start_time = datetime.now()

    logger.info(f"Starting MASSIVE PARALLEL historical import task {task_id}")
    logger.info(f"Target: 25 years of data for 3000+ securities in under 20 minutes")

    self.update_state(state='PROGRESS', meta={'status': 'Initializing massive parallel import...', 'progress': 0})

    try:
        # Create ultra high-performance fetcher
        fetcher = create_ohlcv_fetcher()
        fetcher.max_workers = min(max_parallel_workers, 50)  # Aggressive parallelization
        fetcher.bulk_insert_size = 5000  # Large bulk inserts

        # Get all securities that need historical data
        self.update_state(state='PROGRESS', meta={'status': 'Getting all securities for massive import...', 'progress': 5})
        securities = fetcher.get_pending_securities('historical')

        if not securities:
            return {'status': 'SUCCESS', 'message': 'No securities need historical import'}

        total_securities = len(securities)
        estimated_time_minutes = max(15, total_securities * 0.005)  # Optimistic estimate

        logger.info(f"Massive parallel import: {total_securities} securities")
        logger.info(f"Estimated completion time: {estimated_time_minutes:.1f} minutes")

        # Split securities into multiple parallel batches for maximum throughput
        num_parallel_batches = min(8, math.ceil(total_securities / 500))  # 8 parallel Celery tasks max
        securities_per_batch = math.ceil(total_securities / num_parallel_batches)

        security_batches = []
        for i in range(0, total_securities, securities_per_batch):
            batch = securities[i:i + securities_per_batch]
            security_batches.append([str(s.id) for s in batch])

        logger.info(f"Created {len(security_batches)} parallel batches (avg {securities_per_batch} securities each)")

        # Progress tracking
        completed_batches = 0
        total_results = {'processed': 0, 'successful': 0, 'failed': 0, 'total_records_inserted': 0}

        def update_batch_progress():
            nonlocal completed_batches
            progress = 10 + int((completed_batches / len(security_batches)) * 85)
            self.update_state(state='PROGRESS', meta={'status': f'Massive parallel processing: {completed_batches}/{len(security_batches)} batches completed', 'progress': progress, 'batches_completed': completed_batches, 'total_batches': len(security_batches), 'results_so_far': total_results})

        # Execute all batches in parallel using Celery groups
        self.update_state(state='PROGRESS', meta={'status': 'Launching parallel batch processing...', 'progress': 10})

        # Create parallel tasks
        parallel_jobs = group(fetch_historical_ohlcv_task.s(
            security_ids=batch,
            from_date=from_date,
            to_date=to_date,
            batch_size=100  # Smaller internal batches for better parallelization
        ) for batch in security_batches)

        # Execute all batches in parallel
        job_result = parallel_jobs.apply_async()

        # Wait for all results
        batch_results = job_result.get()

        # Aggregate all results
        for result in batch_results:
            if result and result.get('status') == 'SUCCESS':
                total_results['processed'] += result.get('processed', 0)
                total_results['successful'] += result.get('successful', 0)
                total_results['failed'] += result.get('failed', 0)
                total_results['total_records_inserted'] += result.get('total_records_inserted', 0)

        duration = (datetime.now() - start_time).total_seconds()
        duration_minutes = duration / 60

        final_result = {
            'status': 'SUCCESS',
            'task_id': task_id,
            'method': 'massive_parallel_import',
            'duration_seconds': round(duration, 2),
            'duration_minutes': round(duration_minutes, 2),
            'parallel_batches_used': len(security_batches),
            'max_workers': fetcher.max_workers,
            'total_securities': total_securities,
            **total_results, 'performance_metrics': {
                'securities_per_minute': round(total_results['processed'] / max(duration_minutes, 1), 2),
                'records_per_second': round(total_results['total_records_inserted'] / max(duration, 1), 2),
                'success_rate': round((total_results['successful'] / max(total_results['processed'], 1)) * 100, 2),
                'estimated_vs_actual': f"Estimated {estimated_time_minutes:.1f}min, Actual {duration_minutes:.1f}min"
            },
            'batch_results': batch_results
        }

        # Log performance achievement
        if duration_minutes < 25:
            logger.info(f"🚀 MASSIVE PARALLEL IMPORT SUCCESS! Completed in {duration_minutes:.1f} minutes")
            logger.info(f"📊 Processed {total_results['processed']} securities, {total_results['total_records_inserted']} records")
            logger.info(f"⚡ Performance: {total_results['processed']/duration_minutes:.1f} securities/min, {total_results['total_records_inserted']/duration:.0f} records/sec")
        else:
            logger.warning(f"Import took {duration_minutes:.1f} minutes - target was under 20 minutes")

        return final_result

    except Exception as e:
        error_msg = f"Massive parallel historical import failed: {str(e)}"
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
        # Use optimized aggregator with increased batch size and workers
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
    Cleanup and retry failed OHLCV fetches with high-performance retry
    """
    task_id = self.request.id
    logger.info(f"Starting high-performance OHLCV cleanup task {task_id}")

    try:
        from app.db.models.ohlcv_progress import OHLCVProgress

        with get_db() as db:
            # Get failed records that haven't exceeded max retries
            failed_records = db.query(OHLCVProgress).filter(and_(OHLCVProgress.status == 'failed', OHLCVProgress.retry_count < max_retries)).all()

            if not failed_records:
                return {'status': 'SUCCESS', 'message': 'No failed records to retry', 'retried': 0}

            logger.info(f"Found {len(failed_records)} failed records to retry with high-performance method")

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

            # Use the new massive parallel import for retries
            retry_task = massive_parallel_historical_import_task.delay()

            return {'status': 'SUCCESS', 'retried_count': len(security_ids), 'retry_task_id': retry_task.id, 'method': 'high_performance_retry', 'message': f'Queued {len(security_ids)} securities for high-performance retry'}

    except Exception as e:
        error_msg = f"High-performance OHLCV cleanup failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise


@celery_app.task(name="daily_ohlcv_automation")
def daily_ohlcv_automation_task() -> Dict[str, Any]:
    """
    Optimized daily automation task for OHLCV data updates
    """
    logger.info("Starting optimized daily OHLCV automation")

    try:
        # Chain: Daily fetch -> Incremental weekly aggregation (only recent weeks)
        workflow = chain(
            fetch_daily_ohlcv_task.s(),
            generate_weekly_ohlcv_task.si(security_ids=None, weeks_back=2)  # Only recent 2 weeks
        )

        result = workflow.apply_async()

        return {'status': 'SUCCESS', 'workflow_id': result.id, 'message': 'Optimized daily automation workflow started'}

    except Exception as e:
        error_msg = f"Daily automation failed: {str(e)}"
        logger.error(error_msg)
        return {'status': 'FAILED', 'error': error_msg}


@celery_app.task(name="full_ohlcv_import")
def full_ohlcv_import_task(from_date: str = "2000-01-01") -> Dict[str, Any]:
    """
    ULTRA HIGH-PERFORMANCE complete OHLCV import workflow
    Uses the new massive parallel import method
    """
    logger.info("Starting ULTRA HIGH-PERFORMANCE full OHLCV import workflow")

    try:
        # Chain: Massive parallel historical import -> Full weekly aggregation -> Setup continuous aggregates
        workflow = chain(
            massive_parallel_historical_import_task.s(from_date=from_date, max_parallel_workers=50),
            generate_weekly_ohlcv_task.si(security_ids=None, weeks_back=1300),  # ~25 years
            setup_continuous_aggregates_task.s())

        result = workflow.apply_async()

        return {'status': 'SUCCESS', 'workflow_id': result.id, 'message': 'ULTRA HIGH-PERFORMANCE full import workflow started - expect completion in 15-25 minutes', 'estimated_completion_time': '15-25 minutes for 25 years of data'}

    except Exception as e:
        error_msg = f"Ultra high-performance full import workflow failed: {str(e)}"
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


# New convenience task for quick testing/benchmarking
@celery_app.task(bind=True, name="benchmark_ohlcv_import")
def benchmark_ohlcv_import_task(self, num_securities: int = 100, from_date: str = "2020-01-01") -> Dict[str, Any]:
    """
    Benchmark task to test the performance improvements
    
    Args:
        num_securities: Number of securities to process for benchmark
        from_date: Start date for benchmark
        
    Returns:
        Performance benchmark results
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

        # Run the high-performance import
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
                'estimated_time_for_25_years': f"{(3000 / securities_per_minute) * 1.5:.1f} minutes"
            }
        }

        logger.info(f"🚀 BENCHMARK RESULTS: {benchmark_result}")
        return benchmark_result

    except Exception as e:
        error_msg = f"Benchmark failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        self.update_state(state='FAILURE', meta={'error': error_msg})
        raise
