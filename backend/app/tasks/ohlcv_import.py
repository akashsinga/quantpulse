# app/tasks/ohlcv_import.py

from typing import Dict, List, Any, Optional
from datetime import datetime, date, timedelta
from celery import group, chain, chord
import uuid

from app.core.celery_app import celery_app
from app.services.data_fetchers.ohlcv_fetcher import create_ohlcv_fetcher
from app.services.weekly_aggregator import WeeklyDataAggregator
from app.utils.logger import get_logger
from app.db.models.security import Security
from app.db.session import get_db
from sqlalchemy import and_

logger = get_logger(__name__)


@celery_app.task(bind=True, name="fetch_historical_ohlcv")
def fetch_historical_ohlcv_task(self, security_ids: Optional[List[str]] = None, from_date: str = "2000-01-01", to_date: Optional[str] = None, batch_size: int = 50) -> Dict[str, Any]:
    """
    Celery task to fetch historical OHLCV data
    
    Args:
        security_ids: List of security UUIDs to process (None = all pending)
        from_date: Start date for historical data
        to_date: End date for historical data
        batch_size: Number of securities to process in each batch
        
    Returns:
        Dict with task results and statistics
    """
    task_id = self.request.id
    start_time = datetime.now()

    logger.info(f"Starting historical OHLCV fetch task {task_id}")

    # Update task state
    self.update_state(state='PROGRESS', meta={'status': 'Initializing...', 'progress': 0})

    try:
        fetcher = create_ohlcv_fetcher()

        # Test API connection first
        self.update_state(state='PROGRESS', meta={'status': 'Testing API connection...', 'progress': 5})
        if not fetcher.test_api_connection():
            raise Exception("Failed to connect to Dhan API")

        # Get securities to process
        self.update_state(state='PROGRESS', meta={'status': 'Getting securities list...', 'progress': 10})

        if security_ids:
            # Process specific securities
            with get_db() as db:
                securities = db.query(Security).filter(Security.id.in_(security_ids)).all()
        else:
            # Get all pending securities
            securities = fetcher.get_pending_securities('historical')

        if not securities:
            return {'status': 'SUCCESS', 'message': 'No securities to process', 'processed': 0, 'successful': 0, 'failed': 0}

        total_securities = len(securities)
        logger.info(f"Processing {total_securities} securities for historical data")

        # Process securities in batches
        results = {'processed': 0, 'successful': 0, 'failed': 0, 'errors': []}

        for i in range(0, total_securities, batch_size):
            batch = securities[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_securities + batch_size - 1) // batch_size

            progress = int(15 + (i / total_securities) * 70)  # 15% to 85%
            self.update_state(state='PROGRESS', meta={'status': f'Processing batch {batch_num}/{total_batches}...', 'progress': progress, 'processed': results['processed'], 'successful': results['successful'], 'failed': results['failed']})

            # Process batch
            for security in batch:
                try:
                    # Update progress tracking
                    fetcher.update_progress(security.id, 'historical', 'in_progress')

                    # Fetch historical data
                    result = fetcher.fetch_historical_data_for_security(security, from_date, to_date)

                    results['processed'] += 1

                    if result['status'] == 'success':
                        results['successful'] += 1
                        fetcher.update_progress(security.id, 'historical', 'success')
                        logger.info(f"✓ {security.symbol}: {result['records_inserted']} records")
                    else:
                        results['failed'] += 1
                        fetcher.update_progress(security.id, 'historical', 'failed', result['error'])
                        results['errors'].append(f"{security.symbol}: {result['error']}")
                        logger.warning(f"✗ {security.symbol}: {result['error']}")

                except Exception as e:
                    results['processed'] += 1
                    results['failed'] += 1
                    error_msg = f"Unexpected error for {security.symbol}: {e}"
                    results['errors'].append(error_msg)
                    fetcher.update_progress(security.id, 'historical', 'failed', str(e))
                    logger.error(error_msg)

        # Final progress update
        duration = (datetime.now() - start_time).total_seconds()

        final_result = {
            'status': 'SUCCESS',
            'task_id': task_id,
            'duration_seconds': round(duration, 2),
            'total_securities': total_securities,
            'processed': results['processed'],
            'successful': results['successful'],
            'failed': results['failed'],
            'success_rate': round((results['successful'] / results['processed']) * 100, 2) if results['processed'] > 0 else 0,
            'errors': results['errors'][:10]  # Limit error list
        }

        logger.info(f"Historical OHLCV fetch completed: {final_result}")
        return final_result

    except Exception as e:
        error_msg = f"Historical OHLCV fetch failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        self.update_state(state='FAILURE', meta={'error': error_msg})
        raise


@celery_app.task(bind=True, name="fetch_daily_ohlcv")
def fetch_daily_ohlcv_task(self, security_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Celery task to fetch daily OHLCV data (today's data)
    
    Args:
        security_ids: List of security UUIDs to process (None = all active)
        
    Returns:
        Dict with task results
    """
    task_id = self.request.id
    start_time = datetime.now()

    logger.info(f"Starting daily OHLCV fetch task {task_id}")

    self.update_state(state='PROGRESS', meta={'status': 'Initializing daily fetch...', 'progress': 0})

    try:
        fetcher = create_ohlcv_fetcher()

        # Test API connection
        self.update_state(state='PROGRESS', meta={'status': 'Testing API connection...', 'progress': 10})
        if not fetcher.test_api_connection():
            raise Exception("Failed to connect to Dhan API")

        # Get securities to process
        self.update_state(state='PROGRESS', meta={'status': 'Getting securities list...', 'progress': 20})

        if security_ids:
            with get_db() as db:
                securities = db.query(Security).filter(Security.id.in_(security_ids)).all()
        else:
            securities = fetcher.get_pending_securities('daily')

        if not securities:
            return {'status': 'SUCCESS', 'message': 'No securities need daily updates', 'processed': 0, 'inserted': 0}

        logger.info(f"Processing {len(securities)} securities for daily data")

        # Update progress for all securities
        self.update_state(state='PROGRESS', meta={'status': 'Updating progress tracking...', 'progress': 30})
        for security in securities:
            fetcher.update_progress(security.id, 'daily', 'in_progress')

        # Fetch today's data (batched API call)
        self.update_state(state='PROGRESS', meta={'status': 'Fetching today\'s data...', 'progress': 50})
        result = fetcher.fetch_today_eod_data(securities)

        # Update progress based on results
        self.update_state(state='PROGRESS', meta={'status': 'Updating progress...', 'progress': 80})

        if result['inserted'] > 0:
            # Mark successful securities
            for security in securities:
                fetcher.update_progress(security.id, 'daily', 'success')
        else:
            # Mark as failed if no data inserted
            for security in securities:
                error_msg = '; '.join(result['errors']) if result['errors'] else 'No data received'
                fetcher.update_progress(security.id, 'daily', 'failed', error_msg)

        duration = (datetime.now() - start_time).total_seconds()

        final_result = {'status': 'SUCCESS', 'task_id': task_id, 'duration_seconds': round(duration, 2), 'total_securities': result['total_securities'], 'processed': result['processed'], 'inserted': result['inserted'], 'errors': result['errors']}

        logger.info(f"Daily OHLCV fetch completed: {final_result}")
        return final_result

    except Exception as e:
        error_msg = f"Daily OHLCV fetch failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        self.update_state(state='FAILURE', meta={'error': error_msg})
        raise


@celery_app.task(bind=True, name="generate_weekly_ohlcv")
def generate_weekly_ohlcv_task(self, security_ids: Optional[List[str]] = None, weeks_back: int = 4) -> Dict[str, Any]:
    """
    Celery task to generate weekly OHLCV data from daily data
    
    Args:
        security_ids: List of security UUIDs to process (None = all)
        weeks_back: Number of weeks back to regenerate
        
    Returns:
        Dict with task results
    """
    task_id = self.request.id
    start_time = datetime.now()

    logger.info(f"Starting weekly OHLCV generation task {task_id}")

    self.update_state(state='PROGRESS', meta={'status': 'Initializing weekly aggregation...', 'progress': 0})

    try:
        aggregator = WeeklyDataAggregator()

        # Get securities to process
        self.update_state(state='PROGRESS', meta={'status': 'Getting securities list...', 'progress': 10})

        if security_ids:
            security_uuids = [uuid.UUID(sid) for sid in security_ids]
        else:
            security_uuids = None  # Process all

        # Generate weekly data
        self.update_state(state='PROGRESS', meta={'status': 'Generating weekly data...', 'progress': 30})

        result = aggregator.generate_weekly_data(security_ids=security_uuids, weeks_back=weeks_back, progress_callback=lambda p: self.update_state(state='PROGRESS', meta={'status': f'Processing... {p}%', 'progress': 30 + int(p * 0.6)}))

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
    
    Args:
        max_retries: Maximum retry attempts before giving up
        
    Returns:
        Dict with cleanup results
    """
    task_id = self.request.id
    logger.info(f"Starting OHLCV cleanup task {task_id}")

    try:
        fetcher = create_ohlcv_fetcher()

        with fetcher.get_db_session() as db:
            # Get failed records that haven't exceeded max retries
            from app.db.models.ohlcv_progress import OHLCVProgress

            failed_records = db.query(OHLCVProgress).filter(and_(OHLCVProgress.status == 'failed', OHLCVProgress.retry_count < max_retries)).all()

            if not failed_records:
                return {'status': 'SUCCESS', 'message': 'No failed records to retry', 'retried': 0}

            logger.info(f"Found {len(failed_records)} failed records to retry")

            # Reset failed records for retry
            security_ids = []
            for record in failed_records:
                record.status = 'pending'
                security_ids.append(str(record.security_id))

            db.commit()

            # Trigger retry tasks
            historical_task = fetch_historical_ohlcv_task.delay(security_ids=security_ids)

            return {'status': 'SUCCESS', 'retried_count': len(security_ids), 'historical_task_id': historical_task.id, 'message': f'Queued {len(security_ids)} securities for retry'}

    except Exception as e:
        error_msg = f"OHLCV cleanup failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise


@celery_app.task(name="daily_ohlcv_automation")
def daily_ohlcv_automation_task() -> Dict[str, Any]:
    """
    Daily automation task for OHLCV data updates
    Runs daily data fetch followed by weekly aggregation
    """
    logger.info("Starting daily OHLCV automation")

    try:
        # Chain: Daily fetch -> Weekly aggregation
        workflow = chain(fetch_daily_ohlcv_task.s(), generate_weekly_ohlcv_task.s())

        result = workflow.apply_async()

        return {'status': 'SUCCESS', 'workflow_id': result.id, 'message': 'Daily automation workflow started'}

    except Exception as e:
        error_msg = f"Daily automation failed: {str(e)}"
        logger.error(error_msg)
        return {'status': 'FAILED', 'error': error_msg}


@celery_app.task(name="full_ohlcv_import")
def full_ohlcv_import_task(from_date: str = "2000-01-01") -> Dict[str, Any]:
    """
    Complete OHLCV import workflow
    Historical data -> Weekly aggregation
    """
    logger.info("Starting full OHLCV import workflow")

    try:
        # Chain: Historical fetch -> Weekly aggregation
        workflow = chain(fetch_historical_ohlcv_task.s(from_date=from_date), generate_weekly_ohlcv_task.s())

        result = workflow.apply_async()

        return {'status': 'SUCCESS', 'workflow_id': result.id, 'message': 'Full import workflow started'}

    except Exception as e:
        error_msg = f"Full import workflow failed: {str(e)}"
        logger.error(error_msg)
        return {'status': 'FAILED', 'error': error_msg}
