# backend/app/tasks/import_securities.py
"""
Task for importing securities from Dhan.
Uses DhanService for data fetching/processing and SecurityService for database operations.
"""

import uuid
from typing import Dict, Any
from datetime import datetime

from app.core.celery_app import celery_app
from app.core.celery_base import DatabaseTask
from app.services.dhan_service import DhanService
from app.services.security_service import SecurityService
from app.utils.enum import TaskStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)


@celery_app.task(bind=True, base=DatabaseTask, name="import_securities.import_from_dhan")
def import_securities_from_dhan(self) -> Dict[str, Any]:
    """
    Import securities from Dhan API and update database with parallel processing.
    Clean separation: DhanService handles data, SecurityService handles database.
    """
    # Get task ID safely
    task_id = getattr(self.request, 'id', None) or str(uuid.uuid4())
    start_time = datetime.now()

    try:
        # Update task as started
        self._update_task_status(TaskStatus.STARTED, started_at=start_time, current_message='Starting securities import from Dhan...')

        self._update_progress(0, 'Starting securities import from Dhan...')
        logger.info(f"Starting securities import task {task_id}")

        # Initialize services
        dhan_service = DhanService()
        security_service = SecurityService(self.db)

        # Step 1: Test connection
        self._update_progress(5, 'Testing Dhan API connection...')
        connection_test = dhan_service.test_connection()
        logger.info(f"Dhan connection test: {connection_test}")

        # Step 2: Get active exchanges from database
        self._update_progress(8, 'Getting active exchanges from database...')
        supported_exchanges = security_service.get_active_exchange_codes()

        if not supported_exchanges:
            raise Exception("No active exchanges found in database. Please configure exchanges first.")

        logger.info(f"Processing securities for exchanges: {supported_exchanges}")

        # Step 3: Download raw data
        self._update_progress(15, 'Downloading securities master from Dhan...')
        raw_df = dhan_service.download_securities_master_detailed()
        total_downloaded = len(raw_df)

        # Step 4: Filter securities for active exchanges
        self._update_progress(25, f'Filtering securities for exchanges: {", ".join(supported_exchanges)}...')
        filtered_df = dhan_service.filter_securities_and_futures(raw_df, supported_exchanges)

        # Step 5: Validate and clean data
        self._update_progress(30, 'Validating and cleaning securities data...')
        clean_df = dhan_service.validate_and_clean_data(filtered_df)

        # Step 6: Process data into standardized format
        self._update_progress(35, 'Processing securities data...')
        processed_securities = dhan_service.process_securities_data(clean_df)

        # Step 7: Enrich securities with sector information (parallel processing)
        self._update_progress(45, 'Enriching securities with sector information using parallel processing...')
        try:
            # Only enrich equity securities, skip others
            equity_securities = [sec for sec in processed_securities if sec['security_type'] == 'EQUITY']
            if equity_securities:
                enriched_securities = dhan_service.enrich_securities_with_sector_info(equity_securities, batch_size=15, max_workers=3)

                # Update the processed securities with enriched data
                enriched_dict = {sec['external_id']: sec for sec in enriched_securities}
                for i, sec in enumerate(processed_securities):
                    if sec['external_id'] in enriched_dict:
                        processed_securities[i] = enriched_dict[sec['external_id']]

                logger.info(f"Enriched {len(enriched_securities)} securities with sector information")
        except Exception as e:
            logger.warning(f"Sector enrichment failed, continuing without it: {e}")

        # Step 8: Process all securities in database with parallel processing
        self._update_progress(60, f'Processing {len(processed_securities)} securities in database using parallel processing...')
        securities_stats = security_service.process_securities_batch(processed_securities, max_workers=4)

        # Step 9: Mark expired futures as inactive
        self._update_progress(85, 'Marking expired futures as inactive...')
        expired_count = security_service.mark_expired_futures_inactive()

        # Step 10: Update derivatives eligibility
        self._update_progress(90, 'Updating derivatives eligibility...')
        derivatives_eligibility_stats = security_service.update_derivatives_eligibility()

        # Step 11: Get final statistics
        self._update_progress(95, 'Gathering final statistics...')
        final_stats = security_service.get_import_statistics()

        # Final results
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Calculate summary stats
        total_securities_processed = securities_stats['created'] + securities_stats['updated']
        total_derivatives_processed = securities_stats['derivatives_created'] + securities_stats['derivatives_updated']
        total_errors = securities_stats['errors']

        result = {
            'status': 'SUCCESS',
            'task_id': task_id,
            'duration_seconds': round(duration, 2),
            'started_at': start_time.isoformat(),
            'completed_at': end_time.isoformat(),

            # Download and filtering stats
            'total_downloaded': total_downloaded,
            'total_filtered': len(filtered_df),
            'total_valid': len(clean_df),
            'supported_exchanges': supported_exchanges,

            # Processing stats
            'securities_processed': len(processed_securities),
            'securities_stats': securities_stats,
            'expired_futures_marked': expired_count,
            'derivatives_eligibility_updated': derivatives_eligibility_stats,

            # Summary
            'total_securities_processed': total_securities_processed,
            'total_derivatives_processed': total_derivatives_processed,
            'total_errors': total_errors,

            # Performance metrics
            'processing_mode': 'parallel',
            'parallel_workers_used': 4,

            # Final database state
            'database_stats': final_stats
        }

        # Update task as completed
        self._update_progress(100, 'Securities import completed successfully')
        self._update_task_status(TaskStatus.SUCCESS, completed_at=end_time, result_data=result, execution_time_seconds=int(duration))

        logger.info(f"Securities import completed successfully: {result}")
        return result

    except Exception as e:
        error_msg = f"Securities import failed: {str(e)}"
        logger.error(error_msg, exc_info=True)

        # Update task as failed
        self._update_task_status(TaskStatus.FAILURE, completed_at=datetime.now(), error_message=error_msg, error_traceback=str(e), current_message=error_msg)

        self.update_state(state='FAILURE', meta={'current': 0, 'total': 100, 'message': error_msg})
        raise
