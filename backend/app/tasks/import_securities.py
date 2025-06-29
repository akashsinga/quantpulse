# backend/app/tasks/import_securities.py
"""
Task for importing securities from Dhan.
Uses DhanService for data fetching/processing and SecurityService for database operations.
"""

import uuid
from typing import Dict, Any
from datetime import datetime
from celery import current_task

from app.core.celery_app import celery_app
from app.core.celery_base import BaseTask
from app.core.database import init_database
from app.core.config import settings
from app.services.dhan_service import DhanService
from app.services.security_service import SecurityService
from app.utils.logger import get_logger

logger = get_logger(__name__)


@celery_app.task(bind=True, base=BaseTask, name="import_securities.import_from_dhan")
def import_securities_from_dhan(self) -> Dict[str, Any]:
    """
    Import securities from Dhan API and update database.
    Clean separation: DhanService handles data, SecurityService handles database.
    """
    # Get task ID safely
    task_id = getattr(self.request, 'id', None) or str(uuid.uuid4())
    start_time = datetime.now()

    try:
        self.update_state(state='PROGRESS', meta={'current': 0, 'total': 100, 'message': 'Starting securities import from Dhan...'})
        logger.info(f"Starting securities import task {task_id}")

        # Initialize database connection for this worker
        db_manager = init_database(settings.database.DB_URL)

        # Use database session context manager
        with db_manager.get_session() as db:
            # Initialize services
            dhan_service = DhanService()
            security_service = SecurityService(db)

            # Step 1: Test connection
            self.update_state(state='PROGRESS', meta={'current': 5, 'total': 100, 'message': 'Testing Dhan API connection...'})
            connection_test = dhan_service.test_connection()
            logger.info(f"Dhan connection test: {connection_test}")

            # Step 2: Download raw data
            self.update_state(state='PROGRESS', meta={'current': 10, 'total': 100, 'message': 'Downloading securities master from Dhan...'})
            raw_df = dhan_service.download_securities_master_detailed()
            total_downloaded = len(raw_df)

            # Step 3: Filter securities and futures
            self.update_state(state='PROGRESS', meta={'current': 20, 'total': 100, 'message': 'Filtering NSE securities and futures...'})
            securities_df, futures_df = dhan_service.filter_securities_and_futures(raw_df)

            # Step 4: Validate and clean data
            self.update_state(state='PROGRESS', meta={'current': 25, 'total': 100, 'message': 'Validating and cleaning data...'})
            clean_securities_df, clean_futures_df = dhan_service.validate_and_clean_data(securities_df, futures_df)

            # Step 5: Process data into standardized format
            self.update_state(state='PROGRESS', meta={'current': 30, 'total': 100, 'message': 'Processing securities data...'})
            processed_securities = dhan_service.process_securities_data(clean_securities_df)

            # Step 5.5: Enrich securities with sector information (optional)
            self.update_state(state='PROGRESS', meta={'current': 32, 'total': 100, 'message': 'Enriching securities with sector information...'})
            try:
                # Only enrich equity stocks, skip indices and limit to first 100 for performance
                equity_securities = [sec for sec in processed_securities if sec['security_type'] == 'STOCK']
                if equity_securities:
                    enriched_securities = dhan_service.enrich_securities_with_sector_info(equity_securities, batch_size=5)

                    # Update the processed securities with enriched data
                    enriched_dict = {sec['symbol']: sec for sec in enriched_securities}
                    for i, sec in enumerate(processed_securities):
                        if sec['symbol'] in enriched_dict:
                            processed_securities[i] = enriched_dict[sec['symbol']]

                    logger.info(f"Enriched {len(enriched_securities)} securities with sector information")
            except Exception as e:
                logger.warning(f"Sector enrichment failed, continuing without it: {e}")

            self.update_state(state='PROGRESS', meta={'current': 35, 'total': 100, 'message': 'Processing futures data...'})
            derivative_securities, futures_data = dhan_service.process_futures_data(clean_futures_df)

            # Step 6: Database operations
            self.update_state(state='PROGRESS', meta={'current': 40, 'total': 100, 'message': 'Ensuring NSE exchange exists...'})
            nse_exchange = security_service.ensure_nse_exchange()

            # Step 7: Insert regular securities
            self.update_state(state='PROGRESS', meta={'current': 45, 'total': 100, 'message': f'Processing {len(processed_securities)} securities...'})
            securities_stats = security_service.process_securities_batch(processed_securities, nse_exchange)

            # Step 8: Insert derivative securities (from futures)
            self.update_state(state='PROGRESS', meta={'current': 65, 'total': 100, 'message': f'Processing {len(derivative_securities)} derivative securities...'})
            derivatives_stats = security_service.process_securities_batch(derivative_securities, nse_exchange)

            # Step 9: Insert futures relationships
            self.update_state(state='PROGRESS', meta={'current': 80, 'total': 100, 'message': f'Processing {len(futures_data)} futures relationships...'})
            futures_stats = security_service.process_futures_batch(futures_data)

            # Step 10: Mark expired futures as inactive
            self.update_state(state='PROGRESS', meta={'current': 90, 'total': 100, 'message': 'Marking expired futures as inactive...'})
            expired_count = security_service.mark_expired_futures_inactive()

            # Step 11: Update derivatives eligibility
            self.update_state(state='PROGRESS', meta={'current': 92, 'total': 100, 'message': 'Updating derivatives eligibility...'})
            derivatives_eligibility_stats = security_service.update_derivatives_eligibility()

            # Step 12: Get final statistics
            self.update_state(state='PROGRESS', meta={'current': 95, 'total': 100, 'message': 'Gathering final statistics...'})
            final_stats = security_service.get_import_statistics()
            processing_stats = dhan_service.get_statistics(clean_securities_df, clean_futures_df)

            # Calculate totals
            total_securities_processed = securities_stats['created'] + securities_stats['updated'] + derivatives_stats['created'] + derivatives_stats['updated']
            total_errors = (securities_stats['errors'] + derivatives_stats['errors'] + futures_stats['errors'])

            # Final results
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                'status': 'SUCCESS',
                'task_id': task_id,
                'duration_seconds': round(duration, 2),
                'started_at': start_time.isoformat(),
                'completed_at': end_time.isoformat(),

                # Download stats
                'total_downloaded': total_downloaded,
                'total_filtered_securities': len(securities_df),
                'total_filtered_futures': len(futures_df),
                'total_valid_securities': len(clean_securities_df),
                'total_valid_futures': len(clean_futures_df),

                # Processing stats
                'securities_processed': len(processed_securities),
                'derivatives_processed': len(derivative_securities),
                'futures_processed': len(futures_data),

                # Database operation results
                'securities_stats': securities_stats,
                'derivatives_stats': derivatives_stats,
                'futures_stats': futures_stats,
                'expired_futures_marked': expired_count,
                'derivatives_eligibility_updated': derivatives_eligibility_stats,

                # Summary
                'total_securities_processed': total_securities_processed,
                'total_errors': total_errors,

                # Final database state
                'database_stats': final_stats,
                'processing_stats': processing_stats
            }

            self.update_state(state='PROGRESS', meta={'current': 100, 'total': 100, 'message': 'Securities import completed successfully'})
            logger.info(f"Securities import completed successfully: {result}")

            return result

    except Exception as e:
        error_msg = f"Securities import failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        self.update_state(state='FAILURE', meta={'current': 0, 'total': 100, 'message': error_msg})
        raise


@celery_app.task(bind=True, base=BaseTask, name="import_securities.test_dhan_connection")
def test_dhan_connection(self) -> Dict[str, Any]:
    """Test task to verify Dhan API connection."""
    # Get task ID safely
    task_id = getattr(self.request, 'id', None) or str(uuid.uuid4())

    try:
        self.update_state(state='PROGRESS', meta={'current': 0, 'total': 100, 'message': 'Testing Dhan API connection...'})
        logger.info(f"Starting Dhan connection test task {task_id}")

        dhan_service = DhanService()
        result = dhan_service.test_connection()

        self.update_state(state='PROGRESS', meta={'current': 100, 'total': 100, 'message': 'Connection test completed'})
        logger.info(f"Dhan connection test completed: {result}")
        return result

    except Exception as e:
        error_msg = f"Dhan connection test failed: {str(e)}"
        logger.error(error_msg)
        self.update_state(state='FAILURE', meta={'current': 0, 'total': 100, 'message': error_msg})
        raise


@celery_app.task(bind=True, base=BaseTask, name="import_securities.get_dhan_stats")
def get_dhan_securities_stats(self) -> Dict[str, Any]:
    """Get statistics about Dhan securities without importing."""
    # Get task ID safely
    task_id = getattr(self.request, 'id', None) or str(uuid.uuid4())

    try:
        self.update_state(state='PROGRESS', meta={'current': 0, 'total': 100, 'message': 'Fetching Dhan securities statistics...'})
        logger.info(f"Starting Dhan statistics task {task_id}")

        dhan_service = DhanService()

        # Download and process data
        self.update_state(state='PROGRESS', meta={'current': 20, 'total': 100, 'message': 'Downloading securities data...'})
        raw_df = dhan_service.download_securities_master_detailed()

        self.update_state(state='PROGRESS', meta={'current': 50, 'total': 100, 'message': 'Filtering data...'})
        securities_df, futures_df = dhan_service.filter_securities_and_futures(raw_df)

        self.update_state(state='PROGRESS', meta={'current': 80, 'total': 100, 'message': 'Calculating statistics...'})
        stats = dhan_service.get_statistics(securities_df, futures_df)

        result = {'status': 'success', 'message': 'Statistics retrieved successfully', 'task_id': task_id, 'total_downloaded': len(raw_df), 'statistics': stats}

        self.update_state(state='PROGRESS', meta={'current': 100, 'total': 100, 'message': 'Statistics calculation completed'})
        logger.info(f"Dhan statistics completed: {result}")
        return result

    except Exception as e:
        error_msg = f"Failed to get Dhan securities stats: {str(e)}"
        logger.error(error_msg)
        self.update_state(state='FAILURE', meta={'current': 0, 'total': 100, 'message': error_msg})
        raise
