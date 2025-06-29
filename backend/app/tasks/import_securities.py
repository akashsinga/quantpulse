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
from app.utils.logger import get_logger

logger = get_logger(__name__)


@celery_app.task(bind=True, base=DatabaseTask, name="import_securities.import_from_dhan")
def import_securities_from_dhan(self) -> Dict[str, Any]:
    """
    Import securities from Dhan API and update database.
    Clean separation: DhanService handles data, SecurityService handles database.
    """
    # Get task ID safely
    task_id = getattr(self.request, 'id', None) or str(uuid.uuid4())
    start_time = datetime.now()

    try:
        self.update_progress(0, 100, "Starting securities import from Dhan...")
        logger.info(f"Starting securities import task {task_id}")

        # Initialize services
        dhan_service = DhanService()
        security_service = SecurityService(self.db)

        # Step 1: Test connection
        self.update_progress(5, 100, "Testing Dhan API connection...")
        connection_test = dhan_service.test_connection()
        logger.info(f"Dhan connection test: {connection_test}")

        # Step 2: Download raw data
        self.update_progress(10, 100, "Downloading securities master from Dhan...")
        raw_df = dhan_service.download_securities_master_detailed()
        total_downloaded = len(raw_df)

        # Step 3: Filter securities and futures
        self.update_progress(20, 100, "Filtering NSE securities and futures...")
        securities_df, futures_df = dhan_service.filter_securities_and_futures(raw_df)

        # Step 4: Validate and clean data
        self.update_progress(25, 100, "Validating and cleaning data...")
        clean_securities_df, clean_futures_df = dhan_service.validate_and_clean_data(securities_df, futures_df)

        # Step 5: Process data into standardized format
        self.update_progress(30, 100, "Processing securities data...")
        processed_securities = dhan_service.process_securities_data(clean_securities_df)

        # Step 5.5: Enrich securities with sector information (optional)
        self.update_progress(32, 100, "Enriching securities with sector information...")
        try:
            # Only enrich equity stocks, skip indices and limit to first 100 for performance
            equity_securities = [sec for sec in processed_securities if sec['security_type'] == 'STOCK'][:100]
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

        self.update_progress(35, 100, "Processing futures data...")
        derivative_securities, futures_data = dhan_service.process_futures_data(clean_futures_df)

        # Step 6: Database operations
        self.update_progress(40, 100, "Ensuring NSE exchange exists...")
        nse_exchange = security_service.ensure_nse_exchange()

        # Step 7: Insert regular securities
        self.update_progress(45, 100, f"Processing {len(processed_securities)} securities...")
        securities_stats = security_service.process_securities_batch(processed_securities, nse_exchange)

        # Step 8: Insert derivative securities (from futures)
        self.update_progress(65, 100, f"Processing {len(derivative_securities)} derivative securities...")
        derivatives_stats = security_service.process_securities_batch(derivative_securities, nse_exchange)

        # Step 9: Insert futures relationships
        self.update_progress(80, 100, f"Processing {len(futures_data)} futures relationships...")
        futures_stats = security_service.process_futures_batch(futures_data)

        # Step 10: Mark expired futures as inactive
        self.update_progress(90, 100, "Marking expired futures as inactive...")
        expired_count = security_service.mark_expired_futures_inactive()

        # Step 11: Update derivatives eligibility
        self.update_progress(92, 100, "Updating derivatives eligibility...")
        derivatives_eligibility_stats = security_service.update_derivatives_eligibility()

        # Step 12: Get final statistics
        self.update_progress(95, 100, "Gathering final statistics...")
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

        self.update_progress(100, 100, "Securities import completed successfully")
        logger.info(f"Securities import completed successfully: {result}")

        return result

    except Exception as e:
        error_msg = f"Securities import failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        self.update_progress(0, 100, error_msg)
        raise


@celery_app.task(bind=True, base=DatabaseTask, name="import_securities.test_dhan_connection")
def test_dhan_connection(self) -> Dict[str, Any]:
    """Test task to verify Dhan API connection."""
    # Get task ID safely
    task_id = getattr(self.request, 'id', None) or str(uuid.uuid4())

    try:
        self.update_progress(0, 100, "Testing Dhan API connection...")
        logger.info(f"Starting Dhan connection test task {task_id}")

        dhan_service = DhanService()
        result = dhan_service.test_connection()

        self.update_progress(100, 100, "Connection test completed")
        logger.info(f"Dhan connection test completed: {result}")
        return result

    except Exception as e:
        error_msg = f"Dhan connection test failed: {str(e)}"
        logger.error(error_msg)
        self.update_progress(0, 100, error_msg)
        raise


@celery_app.task(bind=True, base=DatabaseTask, name="import_securities.get_dhan_stats")
def get_dhan_securities_stats(self) -> Dict[str, Any]:
    """Get statistics about Dhan securities without importing."""
    # Get task ID safely
    task_id = getattr(self.request, 'id', None) or str(uuid.uuid4())

    try:
        self.update_progress(0, 100, "Fetching Dhan securities statistics...")
        logger.info(f"Starting Dhan statistics task {task_id}")

        dhan_service = DhanService()

        # Download and process data
        self.update_progress(20, 100, "Downloading securities data...")
        raw_df = dhan_service.download_securities_master_detailed()

        self.update_progress(50, 100, "Filtering data...")
        securities_df, futures_df = dhan_service.filter_securities_and_futures(raw_df)

        self.update_progress(80, 100, "Calculating statistics...")
        stats = dhan_service.get_statistics(securities_df, futures_df)

        result = {'status': 'success', 'message': 'Statistics retrieved successfully', 'task_id': task_id, 'total_downloaded': len(raw_df), 'statistics': stats}

        self.update_progress(100, 100, "Statistics calculation completed")
        logger.info(f"Dhan statistics completed: {result}")
        return result

    except Exception as e:
        error_msg = f"Failed to get Dhan securities stats: {str(e)}"
        logger.error(error_msg)
        self.update_progress(0, 100, error_msg)
        raise
