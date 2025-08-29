# backend/app/tasks/import_securities.py
"""
Task for importing securities from Dhan with comprehensive progress tracking.
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
    Import securities from Dhan API and update database with comprehensive progress tracking.
    Clean separation: DhanService handles data, SecurityService handles database.
    """
    # Get task ID safely
    task_id = getattr(self.request, 'id', None) or str(uuid.uuid4())
    start_time = datetime.now()

    try:
        # Update task as started
        self._update_task_status(TaskStatus.STARTED, started_at=start_time, current_message='Starting securities import from Dhan...')

        logger.info(f"Starting securities import task {task_id}")

        # Initialize services
        dhan_service = DhanService()
        security_service = SecurityService(self.db)

        # Step 1: Test Connection
        self.start_step('test_connection', 'Test Dhan API Connection', 'Testing connectivity to Dhan API...')
        self._update_progress(5, 'Testing Dhan API connection...')

        try:
            connection_test = dhan_service.test_connection()
            self.complete_step('test_connection', 'Connection test successful', connection_test)
            logger.info(f"Dhan connection test: {connection_test}")
        except Exception as e:
            self.fail_step('test_connection', f'Connection test failed: {str(e)}')
            raise Exception(f"Dhan API connection failed: {str(e)}")

        # Step 2: Get Active Exchanges
        self.start_step('get_exchanges', 'Get Active Exchanges', 'Retrieving active exchanges from database...')
        self._update_progress(8, 'Getting active exchanges from database...')

        try:
            supported_exchanges = security_service.get_active_exchange_codes()
            if not supported_exchanges:
                raise Exception("No active exchanges found in database. Please configure exchanges first.")

            self.complete_step('get_exchanges', f'Found {len(supported_exchanges)} active exchanges', {'exchange_count': len(supported_exchanges), 'exchanges': supported_exchanges})
            logger.info(f"Processing securities for exchanges: {supported_exchanges}")
        except Exception as e:
            self.fail_step('get_exchanges', f'Failed to get exchanges: {str(e)}')
            raise

        # Step 3: Download Raw Data
        self.start_step('download_data', 'Download Securities Master', 'Downloading securities data from Dhan API...')
        self._update_progress(15, 'Downloading securities master from Dhan...')

        try:
            raw_df = dhan_service.download_securities_master_detailed()
            total_downloaded = len(raw_df)

            self.complete_step('download_data', f'Downloaded {total_downloaded} total records', {'total_downloaded': total_downloaded, 'download_timestamp': datetime.now().isoformat()})
            self.log_message('INFO', f'Successfully downloaded {total_downloaded} securities from Dhan API')
        except Exception as e:
            self.fail_step('download_data', f'Download failed: {str(e)}')
            raise

        # Step 4: Filter Securities
        self.start_step('filter_data', 'Filter Securities Data', 'Filtering securities for supported exchanges...')
        self._update_progress(25, f'Filtering securities for exchanges: {", ".join(supported_exchanges)}...')

        try:
            filtered_df = dhan_service.filter_securities_and_futures(raw_df, supported_exchanges)
            total_filtered = len(filtered_df)

            self.complete_step('filter_data', f'Filtered to {total_filtered} relevant securities', {'total_filtered': total_filtered, 'filter_ratio': round((total_filtered / total_downloaded) * 100, 2) if total_downloaded > 0 else 0, 'supported_exchanges': supported_exchanges})
            self.log_message('INFO', f'Filtered {total_filtered} securities from {total_downloaded} total records')
        except Exception as e:
            self.fail_step('filter_data', f'Filtering failed: {str(e)}')
            raise

        # Step 5: Validate and Clean Data
        self.start_step('validate_data', 'Validate & Clean Data', 'Validating and cleaning securities data...')
        self._update_progress(30, 'Validating and cleaning securities data...')

        try:
            clean_df = dhan_service.validate_and_clean_data(filtered_df)
            total_valid = len(clean_df)

            self.complete_step('validate_data', f'Validated {total_valid} clean securities', {'total_valid': total_valid, 'validation_ratio': round((total_valid / total_filtered) * 100, 2) if total_filtered > 0 else 0, 'invalid_count': total_filtered - total_valid})
            self.log_message('INFO', f'Validation complete: {total_valid} valid securities from {total_filtered} filtered records')
        except Exception as e:
            self.fail_step('validate_data', f'Validation failed: {str(e)}')
            raise

        # Step 6: Process Data
        self.start_step('process_data', 'Process Securities Data', 'Processing securities into standardized format...')
        self._update_progress(35, 'Processing securities data...')

        try:
            processed_securities = dhan_service.process_securities_data(clean_df)
            total_processed = len(processed_securities)

            self.complete_step('process_data', f'Processed {total_processed} securities', {'total_processed': total_processed, 'processing_timestamp': datetime.now().isoformat()})
            self.log_message('INFO', f'Data processing complete: {total_processed} securities ready for database')
        except Exception as e:
            self.fail_step('process_data', f'Processing failed: {str(e)}')
            raise

        # Step 7: Database Operations
        self.start_step('database_operations', 'Database Operations', 'Processing securities in database...')
        self._update_progress(45, f'Processing {len(processed_securities)} securities in database using parallel processing...')

        try:
            securities_stats = security_service.process_securities_batch(processed_securities, max_workers=4)

            self.complete_step('database_operations', 'Database operations completed successfully', securities_stats)
            self.log_message('INFO', f'Database processing complete: {securities_stats}')
        except Exception as e:
            self.fail_step('database_operations', f'Database operations failed: {str(e)}')
            raise

        # Step 8: Mark Expired Futures
        self.start_step('expire_futures', 'Mark Expired Futures', 'Marking expired futures as inactive...')
        self._update_progress(70, 'Marking expired futures as inactive...')

        try:
            expired_count = security_service.mark_expired_futures_inactive()

            self.complete_step('expire_futures', f'Marked {expired_count} expired futures as inactive', {'expired_count': expired_count})
            self.log_message('INFO', f'Marked {expired_count} expired futures as inactive')
        except Exception as e:
            self.fail_step('expire_futures', f'Failed to mark expired futures: {str(e)}')
            raise

        # Step 9: Update Derivatives Eligibility
        self.start_step('update_derivatives', 'Update Derivatives Eligibility', 'Updating derivatives eligibility flags...')
        self._update_progress(85, 'Updating derivatives eligibility...')

        try:
            derivatives_eligibility_stats = security_service.update_derivatives_eligibility()

            self.complete_step('update_derivatives', 'Derivatives eligibility updated', derivatives_eligibility_stats)
            self.log_message('INFO', f'Updated derivatives eligibility: {derivatives_eligibility_stats}')
        except Exception as e:
            self.fail_step('update_derivatives', f'Failed to update derivatives eligibility: {str(e)}')
            raise

        # Step 10: Final Statistics (was Step 11)
        self.start_step('final_stats', 'Gather Final Statistics', 'Collecting final import statistics...')
        self._update_progress(95, 'Gathering final statistics...')

        try:
            final_stats = security_service.get_import_statistics()

            self.complete_step('final_stats', 'Final statistics collected', final_stats)
            self.log_message('INFO', f'Final database statistics: {final_stats}')
        except Exception as e:
            self.fail_step('final_stats', f'Failed to gather statistics: {str(e)}')
            # Don't fail the entire task for statistics gathering
            final_stats = {}

        # Calculate final results
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

        # Final progress update
        self._update_progress(100, 'Securities import completed successfully')
        self._update_task_status(TaskStatus.SUCCESS, completed_at=end_time, result_data=result, execution_time_seconds=int(duration))

        logger.info(f"Securities import completed successfully: {result}")
        return result

    except Exception as e:
        error_msg = f"Securities import failed: {str(e)}"
        logger.error(error_msg, exc_info=True)

        # Update task as failed with comprehensive error info
        self._update_task_status(TaskStatus.FAILURE, completed_at=datetime.now(), error_message=error_msg, error_traceback=str(e), current_message=error_msg)

        # Log final error
        self.log_message('ERROR', f'Task failed with error: {error_msg}', {'error_type': type(e).__name__, 'error_details': str(e), 'task_id': task_id})

        self.update_state(state='FAILURE', meta={'current': 0, 'total': 100, 'message': error_msg})
        raise
