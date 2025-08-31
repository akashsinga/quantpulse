# backend/app/tasks/import_ohlcv.py
"""
Celery task for importing OHLCV data from Dhan API.
Comprehensive implementation with detailed progress tracking and error handling.
"""

import uuid
from typing import Dict, Any, Optional
from datetime import datetime, date, timedelta

from app.core.celery_app import celery_app
from app.core.celery_base import DatabaseTask
from app.services.ohlcv_service import OHLCVService
from app.utils.enum import TaskStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)


@celery_app.task(bind=True, base=DatabaseTask, name="import_ohlcv.import_from_dhan")
def import_ohlcv_from_dhan(self, security_id: Optional[str] = None, date_from: Optional[str] = None, date_to: Optional[str] = None, timeframe: str = "1D", import_type: str = "INCREMENTAL", force_update: bool = False) -> Dict[str, Any]:
    """
    Import OHLCV data from Dhan API with comprehensive progress tracking.
    
    Args:
        security_id: Specific security ID or None for all securities
        date_from: Start date (ISO format)
        date_to: End date (ISO format)
        timeframe: Data timeframe
        import_type: Import type (FULL, INCREMENTAL, BACKFILL)
        force_update: Force update of existing data
    """
    # Get task ID safely
    task_id = getattr(self.request, 'id', None) or str(uuid.uuid4())
    start_time = datetime.now()

    try:
        # Update task as started
        self._update_task_status(TaskStatus.STARTED, started_at=start_time, current_message='Starting OHLCV data import from Dhan...')

        logger.info(f"Starting OHLCV import task {task_id} (type={import_type})")

        # Initialize service
        ohlcv_service = OHLCVService(self.db)

        # Parse dates
        if date_from:
            date_from_obj = datetime.strptime(date_from, '%Y-%m-%d').date()
        else:
            date_from_obj = date.today() - timedelta(days=30)

        if date_to:
            date_to_obj = datetime.strptime(date_to, '%Y-%m-%d').date()
        else:
            date_to_obj = date.today()

        # Step 1: Validate Input Parameters
        self.start_step('validate_params', 'Validate Input Parameters', 'Validating import parameters...')
        self._update_progress(5, 'Validating import parameters...')

        try:
            # Validate date range
            if date_from_obj > date_to_obj:
                raise ValueError("date_from cannot be greater than date_to")

            # Validate date range isn't too large (prevent overload)
            max_days = 365 * 2  # 2 years maximum
            if (date_to_obj - date_from_obj).days > max_days:
                raise ValueError(f"Date range too large. Maximum {max_days} days allowed")

            # Validate timeframe
            valid_timeframes = ['1D', '1W', '1M']
            if timeframe not in valid_timeframes:
                raise ValueError(f"Invalid timeframe. Must be one of: {valid_timeframes}")

            # Validate import type
            valid_import_types = ['FULL', 'INCREMENTAL', 'BACKFILL']
            if import_type not in valid_import_types:
                raise ValueError(f"Invalid import type. Must be one of: {valid_import_types}")

            validation_result = {'security_id': security_id, 'date_from': date_from_obj.isoformat(), 'date_to': date_to_obj.isoformat(), 'timeframe': timeframe, 'import_type': import_type, 'force_update': force_update, 'date_range_days': (date_to_obj - date_from_obj).days + 1}

            self.complete_step('validate_params', 'Parameters validated successfully', validation_result)
            logger.info(f"Import parameters validated: {validation_result}")

        except Exception as e:
            self.fail_step('validate_params', f'Parameter validation failed: {str(e)}')
            raise

        # Step 2: Initialize Services and Test Connections
        self.start_step('init_services', 'Initialize Services', 'Initializing services and testing connections...')
        self._update_progress(10, 'Initializing services and testing connections...')

        try:
            # Test Dhan API connection
            from app.services.dhan_service import DhanService
            dhan_service = DhanService()
            connection_test = dhan_service.test_connection()

            self.complete_step('init_services', 'Services initialized and connection tested', {'connection_test': connection_test, 'services_ready': True})
            logger.info("Services initialized successfully")

        except Exception as e:
            self.fail_step('init_services', f'Service initialization failed: {str(e)}')
            raise Exception(f"Failed to initialize services: {str(e)}")

        # Step 3: Determine Securities to Process
        self.start_step('get_securities', 'Get Securities for Import', 'Determining securities to process...')
        self._update_progress(15, 'Getting securities list...')

        try:
            if security_id:
                # Single security import
                from app.repositories.securities import SecurityRepository
                security_repo = SecurityRepository(self.db)
                securities = [security_repo.get_by_id_or_raise(security_id)]
                total_securities = 1
            else:
                # Bulk import - get securities based on import type
                securities = ohlcv_service._get_securities_for_import(import_type)
                total_securities = len(securities)

            if not securities:
                result = {'status': 'SUCCESS', 'message': 'No securities found for import', 'total_securities': 0, 'import_stats': {}}
                self.complete_step('get_securities', 'No securities found', result)
                self._update_progress(100, 'No securities found for import')
                self._update_task_status(TaskStatus.SUCCESS, completed_at=datetime.now(), result_data=result)
                return result

            securities_info = {'total_securities': total_securities, 'security_types': {}, 'exchanges': {}}

            # Collect statistics about securities
            for security in securities:
                # Count by security type
                sec_type = security.security_type
                securities_info['security_types'][sec_type] = securities_info['security_types'].get(sec_type, 0) + 1

                # Count by exchange
                exchange_code = security.exchange.code if security.exchange else 'UNKNOWN'
                securities_info['exchanges'][exchange_code] = securities_info['exchanges'].get(exchange_code, 0) + 1

            self.complete_step('get_securities', f'Found {total_securities} securities to process', securities_info)
            self.log_message('INFO', f'Processing {total_securities} securities for OHLCV import', securities_info)

        except Exception as e:
            self.fail_step('get_securities', f'Failed to get securities: {str(e)}')
            raise

        # Step 4: Import OHLCV Data
        self.start_step('import_data', 'Import OHLCV Data', f'Importing OHLCV data for {total_securities} securities...')
        self._update_progress(20, f'Starting OHLCV import for {total_securities} securities...')

        try:
            # Call the service method for actual import
            import_result = ohlcv_service.import_ohlcv_data(security_id=security_id, date_from=date_from_obj, date_to=date_to_obj, timeframe=timeframe, import_type=import_type)

            # Extract import stats
            import_stats = import_result.get('import_stats', {})

            # Update progress based on import results
            if import_result['status'] == 'SUCCESS':
                self._update_progress(85, 'OHLCV data import completed successfully')
                self.complete_step('import_data', 'OHLCV data imported successfully', import_stats)
            else:
                self.fail_step('import_data', f"Import failed: {import_result.get('message', 'Unknown error')}")
                raise Exception(f"Import failed: {import_result.get('message', 'Unknown error')}")

            logger.info(f"OHLCV import completed: {import_stats}")

        except Exception as e:
            self.fail_step('import_data', f'OHLCV import failed: {str(e)}')
            raise

        # Step 5: Data Quality Validation
        self.start_step('validate_data', 'Validate Imported Data', 'Performing data quality validation...')
        self._update_progress(90, 'Validating imported data quality...')

        try:
            validation_stats = {'total_records_imported': import_stats.get('records_created', 0) + import_stats.get('records_updated', 0), 'validation_passed': True, 'quality_checks': []}

            # Basic validation checks
            if import_stats.get('records_created', 0) > 0 or import_stats.get('records_updated', 0) > 0:
                validation_stats['quality_checks'].append('Records successfully imported')

            if import_stats.get('failed', 0) == 0:
                validation_stats['quality_checks'].append('No import failures detected')

            # Calculate success rate
            total_processed = import_stats.get('total_processed', 0)
            successful = import_stats.get('successful', 0)
            if total_processed > 0:
                success_rate = (successful / total_processed) * 100
                validation_stats['success_rate'] = round(success_rate, 2)
                validation_stats['quality_checks'].append(f'Success rate: {success_rate:.2f}%')

            self.complete_step('validate_data', 'Data quality validation completed', validation_stats)

        except Exception as e:
            # Don't fail the entire task for validation issues
            self.log_message('WARNING', f'Data validation had issues: {str(e)}')
            validation_stats = {'validation_passed': False, 'error': str(e)}

        # Step 6: Generate Final Statistics
        self.start_step('final_stats', 'Generate Final Statistics', 'Collecting final import statistics...')
        self._update_progress(95, 'Generating final statistics...')

        try:
            # Get updated data coverage
            coverage_summary = ohlcv_service.get_data_coverage_summary([str(security.id) for security in securities[:10]]  # Sample first 10
                                                                       )

            final_stats = {'import_summary': import_result, 'data_coverage_sample': coverage_summary, 'validation_results': validation_stats}

            self.complete_step('final_stats', 'Final statistics generated', final_stats)

        except Exception as e:
            self.log_message('WARNING', f'Failed to generate final statistics: {str(e)}')
            final_stats = {'error': str(e)}

        # Calculate final results
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        result = {
            'status': 'SUCCESS',
            'task_id': task_id,
            'duration_seconds': round(duration, 2),
            'started_at': start_time.isoformat(),
            'completed_at': end_time.isoformat(),

            # Import configuration
            'import_config': {
                'security_id': security_id,
                'date_from': date_from_obj.isoformat(),
                'date_to': date_to_obj.isoformat(),
                'timeframe': timeframe,
                'import_type': import_type,
                'force_update': force_update
            },

            # Processing results
            'securities_processed': total_securities,
            'import_stats': import_stats,
            'validation_stats': validation_stats,

            # Final statistics
            'final_stats': final_stats
        }

        # Final progress update
        self._update_progress(100, f'OHLCV import completed: {import_stats.get("successful", 0)}/{total_securities} securities processed')
        self._update_task_status(TaskStatus.SUCCESS, completed_at=end_time, result_data=result, execution_time_seconds=int(duration))

        logger.info(f"OHLCV import task completed successfully: {result}")
        return result

    except Exception as e:
        error_msg = f"OHLCV import failed: {str(e)}"
        logger.error(error_msg, exc_info=True)

        # Update task as failed
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        self._update_task_status(TaskStatus.FAILURE, completed_at=end_time, error_message=error_msg, error_traceback=str(e), current_message=error_msg, execution_time_seconds=int(duration))

        # Log final error
        self.log_message('ERROR', f'OHLCV import task failed: {error_msg}', {'error_type': type(e).__name__, 'error_details': str(e), 'task_id': task_id, 'duration_seconds': duration})

        self.update_state(state='FAILURE', meta={'current': 0, 'total': 100, 'message': error_msg})
        raise


@celery_app.task(bind=True, base=DatabaseTask, name="import_ohlcv.backfill_missing_data")
def backfill_missing_ohlcv_data(self, security_ids: Optional[list] = None, date_from: Optional[str] = None, date_to: Optional[str] = None, max_gap_days: int = 30) -> Dict[str, Any]:
    """
    Backfill missing OHLCV data by identifying gaps and importing missing data.
    
    Args:
        security_ids: List of security IDs to process (None for all)
        date_from: Start date for gap analysis
        date_to: End date for gap analysis
        max_gap_days: Maximum gap size to backfill
    """
    task_id = getattr(self.request, 'id', None) or str(uuid.uuid4())
    start_time = datetime.now()

    try:
        self._update_task_status(TaskStatus.STARTED, started_at=start_time, current_message='Starting OHLCV data backfill...')

        logger.info(f"Starting OHLCV backfill task {task_id}")

        ohlcv_service = OHLCVService(self.db)

        # Parse dates
        if date_from:
            date_from_obj = datetime.strptime(date_from, '%Y-%m-%d').date()
        else:
            date_from_obj = date.today() - timedelta(days=90)

        if date_to:
            date_to_obj = datetime.strptime(date_to, '%Y-%m-%d').date()
        else:
            date_to_obj = date.today()

        # Step 1: Identify Securities with Missing Data
        self.start_step('identify_gaps', 'Identify Data Gaps', 'Identifying securities with missing OHLCV data...')
        self._update_progress(10, 'Identifying data gaps...')

        try:
            from app.repositories.market_data import OHLCVRepository
            ohlcv_repo = OHLCVRepository(self.db)

            if security_ids:
                missing_securities = security_ids
            else:
                # Get securities missing data in the specified range
                missing_securities = ohlcv_repo.get_securities_missing_data(date_from_obj, date_to_obj)

            if not missing_securities:
                result = {'status': 'SUCCESS', 'message': 'No securities found with missing data', 'securities_processed': 0, 'backfill_stats': {}}
                self.complete_step('identify_gaps', 'No gaps found', result)
                self._update_task_status(TaskStatus.SUCCESS, completed_at=datetime.now(), result_data=result)
                return result

            gap_info = {'total_securities_with_gaps': len(missing_securities), 'date_range_analyzed': f"{date_from_obj} to {date_to_obj}", 'max_gap_days_setting': max_gap_days}

            self.complete_step('identify_gaps', f'Found {len(missing_securities)} securities with missing data', gap_info)

        except Exception as e:
            self.fail_step('identify_gaps', f'Failed to identify gaps: {str(e)}')
            raise

        # Step 2: Backfill Missing Data
        self.start_step('backfill_data', 'Backfill Missing Data', f'Backfilling data for {len(missing_securities)} securities...')
        self._update_progress(30, 'Starting backfill process...')

        try:
            backfill_stats = {'securities_processed': 0, 'successful_backfills': 0, 'failed_backfills': 0, 'total_records_added': 0}

            for i, security_id in enumerate(missing_securities):
                try:
                    # Update progress
                    progress = 30 + ((i / len(missing_securities)) * 60)  # 30% to 90%
                    self._update_progress(int(progress), f'Backfilling data for security {i+1}/{len(missing_securities)}...')

                    # Import OHLCV data for this security
                    single_import_result = ohlcv_service.import_ohlcv_data(security_id=str(security_id), date_from=date_from_obj, date_to=date_to_obj, import_type="BACKFILL")

                    if single_import_result['status'] == 'SUCCESS':
                        backfill_stats['successful_backfills'] += 1
                        single_stats = single_import_result.get('import_stats', {})
                        backfill_stats['total_records_added'] += single_stats.get('records_created', 0)
                    else:
                        backfill_stats['failed_backfills'] += 1

                    backfill_stats['securities_processed'] += 1

                except Exception as e:
                    logger.warning(f"Failed to backfill data for security {security_id}: {e}")
                    backfill_stats['failed_backfills'] += 1
                    backfill_stats['securities_processed'] += 1
                    continue

            self.complete_step('backfill_data', 'Backfill process completed', backfill_stats)

        except Exception as e:
            self.fail_step('backfill_data', f'Backfill process failed: {str(e)}')
            raise

        # Final results
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        result = {'status': 'SUCCESS', 'task_id': task_id, 'duration_seconds': round(duration, 2), 'backfill_config': {'date_from': date_from_obj.isoformat(), 'date_to': date_to_obj.isoformat(), 'max_gap_days': max_gap_days}, 'backfill_stats': backfill_stats}

        self._update_progress(100, f'Backfill completed: {backfill_stats["successful_backfills"]}/{len(missing_securities)} securities processed')
        self._update_task_status(TaskStatus.SUCCESS, completed_at=end_time, result_data=result, execution_time_seconds=int(duration))

        logger.info(f"OHLCV backfill task completed: {result}")
        return result

    except Exception as e:
        error_msg = f"OHLCV backfill failed: {str(e)}"
        logger.error(error_msg, exc_info=True)

        self._update_task_status(TaskStatus.FAILURE, completed_at=datetime.now(), error_message=error_msg)
        self.update_state(state='FAILURE', meta={'message': error_msg})
        raise
