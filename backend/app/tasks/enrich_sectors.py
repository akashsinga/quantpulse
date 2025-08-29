# backend/app/tasks/enrich_sectors.py
"""
Task for enriching securities with sector information from Dhan API.
Separate from main securities import for better performance and flexibility.
"""

import uuid
from typing import Dict, Any, List
from datetime import datetime
from sqlalchemy import and_

from app.core.celery_app import celery_app
from app.core.celery_base import DatabaseTask
from app.services.dhan_service import DhanService
from app.repositories.securities import SecurityRepository
from app.models.securities import Security
from app.utils.enum import TaskStatus, SecurityType
from app.utils.logger import get_logger

logger = get_logger(__name__)


@celery_app.task(bind=True, base=DatabaseTask, name="enrich_sectors.enrich_from_dhan")
def enrich_sectors_from_dhan(self, force_refresh: bool = False) -> Dict[str, Any]:
    """
    Enrich securities with sector information from Dhan API.
    Only processes EQUITY securities that are missing sector data.
    
    Args:
        force_refresh: If True, refresh all securities regardless of existing sector data
    """
    # Get task ID safely
    task_id = getattr(self.request, 'id', None) or str(uuid.uuid4())
    start_time = datetime.now()

    try:
        # Update task as started
        self._update_task_status(TaskStatus.STARTED, started_at=start_time, current_message='Starting sector enrichment from Dhan...')

        logger.info(f"Starting sector enrichment task {task_id} (force_refresh={force_refresh})")

        # Initialize services
        dhan_service = DhanService()
        security_repo = SecurityRepository(self.db)

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

        # Step 2: Find Securities to Enrich
        self.start_step('find_securities', 'Find Securities to Enrich', 'Finding equity securities that need sector data...')
        self._update_progress(10, 'Finding securities that need sector enrichment...')

        try:
            # Build query for securities needing enrichment
            query_conditions = [
                Security.security_type == SecurityType.EQUITY.value,
                Security.is_active == True,
                Security.is_deleted == False,
                Security.isin.isnot(None)  # Only securities with ISIN can be enriched
            ]

            if not force_refresh:
                # Only get securities without sector data
                query_conditions.append(Security.sector.is_(None))

            securities_to_enrich = self.db.query(Security).filter(and_(*query_conditions)).all()

            total_securities = len(securities_to_enrich)

            if total_securities == 0:
                result = {'status': 'SUCCESS', 'message': 'No securities found that need sector enrichment', 'total_securities': 0, 'enriched_count': 0, 'force_refresh': force_refresh}

                self.complete_step('find_securities', 'No securities need enrichment', result)
                self._update_progress(100, 'No securities found that need enrichment')
                self._update_task_status(TaskStatus.SUCCESS, completed_at=datetime.now(), result_data=result)
                return result

            self.complete_step('find_securities', f'Found {total_securities} securities to enrich', {'total_securities': total_securities, 'force_refresh': force_refresh, 'filter_criteria': 'EQUITY securities with ISIN' + (' (all)' if force_refresh else ' (missing sector data)')})

            self.log_message('INFO', f'Found {total_securities} securities needing sector enrichment')

        except Exception as e:
            self.fail_step('find_securities', f'Failed to find securities: {str(e)}')
            raise

        # Step 3: Group Securities by Exchange
        self.start_step('group_securities', 'Group by Exchange', 'Grouping securities by exchange for processing...')
        self._update_progress(15, 'Grouping securities by exchange...')

        try:
            securities_by_exchange = {}
            for security in securities_to_enrich:
                exchange_code = security.exchange.code if security.exchange else 'UNKNOWN'
                if exchange_code not in securities_by_exchange:
                    securities_by_exchange[exchange_code] = []
                securities_by_exchange[exchange_code].append(security)

            exchange_summary = {exchange: len(securities) for exchange, securities in securities_by_exchange.items()}

            self.complete_step('group_securities', f'Grouped into {len(securities_by_exchange)} exchanges', {'exchanges_count': len(securities_by_exchange), 'securities_by_exchange': exchange_summary})

            self.log_message('INFO', f'Securities grouped by exchange: {exchange_summary}')

        except Exception as e:
            self.fail_step('group_securities', f'Failed to group securities: {str(e)}')
            raise

        # Step 4: Process Securities by Exchange
        self.start_step('enrich_data', 'Enrich Sector Data', 'Processing securities by exchange...')

        total_enriched = 0
        total_errors = 0
        exchange_results = {}

        for i, (exchange_code, exchange_securities) in enumerate(securities_by_exchange.items()):
            try:
                progress = 20 + (i / len(securities_by_exchange)) * 70  # 20% to 90%
                self._update_progress(int(progress), f'Processing {len(exchange_securities)} securities for {exchange_code}...')

                # Convert securities to the format expected by DhanService
                securities_data = []
                for security in exchange_securities:
                    securities_data.append({'symbol': security.symbol, 'external_id': security.external_id, 'isin': security.isin, 'security_type': security.security_type})

                # Use DhanService to enrich with sector data
                enriched_securities = dhan_service.enrich_securities_with_sector_info(
                    securities_data,
                    batch_size=15,
                    max_workers=2  # Conservative for manual task
                )

                # Update database with enriched data
                exchange_enriched = 0
                exchange_errors = 0

                for enriched_security in enriched_securities:
                    try:
                        # Find the security in database by external_id
                        security = next((s for s in exchange_securities if s.external_id == enriched_security['external_id']), None)

                        if security and (enriched_security.get('sector') or enriched_security.get('industry')):
                            # Update security with sector/industry data
                            update_data = {}
                            if enriched_security.get('sector'):
                                update_data['sector'] = enriched_security['sector']
                            if enriched_security.get('industry'):
                                update_data['industry'] = enriched_security['industry']

                            if update_data:
                                security_repo.update(security, update_data)
                                exchange_enriched += 1

                    except Exception as e:
                        logger.warning(f"Error updating security {enriched_security.get('symbol', 'unknown')}: {e}")
                        exchange_errors += 1

                total_enriched += exchange_enriched
                total_errors += exchange_errors

                exchange_results[exchange_code] = {'total_securities': len(exchange_securities), 'enriched_count': exchange_enriched, 'error_count': exchange_errors}

                self.log_message('INFO', f'Completed {exchange_code}: enriched {exchange_enriched}/{len(exchange_securities)} securities')

            except Exception as e:
                logger.error(f"Error processing exchange {exchange_code}: {e}")
                total_errors += len(exchange_securities)
                exchange_results[exchange_code] = {'total_securities': len(exchange_securities), 'enriched_count': 0, 'error_count': len(exchange_securities), 'error_message': str(e)}

        # Commit all changes
        try:
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise Exception(f"Failed to commit sector enrichment changes: {str(e)}")

        self.complete_step('enrich_data', f'Enriched {total_enriched} securities', {'total_processed': total_securities, 'enriched_count': total_enriched, 'error_count': total_errors, 'success_rate': round((total_enriched / total_securities) * 100, 2) if total_securities > 0 else 0, 'exchange_results': exchange_results})

        # Step 5: Final Statistics
        self.start_step('final_stats', 'Gather Final Statistics', 'Collecting enrichment statistics...')
        self._update_progress(95, 'Gathering final statistics...')

        try:
            # Count securities with sector data
            total_equity_securities = self.db.query(Security).filter(Security.security_type == SecurityType.EQUITY.value, Security.is_active == True, Security.is_deleted == False).count()

            securities_with_sector = self.db.query(Security).filter(Security.security_type == SecurityType.EQUITY.value, Security.is_active == True, Security.is_deleted == False, Security.sector.isnot(None)).count()

            final_stats = {'total_equity_securities': total_equity_securities, 'securities_with_sector': securities_with_sector, 'sector_coverage_percentage': round((securities_with_sector / total_equity_securities) * 100, 2) if total_equity_securities > 0 else 0}

            self.complete_step('final_stats', 'Final statistics collected', final_stats)

        except Exception as e:
            self.fail_step('final_stats', f'Failed to gather statistics: {str(e)}')
            final_stats = {}

        # Calculate final results
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        result = {
            'status': 'SUCCESS',
            'task_id': task_id,
            'duration_seconds': round(duration, 2),
            'started_at': start_time.isoformat(),
            'completed_at': end_time.isoformat(),

            # Processing stats
            'total_securities_found': total_securities,
            'total_enriched': total_enriched,
            'total_errors': total_errors,
            'success_rate': round((total_enriched / total_securities) * 100, 2) if total_securities > 0 else 0,
            'force_refresh': force_refresh,

            # Exchange breakdown
            'exchange_results': exchange_results,

            # Final database state
            'final_stats': final_stats
        }

        # Final progress update
        self._update_progress(100, f'Sector enrichment completed: {total_enriched}/{total_securities} securities enriched')
        self._update_task_status(TaskStatus.SUCCESS, completed_at=end_time, result_data=result, execution_time_seconds=int(duration))

        logger.info(f"Sector enrichment completed successfully: {result}")
        return result

    except Exception as e:
        error_msg = f"Sector enrichment failed: {str(e)}"
        logger.error(error_msg, exc_info=True)

        # Update task as failed
        self._update_task_status(TaskStatus.FAILURE, completed_at=datetime.now(), error_message=error_msg, error_traceback=str(e), current_message=error_msg)

        # Log final error
        self.log_message('ERROR', f'Task failed with error: {error_msg}', {'error_type': type(e).__name__, 'error_details': str(e), 'task_id': task_id})

        self.update_state(state='FAILURE', meta={'current': 0, 'total': 100, 'message': error_msg})
        raise
