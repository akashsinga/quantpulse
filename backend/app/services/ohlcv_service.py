# backend/app/services/ohlcv_service.py
"""
Service for OHLCV data management and operations.
Handles data import, processing, and retrieval with parallel processing support.
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from datetime import date, datetime, timedelta
from sqlalchemy.orm import Session
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from app.repositories.market_data import OHLCVRepository, MarketDataImportLogRepository
from app.repositories.securities import SecurityRepository
from app.services.dhan_service import DhanService
from app.models.securities import Security
from app.utils.logger import get_logger
from app.utils.enum import Timeframe

logger = get_logger(__name__)


class OHLCVService:
    """Service for OHLCV data operations with comprehensive import and processing capabilities"""

    def __init__(self, db: Session):
        self.db = db
        self.ohlcv_repo = OHLCVRepository(db)
        self.security_repo = SecurityRepository(db)
        self.import_log_repo = MarketDataImportLogRepository(db)
        self.dhan_service = DhanService()
        self._lock = threading.Lock()

    def import_ohlcv_data(self, security_id: Optional[str] = None, date_from: date = None, date_to: date = None, timeframe: str = Timeframe.DAILY.value, import_type: str = "INCREMENTAL") -> Dict[str, Any]:
        """
        Import OHLCV data for securities from Dhan API
        
        Args:
            security_id: Specific security ID or None for all active securities
            date_from: Start date for import
            date_to: End date for import  
            timeframe: Data timeframe (DAILY, WEEKLY, etc.)
            import_type: Type of import (FULL, INCREMENTAL, BACKFILL)
        """
        start_time = datetime.now()

        # Set default dates if not provided
        if not date_to:
            date_to = date.today()
        if not date_from:
            date_from = date_to - timedelta(days=30)  # Default 30 days back

        logger.info(f"Starting OHLCV import: {date_from} to {date_to} ({import_type})")

        try:
            # Get securities to process
            if security_id:
                securities = [self.security_repo.get_by_id_or_raise(security_id)]
            else:
                securities = self._get_securities_for_import(import_type)

            if not securities:
                return {'status': 'SUCCESS', 'message': 'No securities found for import', 'total_securities': 0, 'import_stats': {}}

            logger.info(f"Processing {len(securities)} securities for OHLCV import")

            # Process securities with parallel execution
            import_stats = self._process_securities_parallel(securities, date_from, date_to, timeframe, import_type)

            # Create summary import log
            self._create_summary_import_log(securities, date_from, date_to, import_stats, import_type, start_time)

            execution_time = (datetime.now() - start_time).total_seconds()

            result = {'status': 'SUCCESS', 'message': f'OHLCV import completed for {len(securities)} securities', 'total_securities': len(securities), 'date_from': date_from.isoformat(), 'date_to': date_to.isoformat(), 'timeframe': timeframe, 'import_type': import_type, 'execution_time_seconds': round(execution_time, 2), 'import_stats': import_stats}

            logger.info(f"OHLCV import completed: {result}")
            return result

        except Exception as e:
            error_msg = f"OHLCV import failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            # Log failed import
            self._create_failed_import_log(date_from, date_to, error_msg, import_type, start_time)

            return {'status': 'FAILURE', 'message': error_msg, 'total_securities': 0, 'import_stats': {'errors': 1}}

    def _get_securities_for_import(self, import_type: str) -> List[Security]:
        """Get securities that need OHLCV data import"""

        # Get active securities that are tradeable
        base_query = self.db.query(Security).filter(Security.is_active == True, Security.is_tradeable == True, Security.is_deleted == False)

        # For derivatives, we primarily want underlying securities
        # but can also import derivative data if needed
        if import_type == "FULL":
            # Get all tradeable securities
            securities = base_query.all()
        else:
            # For incremental, prioritize equities and indices
            securities = base_query.filter(Security.security_type.in_(['EQUITY', 'INDEX'])).all()

        logger.info(f"Found {len(securities)} securities for {import_type} import")
        return securities

    def _process_securities_parallel(self, securities: List[Security], date_from: date, date_to: date, timeframe: str, import_type: str, max_workers: int = 4) -> Dict[str, int]:
        """Process securities with parallel execution"""

        stats = {'total_processed': 0, 'successful': 0, 'failed': 0, 'records_created': 0, 'records_updated': 0, 'records_skipped': 0}

        # Group securities for batch processing
        batch_size = max(1, len(securities) // max_workers)
        security_batches = [securities[i:i + batch_size] for i in range(0, len(securities), batch_size)]

        logger.info(f"Processing {len(securities)} securities in {len(security_batches)} batches with {max_workers} workers")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit batch processing tasks
            future_to_batch = {}
            for i, batch in enumerate(security_batches):
                future = executor.submit(self._process_security_batch, batch, date_from, date_to, timeframe, import_type, i)
                future_to_batch[future] = i

            # Collect results from all batches
            for future in as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    batch_stats = future.result()

                    # Merge stats thread-safely
                    with self._lock:
                        for key in ['total_processed', 'successful', 'failed', 'records_created', 'records_updated', 'records_skipped']:
                            stats[key] += batch_stats.get(key, 0)

                    logger.info(f"Batch {batch_idx} completed: {batch_stats}")

                except Exception as e:
                    logger.error(f"Batch {batch_idx} failed: {e}")
                    with self._lock:
                        stats['failed'] += len(security_batches[batch_idx])

        return stats

    def _process_security_batch(self, securities: List[Security], date_from: date, date_to: date, timeframe: str, import_type: str, batch_idx: int) -> Dict[str, int]:
        """Process a batch of securities in a single thread"""
        from app.core.database import DatabaseManager
        from app.core.config import settings

        # Create new DB session for this thread
        thread_db_manager = DatabaseManager(settings.database.DB_URL)

        with thread_db_manager.get_session() as thread_db:
            # Create repositories with thread-local session
            ohlcv_repo = OHLCVRepository(thread_db)
            import_log_repo = MarketDataImportLogRepository(thread_db)

            batch_stats = {'total_processed': len(securities), 'successful': 0, 'failed': 0, 'records_created': 0, 'records_updated': 0, 'records_skipped': 0}

            logger.info(f"Batch {batch_idx}: Processing {len(securities)} securities")

            for security in securities:
                try:
                    # Import OHLCV data for this security
                    security_stats = self._import_security_ohlcv(security, date_from, date_to, timeframe, ohlcv_repo, import_log_repo, import_type)

                    batch_stats['successful'] += 1
                    batch_stats['records_created'] += security_stats.get('created', 0)
                    batch_stats['records_updated'] += security_stats.get('updated', 0)
                    batch_stats['records_skipped'] += security_stats.get('skipped', 0)

                except Exception as e:
                    logger.warning(f"Failed to import OHLCV for {security.symbol}: {e}")
                    batch_stats['failed'] += 1
                    continue

            logger.info(f"Batch {batch_idx} completed: {batch_stats}")
            return batch_stats

    def _import_security_ohlcv(self, security: Security, date_from: date, date_to: date, timeframe: str, ohlcv_repo: OHLCVRepository, import_log_repo: MarketDataImportLogRepository, import_type: str) -> Dict[str, int]:
        """Import OHLCV data for a single security"""

        security_stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

        try:
            # Get OHLCV data from Dhan API
            ohlcv_data = self.dhan_service.get_ohlcv_data(security.external_id, date_from, date_to)

            if not ohlcv_data:
                logger.debug(f"No OHLCV data received for {security.symbol}")
                security_stats['skipped'] += 1
                return security_stats

            # Process and store OHLCV data
            for data_point in ohlcv_data:
                try:
                    ohlcv_dict = self._convert_dhan_ohlcv_to_dict(data_point, security.id)

                    existing = ohlcv_repo.get_by_security_and_date(security.id, ohlcv_dict['date'], timeframe)

                    if existing:
                        # Update existing record
                        ohlcv_repo.update(existing, ohlcv_dict)
                        security_stats['updated'] += 1
                    else:
                        # Create new record
                        ohlcv_dict['timeframe'] = timeframe
                        new_ohlcv = ohlcv_repo.create(type(ohlcv_repo.model)(**ohlcv_dict))
                        security_stats['created'] += 1

                except Exception as e:
                    logger.warning(f"Error processing OHLCV data point for {security.symbol}: {e}")
                    security_stats['errors'] += 1
                    continue

            # Create import log for this security
            self._create_security_import_log(security, date_from, date_to, security_stats, import_type, import_log_repo)

        except Exception as e:
            logger.error(f"Error importing OHLCV for security {security.symbol}: {e}")
            security_stats['errors'] += 1

        return security_stats

    def _convert_dhan_ohlcv_to_dict(self, dhan_data: Dict[str, Any], security_id: str) -> Dict[str, Any]:
        """Convert Dhan API OHLCV data to our database format"""

        return {
            'security_id': security_id,
            'date': datetime.strptime(dhan_data['date'], '%Y-%m-%d').date(),
            'open_price': float(dhan_data['open']),
            'high_price': float(dhan_data['high']),
            'low_price': float(dhan_data['low']),
            'close_price': float(dhan_data['close']),
            'volume': int(dhan_data.get('volume', 0)),
            'value': float(dhan_data.get('value', 0)) if dhan_data.get('value') else None,
            'trades': int(dhan_data.get('trades', 0)) if dhan_data.get('trades') else None,
            'data_source': 'DHAN',
            'last_updated': datetime.now(),
            'is_adjusted': False  # Dhan provides raw prices
        }

    def _create_security_import_log(self, security: Security, date_from: date, date_to: date, stats: Dict[str, int], import_type: str, import_log_repo: MarketDataImportLogRepository):
        """Create import log for a security"""

        total_processed = stats['created'] + stats['updated'] + stats['skipped'] + stats['errors']
        status = 'SUCCESS' if stats['errors'] == 0 else 'PARTIAL' if total_processed > stats['errors'] else 'FAILURE'

        log_data = {'security_id': security.id, 'import_date': date.today(), 'date_from': date_from, 'date_to': date_to, 'total_records_processed': total_processed, 'records_created': stats['created'], 'records_updated': stats['updated'], 'records_skipped': stats['skipped'], 'records_failed': stats['errors'], 'status': status, 'data_source': 'DHAN', 'import_type': import_type}

        import_log_repo.create_import_log(log_data)

    def _create_summary_import_log(self, securities: List[Security], date_from: date, date_to: date, stats: Dict[str, int], import_type: str, start_time: datetime):
        """Create summary import log for the entire operation"""

        execution_time = (datetime.now() - start_time).total_seconds()

        log_data = {
            'security_id': None,  # Bulk import
            'import_date': date.today(),
            'date_from': date_from,
            'date_to': date_to,
            'total_records_processed': stats['records_created'] + stats['records_updated'] + stats['records_skipped'],
            'records_created': stats['records_created'],
            'records_updated': stats['records_updated'],
            'records_skipped': stats['records_skipped'],
            'records_failed': stats['failed'],
            'status': 'SUCCESS' if stats['failed'] == 0 else 'PARTIAL',
            'data_source': 'DHAN',
            'import_type': import_type,
            'execution_time_seconds': int(execution_time),
            'api_calls_made': len(securities)  # Approximate
        }

        self.import_log_repo.create_import_log(log_data)

    def _create_failed_import_log(self, date_from: date, date_to: date, error_msg: str, import_type: str, start_time: datetime):
        """Create failed import log"""

        execution_time = (datetime.now() - start_time).total_seconds()

        log_data = {
            'security_id': None,
            'import_date': date.today(),
            'date_from': date_from,
            'date_to': date_to,
            'total_records_processed': 0,
            'records_created': 0,
            'records_updated': 0,
            'records_skipped': 0,
            'records_failed': 1,
            'status': 'FAILURE',
            'data_source': 'DHAN',
            'import_type': import_type,
            'error_message': error_msg[:1000],  # Truncate if too long
            'execution_time_seconds': int(execution_time)
        }

        self.import_log_repo.create_import_log(log_data)

    def get_ohlcv_data(self, security_id: str, date_from: date = None, date_to: date = None, timeframe: str = Timeframe.DAILY.value, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get OHLCV data for a security"""

        # Set default dates if not provided
        if not date_to:
            date_to = date.today()
        if not date_from:
            date_from = date_to - timedelta(days=90)  # Default 90 days

        ohlcv_records = self.ohlcv_repo.get_by_security_date_range(security_id, date_from, date_to, timeframe, limit=limit)

        return [record.to_dict() for record in ohlcv_records]

    def get_data_coverage_summary(self, security_ids: List[str] = None) -> Dict[str, Any]:
        """Get data coverage summary for securities"""

        if security_ids:
            securities = [self.security_repo.get_by_id_or_raise(sid) for sid in security_ids]
        else:
            securities = self.security_repo.get_many_by_field('is_active', True, limit=100)

        summary = {'total_securities': len(securities), 'securities_with_data': 0, 'average_coverage_percentage': 0.0, 'earliest_data_date': None, 'latest_data_date': None, 'securities_coverage': []}

        coverage_percentages = []
        earliest_dates = []
        latest_dates = []

        for security in securities:
            coverage = self.ohlcv_repo.get_data_coverage_stats(security.id)

            if coverage['total_records'] > 0:
                summary['securities_with_data'] += 1
                coverage_percentages.append(coverage['coverage_percentage'])

                if coverage['earliest_date']:
                    earliest_dates.append(coverage['earliest_date'])
                if coverage['latest_date']:
                    latest_dates.append(coverage['latest_date'])

            security_info = {'security_id': str(security.id), 'symbol': security.symbol, 'name': security.name, **coverage}
            summary['securities_coverage'].append(security_info)

        if coverage_percentages:
            summary['average_coverage_percentage'] = round(sum(coverage_percentages) / len(coverage_percentages), 2)

        if earliest_dates:
            summary['earliest_data_date'] = min(earliest_dates)

        if latest_dates:
            summary['latest_data_date'] = max(latest_dates)

        return summary
