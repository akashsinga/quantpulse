# app/services/data_fetchers/ohlcv_fetcher.py

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, text
from sqlalchemy.dialects.postgresql import insert
import uuid

from app.db.session import get_db
from app.db.models.security import Security
from app.db.models.ohlcv_daily import OHLCVDaily
from app.db.models.ohlcv_progress import OHLCVProgress
from .dhan_api_client import DhanAPIClient, DhanAPIError, RateLimitError
from .data_parser import OHLCVDataParser
from .rate_limiter import RateLimitedAPIClient
from app.utils.logger import get_logger

logger = get_logger(__name__)


class OHLCVFetcher:
    """
    Main OHLCV data fetcher service
    Handles both historical and daily data fetching with proper error handling and resumption
    """

    def __init__(self):
        self.api_client = DhanAPIClient()
        self.rate_limited_client = RateLimitedAPIClient(
            requests_per_second=18.0,  # Slightly below 20 to be safe
            circuit_breaker_threshold=10,
            circuit_breaker_timeout=60)
        self.parser = OHLCVDataParser()

        logger.info("Initialized OHLCV Fetcher")

    def fetch_historical_data_for_security(self, security: Security, from_date: str = "2000-01-01", to_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch historical data for a single security
        
        Args:
            security: Security object
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            Dict with status and result info
        """
        result = {'security_id': security.id, 'symbol': security.symbol, 'status': 'failed', 'records_inserted': 0, 'error': None}

        try:
            # Skip non-stock/index securities
            if security.security_type not in ['STOCK', 'INDEX']:
                result['status'] = 'skipped'
                result['error'] = f"Unsupported security type: {security.security_type}"
                return result

            logger.info(f"Fetching historical data for {security.symbol} ({security.security_type})")

            # Get exchange segment and instrument type
            exchange_segment = self.parser.get_exchange_segment(security.security_type)
            instrument = self.parser.get_instrument_type(security.security_type)

            # Fetch data with rate limiting and circuit breaker
            response_data = self.rate_limited_client.execute_request(self.api_client.fetch_historical_data, str(security.external_id), exchange_segment, instrument, from_date, to_date, timeout=30.0)

            # Parse response data
            records = self.parser.parse_historical_response(security.id, response_data, source="dhan_api")

            if not records:
                result['status'] = 'skipped'
                result['error'] = "No valid records in response"
                return result

            # Insert records with conflict resolution (handle duplicates)
            inserted_count = self._bulk_insert_daily_data(records)

            result['status'] = 'success'
            result['records_inserted'] = inserted_count

            logger.info(f"Successfully processed {security.symbol}: {inserted_count} records")

        except RateLimitError as e:
            result['error'] = f"Rate limit error: {e}"
            logger.warning(f"Rate limit hit for {security.symbol}: {e}")

        except DhanAPIError as e:
            result['error'] = f"API error: {e}"
            logger.error(f"API error for {security.symbol}: {e}")

        except Exception as e:
            result['error'] = f"Unexpected error: {e}"
            logger.error(f"Unexpected error for {security.symbol}: {e}")

        return result

    def fetch_today_eod_data(self, securities: List[Security]) -> Dict[str, Any]:
        """
        Fetch today's EOD data for multiple securities
        
        Args:
            securities: List of Security objects
            
        Returns:
            Dict with overall status and results
        """
        result = {'total_securities': len(securities), 'processed': 0, 'inserted': 0, 'errors': []}

        try:
            # Filter valid securities
            valid_securities = [s for s in securities if s.security_type in ['STOCK', 'INDEX']]
            if not valid_securities:
                result['errors'].append("No valid securities to process")
                return result

            logger.info(f"Fetching today EOD data for {len(valid_securities)} securities")

            # Group securities by segment for batch API call
            securities_by_segment = self.parser.group_securities_by_segment(valid_securities)

            # Create security mapping for response parsing
            security_mapping = self.parser.create_security_mapping(valid_securities)

            # Fetch data with rate limiting
            response_data = self.rate_limited_client.execute_request(self.api_client.fetch_today_eod_data, securities_by_segment, timeout=30.0)

            # Parse response data
            records = self.parser.parse_today_eod_response(response_data, security_mapping, source="dhan_api")

            if records:
                # Insert records with conflict resolution
                inserted_count = self._bulk_insert_daily_data(records)
                result['inserted'] = inserted_count
                result['processed'] = len(records)

                logger.info(f"Successfully processed today EOD data: {inserted_count} records")
            else:
                result['errors'].append("No valid records in today EOD response")

        except Exception as e:
            error_msg = f"Error fetching today EOD data: {e}"
            result['errors'].append(error_msg)
            logger.error(error_msg)

        return result

    def _bulk_insert_daily_data(self, records: List[Dict[str, Any]]) -> int:
        """
        Bulk insert OHLCV records with duplicate handling
        
        Args:
            records: List of OHLCV records
            
        Returns:
            Number of records inserted/updated
        """
        if not records:
            return 0

        with get_db() as db:
            try:
                # Use PostgreSQL's ON CONFLICT for upsert
                stmt = insert(OHLCVDaily).values(records)

                # Handle conflicts on primary key (time, security_id)
                stmt = stmt.on_conflict_do_update(index_elements=['time', 'security_id'], set_={'open': stmt.excluded.open, 'high': stmt.excluded.high, 'low': stmt.excluded.low, 'close': stmt.excluded.close, 'volume': stmt.excluded.volume, 'adjusted_close': stmt.excluded.adjusted_close, 'source': stmt.excluded.source})

                result = db.execute(stmt)
                db.commit()

                # PostgreSQL doesn't return affected rows for upsert, so return input count
                return len(records)

            except Exception as e:
                db.rollback()
                logger.error(f"Error in bulk insert: {e}")
                raise

    def get_active_securities(self, security_types: List[str] = None) -> List[Security]:
        """
        Get active securities for OHLCV fetching
        
        Args:
            security_types: List of security types to filter (default: ['STOCK', 'INDEX'])
            
        Returns:
            List of active Security objects
        """
        if security_types is None:
            security_types = ['STOCK', 'INDEX']

        with get_db() as db:
            securities = db.query(Security).filter(and_(Security.is_active == True, Security.security_type.in_(security_types))).all()

            logger.info(f"Found {len(securities)} active securities")
            return securities

    def get_pending_securities(self, operation_type: str = 'historical') -> List[Security]:
        """
        Get securities that need data fetching based on progress tracking
        
        Args:
            operation_type: 'historical' or 'daily'
            
        Returns:
            List of Securities that need processing
        """
        with get_db() as db:
            if operation_type == 'historical':
                # Securities that don't have progress record or failed historical fetch
                securities = db.query(Security).outerjoin(OHLCVProgress, Security.id == OHLCVProgress.security_id).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']), (OHLCVProgress.last_historical_fetch.is_(None) | (OHLCVProgress.status == 'failed')))).all()
            else:  # daily
                # Securities that need daily update (no update today or failed)
                today = date.today()
                securities = db.query(Security).outerjoin(OHLCVProgress, Security.id == OHLCVProgress.security_id).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']), (OHLCVProgress.last_daily_fetch.is_(None) | (OHLCVProgress.last_daily_fetch < today) | (OHLCVProgress.status == 'failed')))).all()

            logger.info(f"Found {len(securities)} securities pending {operation_type} processing")
            return securities

    def update_progress(self, security_id: uuid.UUID, operation_type: str, status: str, error_message: str = None):
        """
        Update progress tracking for a security
        
        Args:
            security_id: Security UUID
            operation_type: 'historical' or 'daily'
            status: 'success', 'failed', 'in_progress'
            error_message: Error message if failed
        """
        with get_db() as db:
            try:
                # Get or create progress record
                progress = db.query(OHLCVProgress).filter(OHLCVProgress.security_id == security_id).first()

                if not progress:
                    progress = OHLCVProgress(security_id=security_id)
                    db.add(progress)

                # Update fields based on operation type
                if operation_type == 'historical':
                    if status == 'success':
                        progress.last_historical_fetch = date.today()
                elif operation_type == 'daily':
                    if status == 'success':
                        progress.last_daily_fetch = date.today()

                progress.status = status
                progress.error_message = error_message
                progress.updated_at = datetime.now()

                if status != 'success':
                    progress.retry_count = (progress.retry_count or 0) + 1
                else:
                    progress.retry_count = 0

                db.commit()

            except Exception as e:
                db.rollback()
                logger.error(f"Error updating progress for {security_id}: {e}")

    def get_progress_summary(self) -> Dict[str, Any]:
        """
        Get overall progress summary
        
        Returns:
            Dict with progress statistics
        """
        with get_db() as db:
            total_securities = db.query(Security).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']))).count()

            # Historical progress
            historical_completed = db.query(OHLCVProgress).filter(and_(OHLCVProgress.last_historical_fetch.isnot(None), OHLCVProgress.status == 'success')).count()

            # Daily progress (today)
            today = date.today()
            daily_completed = db.query(OHLCVProgress).filter(and_(OHLCVProgress.last_daily_fetch == today, OHLCVProgress.status == 'success')).count()

            # Failed records
            failed_count = db.query(OHLCVProgress).filter(OHLCVProgress.status == 'failed').count()

            return {'total_securities': total_securities, 'historical_completed': historical_completed, 'historical_pending': total_securities - historical_completed, 'daily_completed_today': daily_completed, 'failed_securities': failed_count, 'historical_progress_percent': round((historical_completed / total_securities) * 100, 2) if total_securities > 0 else 0}

    def test_api_connection(self) -> bool:
        """
        Test API connection
        
        Returns:
            True if connection successful
        """
        return self.api_client.test_connection()


def create_ohlcv_fetcher() -> OHLCVFetcher:
    """Factory function to create OHLCV fetcher instance"""
    return OHLCVFetcher()
