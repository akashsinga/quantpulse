# app/services/data_fetchers/ohlcv_fetcher.py

import gc
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from app.config import settings
from app.db.models.ohlcv_daily import OHLCVDaily
from app.db.models.ohlcv_progress import OHLCVProgress
from app.db.models.security import Security
from app.db.session import get_db
from app.utils.logger import get_logger
from sqlalchemy import and_, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from .data_parser import OHLCVDataParser
from .dhan_api_client import DhanAPIClient, DhanAPIError, RateLimitError
from .rate_limiter import get_global_rate_limiter

logger = get_logger(__name__)


class SimpleRateLimitedDhanClient:
    """
    SIMPLIFIED Dhan API client with fixed rate limiting
    """

    def __init__(self, client_id: str = None):
        self.client_id = client_id or f"api_client_{uuid.uuid4().hex[:8]}"
        self.api_client = DhanAPIClient()

        # Use the GLOBAL rate limiter (shared across all instances)
        self.rate_limiter = get_global_rate_limiter(requests_per_second=5.0)  # Very conservative

        logger.info(f"Initialized simple rate-limited API client {self.client_id}")

    def fetch_historical_data(self, security_id: str, exchange_segment: str, instrument: str, from_date: str = "2000-01-01", to_date: Optional[str] = None) -> Dict[str, Any]:
        """Fetch historical data with SIMPLE rate limiting"""
        return self._execute_with_rate_limit(self.api_client.fetch_historical_data, security_id, exchange_segment, instrument, from_date, to_date, operation=f"historical_{security_id}")

    def fetch_today_eod_data(self, securities_by_segment: Dict[str, List[str]]) -> Dict[str, Any]:
        """Fetch today's EOD data with SIMPLE rate limiting"""
        return self._execute_with_rate_limit(self.api_client.fetch_today_eod_data, securities_by_segment, operation="today_eod")

    def test_connection(self) -> bool:
        """Test connection with SIMPLE rate limiting"""
        try:
            return self._execute_with_rate_limit(self.api_client.test_connection, operation="connection_test")
        except:
            return False

    def _execute_with_rate_limit(self, api_method, *args, operation: str = "api_call", **kwargs):
        """Execute API method with SIMPLE rate limiting - FIXED VERSION"""
        start_time = time.time()

        # Only log for important operations, not every call
        if operation in ["connection_test", "today_eod"] or "historical" not in operation:
            logger.info(f"[{self.client_id}] Requesting rate limit permission for {operation}")

        # CRITICAL FIX: Always acquire rate limit permission first
        if not self.rate_limiter.acquire(tokens=1, timeout=120.0, client_id=self.client_id):
            error_msg = f"FAILED to acquire rate limit permission for {operation}"
            logger.error(f"[{self.client_id}] {error_msg}")
            raise Exception(error_msg)  # Don't convert to custom exceptions

        try:
            # Execute the API call
            result = api_method(*args, **kwargs)

            duration = time.time() - start_time

            # Only log important operations
            if operation in ["connection_test", "today_eod"] or "historical" not in operation:
                logger.info(f"[{self.client_id}] API call {operation} SUCCESS in {duration:.3f}s")

            return result

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[{self.client_id}] API call {operation} FAILED after {duration:.3f}s: {e}")

            # DON'T convert exceptions - let them bubble up naturally
            raise

    def get_rate_limit_status(self) -> Dict[str, Any]:
        """Get rate limiter status"""
        return self.rate_limiter.get_status()


class ChunkedSequentialOHLCVFetcher:
    """
    CHUNKED SEQUENTIAL OHLCV fetcher - processes securities in chunks to manage memory
    Handles millions of records efficiently without memory issues
    """

    def __init__(self, chunk_size: int = 10):
        self.chunk_size = chunk_size  # Securities per chunk - VERY SMALL for memory safety
        self.parser = OHLCVDataParser()
        self._stats = {'total_processed': 0, 'successful': 0, 'failed': 0, 'total_records': 0, 'chunks_completed': 0, 'start_time': None}

        logger.info(f"Initialized CHUNKED SEQUENTIAL OHLCV fetcher (chunk_size={chunk_size})")

    def fetch_historical_data_sequential(self, securities: List[Security], from_date: str = "2000-01-01", to_date: Optional[str] = None, progress_callback=None) -> Dict[str, Any]:
        """
        FIXED: Fetch historical data with CHUNKED processing to handle large datasets
        Processes securities in chunks to avoid memory issues with millions of records
        """
        start_time = time.time()
        self._stats['start_time'] = start_time

        if not securities:
            return {'status': 'SUCCESS', 'message': 'No securities to process'}

        logger.info(f"Starting CHUNKED SEQUENTIAL fetch for {len(securities)} securities")

        # Filter valid securities
        valid_securities = [s for s in securities if s.security_type in ['STOCK', 'INDEX'] and s.is_active]

        if not valid_securities:
            return {'status': 'SUCCESS', 'message': 'No valid securities to process'}

        # Create ONE API client
        api_client = SimpleRateLimitedDhanClient()

        # Test connection first
        if not api_client.test_connection():
            raise Exception("Failed to connect to Dhan API")

        all_results = []
        total_inserted = 0

        # Create chunks: Process securities in smaller batches to manage memory
        # CRITICAL: Smaller chunks due to large dataset (25 years = ~2000 records per security)
        security_chunks = [valid_securities[i:i + self.chunk_size] for i in range(0, len(valid_securities), self.chunk_size)]

        logger.info(f"Processing {len(valid_securities)} securities in {len(security_chunks)} chunks of {self.chunk_size}")
        logger.info(f"Estimated records per chunk: {self.chunk_size * 2000} records (~{self.chunk_size * 2000 * 1.5 / 1024 / 1024:.1f}MB)")

        # Process each chunk separately
        for chunk_idx, security_chunk in enumerate(security_chunks):
            logger.info(f"=== Processing chunk {chunk_idx + 1}/{len(security_chunks)} ({len(security_chunk)} securities) ===")

            chunk_records = []
            chunk_results = []
            chunk_start_time = time.time()

            # Process securities in this chunk
            for i, security in enumerate(security_chunk):
                try:
                    global_idx = chunk_idx * self.chunk_size + i + 1

                    # Minimal logging - only every 3rd security or important ones
                    if global_idx % 3 == 0 or global_idx <= 2:
                        logger.info(f"Processing security {global_idx}/{len(valid_securities)}: {security.symbol}")

                    # Fetch single security
                    result = self._fetch_single_security_sequential(api_client, security, from_date, to_date)
                    chunk_results.append(result)

                    if result['status'] == 'success' and result.get('records'):
                        chunk_records.extend(result['records'])

                        # Log memory-critical information for large securities
                        if len(result['records']) > 1000:
                            logger.info(f"  → Got {len(result['records'])} records for {security.symbol} (chunk total: {len(chunk_records)})")

                    # Memory check - if chunk getting too large, process it early
                    if len(chunk_records) > 50000:  # 50K records limit per chunk (REDUCED)
                        logger.warning(f"Chunk getting large ({len(chunk_records)} records), processing early...")

                        # Insert current chunk data
                        if chunk_records:
                            chunk_inserted = self._bulk_insert_chunk_records(chunk_records)
                            total_inserted += chunk_inserted
                            logger.info(f"  → Early insert: {chunk_inserted} records")

                            # Clear chunk data from memory
                            chunk_records = []
                            gc.collect()

                    # Update stats
                    self._stats['total_processed'] += 1
                    if result['status'] == 'success':
                        self._stats['successful'] += 1
                    else:
                        self._stats['failed'] += 1

                    # Update progress
                    if progress_callback:
                        progress = int((global_idx / len(valid_securities)) * 70)  # 70% for fetching
                        progress_callback(progress)

                except Exception as e:
                    logger.error(f"Error processing {security.symbol}: {e}")
                    chunk_results.append({'security_id': security.id, 'symbol': security.symbol, 'status': 'failed', 'error': str(e), 'records': []})
                    self._stats['total_processed'] += 1
                    self._stats['failed'] += 1

            # Insert remaining chunk data to database
            chunk_inserted = 0
            if chunk_records:
                logger.info(f"Inserting final chunk {chunk_idx + 1} data: {len(chunk_records)} records")
                chunk_inserted = self._bulk_insert_chunk_records(chunk_records)
                total_inserted += chunk_inserted
                logger.info(f"  → Inserted {chunk_inserted} records for chunk {chunk_idx + 1}")

            # Update progress for this chunk
            self._bulk_update_progress(chunk_results)

            # Add to overall results
            all_results.extend(chunk_results)

            # AGGRESSIVE memory cleanup - CRITICAL for large datasets
            del chunk_records
            del chunk_results
            gc.collect()  # Force garbage collection

            # Update chunk stats
            self._stats['chunks_completed'] += 1
            self._stats['total_records'] += chunk_inserted

            chunk_duration = time.time() - chunk_start_time

            # Update progress
            if progress_callback:
                progress = int(70 + ((chunk_idx + 1) / len(security_chunks)) * 20)  # 70-90% for chunks
                progress_callback(progress)

            logger.info(f"Completed chunk {chunk_idx + 1}/{len(security_chunks)} in {chunk_duration:.1f}s - Inserted {chunk_inserted} records")

            # Force a small delay between chunks to let system recover
            time.sleep(1.0)  # Increased delay for better memory recovery

        if progress_callback:
            progress_callback(100)

        # Calculate final statistics
        duration = time.time() - start_time
        successful = sum(1 for r in all_results if r['status'] == 'success')
        failed = len(all_results) - successful

        result = {
            'status': 'SUCCESS',
            'total_securities': len(securities),
            'valid_securities': len(valid_securities),
            'processed': len(all_results),
            'successful': successful,
            'failed': failed,
            'total_records_inserted': total_inserted,
            'duration_seconds': round(duration, 2),
            'duration_minutes': round(duration / 60, 2),
            'duration_hours': round(duration / 3600, 2),
            'records_per_second': round(total_inserted / duration, 2) if duration > 0 else 0,
            'securities_per_minute': round((len(all_results) / duration) * 60, 2) if duration > 0 else 0,
            'chunks_processed': len(security_chunks),
            'chunk_size': self.chunk_size,
            'method': 'chunked_sequential_safe'
        }

        logger.info(f"Chunked sequential fetch completed: {result}")
        return result

    def _fetch_single_security_sequential(self, client: SimpleRateLimitedDhanClient, security: Security, from_date: str, to_date: Optional[str]) -> Dict[str, Any]:
        """Fetch single security with proper error handling"""
        try:
            # Get exchange segment and instrument type
            exchange_segment = self.parser.get_exchange_segment(security.security_type)
            instrument = self.parser.get_instrument_type(security.security_type)

            # Make the API call (rate limiting handled inside client)
            response_data = client.fetch_historical_data(str(security.external_id), exchange_segment, instrument, from_date, to_date)

            # Parse response data
            records = self.parser.parse_historical_response(security.id, response_data, source="dhan_api_chunked")

            return {'security_id': security.id, 'symbol': security.symbol, 'status': 'success', 'records_count': len(records), 'records': records}

        except Exception as e:
            logger.error(f"Error fetching {security.symbol}: {e}")
            return {'security_id': security.id, 'symbol': security.symbol, 'status': 'failed', 'error': str(e), 'records': []}

    def _bulk_insert_chunk_records(self, chunk_records: List[Dict[str, Any]]) -> int:
        """Insert a chunk of records efficiently"""
        if not chunk_records:
            return 0

        try:
            with get_db() as db:
                stmt = insert(OHLCVDaily).values(chunk_records)
                stmt = stmt.on_conflict_do_update(index_elements=['time', 'security_id'], set_={'open': stmt.excluded.open, 'high': stmt.excluded.high, 'low': stmt.excluded.low, 'close': stmt.excluded.close, 'volume': stmt.excluded.volume, 'adjusted_close': stmt.excluded.adjusted_close, 'source': stmt.excluded.source})

                db.execute(stmt)
                db.commit()

                return len(chunk_records)

        except Exception as e:
            logger.error(f"Chunk insert failed: {e}")
            return 0

    def fetch_today_eod_data_sequential(self, securities: List[Security]) -> Dict[str, Any]:
        """Fetch today's EOD data SEQUENTIALLY"""
        if not securities:
            return {'status': 'SUCCESS', 'message': 'No securities to process'}

        try:
            valid_securities = [s for s in securities if s.security_type in ['STOCK', 'INDEX'] and s.is_active]

            if not valid_securities:
                return {'status': 'SUCCESS', 'message': 'No valid securities to process'}

            # Group by segment
            securities_by_segment = defaultdict(list)
            for security in valid_securities:
                segment = "NSE_EQ" if security.security_type == "STOCK" else "IDX_I"
                securities_by_segment[segment].append(str(security.external_id))

            # Create security mapping
            security_mapping = {str(sec.external_id): sec.id for sec in valid_securities}

            # Use sequential API client
            client = SimpleRateLimitedDhanClient()
            response_data = client.fetch_today_eod_data(dict(securities_by_segment))

            # Parse and insert
            records = self.parser.parse_today_eod_response(response_data, security_mapping, source="dhan_api_sequential")

            if records:
                inserted_count = self._bulk_insert_chunk_records(records)
                return {'status': 'SUCCESS', 'total_securities': len(valid_securities), 'processed': len(records), 'inserted': inserted_count}
            else:
                return {'status': 'SUCCESS', 'total_securities': len(valid_securities), 'processed': 0, 'inserted': 0, 'message': 'No valid EOD data received'}

        except Exception as e:
            logger.error(f"Sequential EOD fetch failed: {e}")
            return {'status': 'FAILED', 'error': str(e), 'total_securities': len(securities), 'processed': 0, 'inserted': 0}

    def test_api_connection(self) -> bool:
        """Test API connection"""
        try:
            client = SimpleRateLimitedDhanClient()
            return client.test_connection()
        except:
            return False

    def get_rate_limit_status(self) -> Dict[str, Any]:
        """Get rate limiter status"""
        try:
            client = SimpleRateLimitedDhanClient()
            return client.get_rate_limit_status()
        except Exception as e:
            return {'error': str(e), 'redis_connected': False}

    def _bulk_update_progress(self, results: List[Dict[str, Any]]):
        """Update progress tracking"""
        try:
            with get_db() as db:
                success_ids = [r['security_id'] for r in results if r['status'] == 'success']
                failed_results = [(r['security_id'], r.get('error', 'Unknown error')) for r in results if r['status'] != 'success']

                today = date.today()

                if success_ids:
                    update_success_query = text("""
                        INSERT INTO ohlcv_fetch_progress (security_id, last_historical_fetch, status, retry_count, updated_at)
                        SELECT unnest(:security_ids), :fetch_date, 'success', 0, NOW()
                        ON CONFLICT (security_id) 
                        DO UPDATE SET 
                            last_historical_fetch = :fetch_date,
                            status = 'success', 
                            retry_count = 0,
                            error_message = NULL,
                            updated_at = NOW()
                    """)
                    db.execute(update_success_query, {'security_ids': success_ids, 'fetch_date': today})

                if failed_results:
                    for security_id, error_msg in failed_results:
                        update_failed_query = text("""
                            INSERT INTO ohlcv_fetch_progress (security_id, status, error_message, retry_count, updated_at)
                            VALUES (:security_id, 'failed', :error_msg, 1, NOW())
                            ON CONFLICT (security_id)
                            DO UPDATE SET 
                                status = 'failed',
                                error_message = :error_msg,
                                retry_count = COALESCE(ohlcv_fetch_progress.retry_count, 0) + 1,
                                updated_at = NOW()
                        """)
                        db.execute(update_failed_query, {'security_id': security_id, 'error_msg': error_msg[:500]})

                db.commit()

                # Only log if significant numbers
                if len(success_ids) > 0 or len(failed_results) > 0:
                    logger.info(f"Updated progress for {len(success_ids)} successful and {len(failed_results)} failed securities")

        except Exception as e:
            logger.error(f"Error updating progress tracking: {e}")

    def get_pending_securities(self, operation_type: str = 'historical') -> List[Security]:
        """Get securities that need data fetching"""
        with get_db() as db:
            if operation_type == 'historical':
                securities = db.query(Security).outerjoin(OHLCVProgress, Security.id == OHLCVProgress.security_id).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']), (OHLCVProgress.last_historical_fetch.is_(None) | (OHLCVProgress.status == 'failed')))).all()
            else:  # daily
                today = date.today()
                securities = db.query(Security).outerjoin(OHLCVProgress, Security.id == OHLCVProgress.security_id).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']), (OHLCVProgress.last_daily_fetch.is_(None) | (OHLCVProgress.last_daily_fetch < today) | (OHLCVProgress.status == 'failed')))).all()

            logger.info(f"Found {len(securities)} securities pending {operation_type} processing")
            return securities

    def get_progress_summary(self) -> Dict[str, Any]:
        """Get overall progress summary"""
        with get_db() as db:
            total_securities = db.query(Security).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']))).count()

            historical_completed = db.query(OHLCVProgress).filter(and_(OHLCVProgress.last_historical_fetch.isnot(None), OHLCVProgress.status == 'success')).count()

            today = date.today()
            daily_completed = db.query(OHLCVProgress).filter(and_(OHLCVProgress.last_daily_fetch == today, OHLCVProgress.status == 'success')).count()

            failed_count = db.query(OHLCVProgress).filter(OHLCVProgress.status == 'failed').count()

            return {'total_securities': total_securities, 'historical_completed': historical_completed, 'historical_pending': total_securities - historical_completed, 'daily_completed_today': daily_completed, 'failed_securities': failed_count, 'historical_progress_percent': round((historical_completed / total_securities) * 100, 2) if total_securities > 0 else 0, 'method': 'chunked_sequential_safe'}

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get real-time performance statistics"""
        current_time = time.time()
        duration = current_time - (self._stats['start_time'] or current_time)

        return {
            'total_processed': self._stats['total_processed'],
            'successful': self._stats['successful'],
            'failed': self._stats['failed'],
            'chunks_completed': self._stats['chunks_completed'],
            'total_records_inserted': self._stats['total_records'],
            'success_rate': (self._stats['successful'] / max(self._stats['total_processed'], 1)) * 100,
            'duration_seconds': round(duration, 2),
            'duration_minutes': round(duration / 60, 2),
            'securities_per_second': round(self._stats['total_processed'] / max(duration, 1), 2),
            'records_per_second': round(self._stats['total_records'] / max(duration, 1), 2),
            'chunk_size': self.chunk_size,
            'method': 'chunked_sequential'
        }


# Factory function updated for chunked fetcher
def create_ohlcv_fetcher(chunk_size: int = 10) -> ChunkedSequentialOHLCVFetcher:
    """Factory function to create CHUNKED SEQUENTIAL OHLCV fetcher with memory-safe chunk size"""
    return ChunkedSequentialOHLCVFetcher(chunk_size=chunk_size)
