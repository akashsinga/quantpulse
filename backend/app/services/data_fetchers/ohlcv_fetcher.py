# app/services/data_fetchers/ohlcv_fetcher.py

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, text
from sqlalchemy.dialects.postgresql import insert
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import uuid
import time
import threading

from app.db.session import get_db
from app.db.models.security import Security
from app.db.models.ohlcv_daily import OHLCVDaily
from app.db.models.ohlcv_progress import OHLCVProgress
from .dhan_api_client import DhanAPIClient, DhanAPIError, RateLimitError
from .data_parser import OHLCVDataParser
from .rate_limiter import create_distributed_rate_limiter
from app.utils.logger import get_logger
from app.config import settings

logger = get_logger(__name__)


class DistributedRateLimitedDhanClient:
    """
    Dhan API client with Redis distributed rate limiting
    Coordinates API calls across all workers
    """

    def __init__(self, client_id: str = None):
        self.client_id = client_id or f"ohlcv_worker_{uuid.uuid4().hex[:8]}"
        self.api_client = DhanAPIClient()

        # Single distributed rate limiter for all API calls
        try:
            self.rate_limiter = create_distributed_rate_limiter(requests_per_second=15.0  # Conservative - 15/sec vs Dhan's 20/sec
                                                                )
            logger.info(f"Initialized distributed API client {self.client_id} with 15 req/sec global limit")
        except Exception as e:
            logger.error(f"CRITICAL: Failed to initialize Redis rate limiter: {e}")
            raise Exception(f"Cannot proceed without distributed rate limiting: {e}")

    def fetch_historical_data(self, security_id: str, exchange_segment: str, instrument: str, from_date: str = "2000-01-01", to_date: Optional[str] = None) -> Dict[str, Any]:
        """Fetch historical data with distributed rate limiting"""
        return self._execute_with_coordination(self.api_client.fetch_historical_data, security_id, exchange_segment, instrument, from_date, to_date, operation=f"historical_{security_id}")

    def fetch_today_eod_data(self, securities_by_segment: Dict[str, List[str]]) -> Dict[str, Any]:
        """Fetch today's EOD data with distributed rate limiting"""
        return self._execute_with_coordination(self.api_client.fetch_today_eod_data, securities_by_segment, operation="today_eod")

    def test_connection(self) -> bool:
        """Test connection with distributed rate limiting"""
        try:
            return self._execute_with_coordination(self.api_client.test_connection, operation="connection_test")
        except:
            return False

    def _execute_with_coordination(self, api_method, *args, operation: str = "api_call", **kwargs):
        """Execute API method with STRICT distributed coordination"""
        start_time = time.time()

        # CRITICAL: Always acquire rate limit permission first
        logger.info(f"[{self.client_id}] Requesting rate limit permission for {operation}")

        # Use longer timeout to ensure we get permission
        if not self.rate_limiter.acquire(tokens=1, timeout=60.0, client_id=self.client_id):
            error_msg = f"FAILED to acquire rate limit permission for {operation} after 60s timeout"
            logger.error(f"[{self.client_id}] {error_msg}")
            # Use standard exception for Celery compatibility
            raise TimeoutError(error_msg)

        logger.info(f"[{self.client_id}] Rate limit permission GRANTED for {operation}")

        try:
            # Execute the API call
            result = api_method(*args, **kwargs)

            # Record success
            self.rate_limiter.record_api_success(self.client_id)

            duration = time.time() - start_time
            logger.info(f"[{self.client_id}] API call {operation} SUCCESS in {duration:.3f}s")

            return result

        except RateLimitError as e:
            # Record failure and convert to standard exception
            self.rate_limiter.record_api_failure(self.client_id, str(e))
            duration = time.time() - start_time
            logger.error(f"[{self.client_id}] API call {operation} RATE LIMITED after {duration:.3f}s: {e}")
            raise TimeoutError(f"Rate limit error: {str(e)}")

        except DhanAPIError as e:
            # Only record API failures, not connection/SSL errors
            if "rate limit" in str(e).lower() or "429" in str(e):
                self.rate_limiter.record_api_failure(self.client_id, str(e))
            else:
                # Don't penalize circuit breaker for connection issues
                logger.warning(f"[{self.client_id}] API error (not counting as circuit breaker failure): {e}")

            duration = time.time() - start_time
            logger.error(f"[{self.client_id}] API call {operation} FAILED after {duration:.3f}s: {e}")
            raise ConnectionError(f"API error: {str(e)}")

        except Exception as e:
            # Don't penalize circuit breaker for connection issues
            logger.warning(f"[{self.client_id}] Connection/SSL error (not counting as API failure): {e}")

            duration = time.time() - start_time
            logger.error(f"[{self.client_id}] API call {operation} FAILED after {duration:.3f}s: {e}")

            # Use standard exception for Celery compatibility
            raise ConnectionError(f"Connection error: {str(e)}")

    def get_rate_limit_status(self) -> Dict[str, Any]:
        """Get distributed rate limiter status"""
        return self.rate_limiter.get_status()


class HighPerformanceOHLCVFetcher:
    """
    High-performance OHLCV fetcher with distributed rate limiting
    All workers coordinate through Redis for optimal API usage
    """

    def __init__(self, max_workers: int = None, bulk_insert_size: int = None):
        self.max_workers = max_workers or settings.OHLCV_HISTORICAL_WORKERS
        self.bulk_insert_size = bulk_insert_size or settings.OHLCV_BULK_INSERT_SIZE

        # Thread-safe counters
        self._lock = threading.Lock()
        self._stats = {'total_processed': 0, 'successful': 0, 'failed': 0, 'total_records': 0, 'api_calls': 0, 'start_time': None}

        self.parser = OHLCVDataParser()

        logger.info(f"Initialized distributed OHLCV fetcher (workers={self.max_workers}, distributed rate limiting)")

    def fetch_historical_data_parallel(self, securities: List[Security], from_date: str = "2000-01-01", to_date: Optional[str] = None, progress_callback=None) -> Dict[str, Any]:
        """
        Fetch historical data with distributed coordination
        All workers share the same Redis-coordinated rate limit
        """
        start_time = time.time()
        self._stats['start_time'] = start_time

        if not securities:
            return {'status': 'SUCCESS', 'message': 'No securities to process'}

        logger.info(f"Starting distributed parallel fetch for {len(securities)} securities")

        # Filter valid securities
        valid_securities = [s for s in securities if s.security_type in ['STOCK', 'INDEX'] and s.is_active]

        # Create smaller batches for better distribution
        security_batches = self._create_distributed_batches(valid_securities)

        # Process all batches with distributed coordination
        all_results = []
        batch_records = []

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all batch jobs
                future_to_batch = {executor.submit(self._process_security_batch_distributed, batch, from_date, to_date): i for i, batch in enumerate(security_batches)}

                # Process results as they complete
                completed_batches = 0
                for future in as_completed(future_to_batch):
                    try:
                        batch_result = future.result()
                        all_results.extend(batch_result['results'])
                        batch_records.extend(batch_result['records'])

                        completed_batches += 1

                        # Update progress
                        if progress_callback:
                            progress = int((completed_batches / len(security_batches)) * 80)
                            progress_callback(progress)

                        if completed_batches % 5 == 0:
                            logger.info(f"Completed {completed_batches}/{len(security_batches)} batches with distributed coordination")

                    except Exception as e:
                        logger.error(f"Batch processing failed: {e}")
                        continue

            # Bulk insert all records
            if progress_callback:
                progress_callback(85)

            inserted_count = self._bulk_insert_all_records(batch_records, progress_callback)

            if progress_callback:
                progress_callback(95)

            self._bulk_update_progress(all_results)

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
                'total_records_inserted': inserted_count,
                'duration_seconds': round(duration, 2),
                'records_per_second': round(inserted_count / duration, 2) if duration > 0 else 0,
                'securities_per_minute': round((len(all_results) / duration) * 60, 2) if duration > 0 else 0,
                'api_calls_made': self._stats['api_calls'],
                'method': 'distributed_coordinated_parallel'
            }

            logger.info(f"Distributed parallel fetch completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Distributed parallel fetch failed: {e}")
            raise

    def _create_distributed_batches(self, securities: List[Security]) -> List[List[Security]]:
        """Create optimal batches for distributed processing"""
        # Smaller batches for better load distribution across workers
        batch_size = 3  # Small batches to maximize parallel efficiency

        batches = []
        for i in range(0, len(securities), batch_size):
            batches.append(securities[i:i + batch_size])

        logger.info(f"Created {len(batches)} distributed batches (size=3) for optimal coordination")
        return batches

    def _process_security_batch_distributed(self, securities: List[Security], from_date: str, to_date: Optional[str]) -> Dict[str, Any]:
        """Process a batch with distributed API client"""
        # Each worker gets its own distributed client
        api_client = DistributedRateLimitedDhanClient()

        batch_results = []
        batch_records = []

        for security in securities:
            try:
                result = self._fetch_single_security_distributed(api_client, security, from_date, to_date)
                batch_results.append(result)

                if result['status'] == 'success' and result.get('records'):
                    batch_records.extend(result['records'])

                # Update thread-safe stats
                with self._lock:
                    self._stats['total_processed'] += 1
                    self._stats['api_calls'] += 1
                    if result['status'] == 'success':
                        self._stats['successful'] += 1
                    else:
                        self._stats['failed'] += 1

            except Exception as e:
                logger.error(f"Error processing {security.symbol}: {e}")
                batch_results.append({'security_id': security.id, 'symbol': security.symbol, 'status': 'failed', 'error': str(e), 'records': []})

                with self._lock:
                    self._stats['total_processed'] += 1
                    self._stats['failed'] += 1

        return {'results': batch_results, 'records': batch_records}

    def _fetch_single_security_distributed(self, client: DistributedRateLimitedDhanClient, security: Security, from_date: str, to_date: Optional[str]) -> Dict[str, Any]:
        """Fetch single security with distributed coordination"""
        try:
            # Get exchange segment and instrument type
            exchange_segment = self.parser.get_exchange_segment(security.security_type)
            instrument = self.parser.get_instrument_type(security.security_type)

            # The distributed client handles rate limiting automatically
            response_data = client.fetch_historical_data(str(security.external_id), exchange_segment, instrument, from_date, to_date)

            # Parse response data
            records = self.parser.parse_historical_response(security.id, response_data, source="dhan_api_distributed")

            return {'security_id': security.id, 'symbol': security.symbol, 'status': 'success', 'records_count': len(records), 'records': records}

        except TimeoutError as e:
            logger.warning(f"Distributed rate limit timeout for {security.symbol}: {e}")
            return {'security_id': security.id, 'symbol': security.symbol, 'status': 'rate_limited', 'error': str(e), 'records': []}
        except ConnectionError as e:
            logger.warning(f"Connection error for {security.symbol}: {e}")
            return {'security_id': security.id, 'symbol': security.symbol, 'status': 'connection_error', 'error': str(e), 'records': []}
        except Exception as e:
            logger.error(f"Unexpected error for {security.symbol}: {e}")
            return {'security_id': security.id, 'symbol': security.symbol, 'status': 'failed', 'error': str(e), 'records': []}

    def fetch_today_eod_data_optimized(self, securities: List[Security]) -> Dict[str, Any]:
        """Optimized today's EOD data fetching with distributed coordination"""
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

            # Use distributed API client
            client = DistributedRateLimitedDhanClient()
            response_data = client.fetch_today_eod_data(dict(securities_by_segment))

            # Parse and insert
            records = self.parser.parse_today_eod_response(response_data, security_mapping, source="dhan_api_distributed")

            if records:
                inserted_count = self._bulk_insert_all_records(records)
                return {'status': 'SUCCESS', 'total_securities': len(valid_securities), 'processed': len(records), 'inserted': inserted_count}
            else:
                return {'status': 'SUCCESS', 'total_securities': len(valid_securities), 'processed': 0, 'inserted': 0, 'message': 'No valid EOD data received'}

        except Exception as e:
            logger.error(f"Distributed EOD fetch failed: {e}")
            return {'status': 'FAILED', 'error': str(e), 'total_securities': len(securities), 'processed': 0, 'inserted': 0}

    def test_api_connection(self) -> bool:
        """Test API connection using distributed client"""
        try:
            client = DistributedRateLimitedDhanClient()
            return client.test_connection()
        except:
            return False

    def get_distributed_rate_limit_status(self) -> Dict[str, Any]:
        """Get distributed rate limiter status with debugging info"""
        try:
            client = DistributedRateLimitedDhanClient()
            status = client.get_rate_limit_status()

            # Add debugging info
            status['debug_info'] = {'redis_connected': True, 'rate_limiter_active': True, 'current_timestamp': time.time()}

            return status
        except Exception as e:
            logger.error(f"Failed to get distributed rate limit status: {e}")
            return {'error': str(e), 'debug_info': {'redis_connected': False, 'rate_limiter_active': False, 'fallback_mode': True}}

    def reset_circuit_breaker(self) -> Dict[str, Any]:
        """Reset the distributed circuit breaker (admin function)"""
        try:
            client = DistributedRateLimitedDhanClient()
            client.rate_limiter.reset_circuit_breaker()
            return {'status': 'SUCCESS', 'message': 'Circuit breaker reset successfully'}
        except Exception as e:
            logger.error(f"Failed to reset circuit breaker: {e}")
            return {'status': 'FAILED', 'error': str(e)}

    def test_distributed_rate_limiting(self) -> Dict[str, Any]:
        """Test the distributed rate limiting system"""
        logger.info("Testing distributed rate limiting system...")

        try:
            client = DistributedRateLimitedDhanClient(client_id="rate_limit_test")

            # Test multiple rapid acquisitions
            test_results = []
            for i in range(5):
                start_time = time.time()
                success = client.rate_limiter.acquire(tokens=1, timeout=5.0, client_id=f"test_{i}")
                duration = time.time() - start_time

                test_results.append({'attempt': i + 1, 'success': success, 'duration_seconds': round(duration, 3), 'timestamp': time.time()})

                if success:
                    logger.info(f"Rate limit test {i+1}: SUCCESS in {duration:.3f}s")
                else:
                    logger.warning(f"Rate limit test {i+1}: FAILED in {duration:.3f}s")

            status = client.get_rate_limit_status()

            return {'test_completed': True, 'test_results': test_results, 'rate_limiter_status': status, 'summary': {'successful_acquisitions': sum(1 for r in test_results if r['success']), 'failed_acquisitions': sum(1 for r in test_results if not r['success']), 'avg_duration': sum(r['duration_seconds'] for r in test_results) / len(test_results)}}

        except Exception as e:
            logger.error(f"Rate limiting test failed: {e}")
            return {'test_completed': False, 'error': str(e), 'redis_connection_failed': True}

    # Keep existing methods for bulk insertion and progress tracking
    def _bulk_insert_all_records(self, all_records: List[Dict[str, Any]], progress_callback=None) -> int:
        """Ultra-fast bulk insert for maximum database throughput"""
        if not all_records:
            return 0

        logger.info(f"Bulk inserting {len(all_records)} records")

        chunk_size = self.bulk_insert_size * 2
        total_inserted = 0
        total_chunks = len(all_records) // chunk_size + (1 if len(all_records) % chunk_size else 0)

        with ThreadPoolExecutor(max_workers=min(6, total_chunks)) as executor:
            chunk_futures = []

            for i in range(0, len(all_records), chunk_size):
                chunk = all_records[i:i + chunk_size]
                future = executor.submit(self._insert_chunk_optimized, chunk, i // chunk_size + 1, total_chunks)
                chunk_futures.append(future)

            for future in as_completed(chunk_futures):
                try:
                    inserted = future.result()
                    total_inserted += inserted

                    if progress_callback:
                        progress = 85 + int((total_inserted / len(all_records)) * 10)
                        progress_callback(min(progress, 95))

                except Exception as e:
                    logger.error(f"Chunk insertion failed: {e}")

        logger.info(f"Bulk insert completed: {total_inserted} records")
        return total_inserted

    def _insert_chunk_optimized(self, chunk: List[Dict[str, Any]], chunk_num: int, total_chunks: int) -> int:
        """Insert a single chunk with optimized PostgreSQL settings"""
        try:
            with get_db() as db:
                # Only set settings that don't require superuser privileges
                try:
                    db.execute(text("SET synchronous_commit = OFF"))
                except Exception as e:
                    logger.debug(f"Could not set synchronous_commit: {e}")

                stmt = insert(OHLCVDaily).values(chunk)
                stmt = stmt.on_conflict_do_update(index_elements=['time', 'security_id'], set_={'open': stmt.excluded.open, 'high': stmt.excluded.high, 'low': stmt.excluded.low, 'close': stmt.excluded.close, 'volume': stmt.excluded.volume, 'adjusted_close': stmt.excluded.adjusted_close, 'source': stmt.excluded.source})

                db.execute(stmt)
                db.commit()

                if chunk_num % 5 == 0:
                    logger.info(f"Inserted chunk {chunk_num}/{total_chunks}")

                return len(chunk)

        except Exception as e:
            logger.error(f"Error inserting chunk {chunk_num}: {e}")
            return 0

    def _bulk_update_progress(self, results: List[Dict[str, Any]]):
        """Bulk update progress tracking"""
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

            return {'total_securities': total_securities, 'historical_completed': historical_completed, 'historical_pending': total_securities - historical_completed, 'daily_completed_today': daily_completed, 'failed_securities': failed_count, 'historical_progress_percent': round((historical_completed / total_securities) * 100, 2) if total_securities > 0 else 0, 'distributed_rate_limiting': True}

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get real-time performance statistics"""
        with self._lock:
            current_time = time.time()
            duration = current_time - (self._stats['start_time'] or current_time)

            return {
                'total_processed': self._stats['total_processed'],
                'successful': self._stats['successful'],
                'failed': self._stats['failed'],
                'success_rate': (self._stats['successful'] / max(self._stats['total_processed'], 1)) * 100,
                'duration_seconds': round(duration, 2),
                'securities_per_second': round(self._stats['total_processed'] / max(duration, 1), 2),
                'api_calls_made': self._stats['api_calls'],
                'avg_api_calls_per_second': round(self._stats['api_calls'] / max(duration, 1), 2),
                'distributed_rate_limiting': True
            }


# Factory function updated for distributed fetcher
def create_ohlcv_fetcher() -> HighPerformanceOHLCVFetcher:
    """Factory function to create distributed OHLCV fetcher"""
    return HighPerformanceOHLCVFetcher()
