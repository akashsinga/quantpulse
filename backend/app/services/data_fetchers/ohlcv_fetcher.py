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
import queue

from app.db.session import get_db
from app.db.models.security import Security
from app.db.models.ohlcv_daily import OHLCVDaily
from app.db.models.ohlcv_progress import OHLCVProgress
from .dhan_api_client import DhanAPIClient, DhanAPIError, RateLimitError
from .data_parser import OHLCVDataParser
from .rate_limiter import RateLimitedAPIClient
from app.utils.logger import get_logger
from app.config import settings

logger = get_logger(__name__)


class HighPerformanceOHLCVFetcher:
    """
    Highly optimized OHLCV data fetcher for massive parallel processing
    Designed to handle 25 years of data for 3000+ securities in under 20 minutes
    """

    def __init__(self, max_workers: int = None, bulk_insert_size: int = None):
        self.max_workers = max_workers or min(settings.OHLCV_HISTORICAL_WORKERS, 4)  # VERY conservative: 4 workers
        self.bulk_insert_size = bulk_insert_size or settings.OHLCV_BULK_INSERT_SIZE

        # Minimal API client pool to avoid overwhelming API
        self.api_clients = [DhanAPIClient() for _ in range(min(self.max_workers, 2))]  # Only 2 API clients
        self.client_pool = queue.Queue()
        for client in self.api_clients:
            self.client_pool.put(client)

        # VERY conservative rate limiter - prioritize success over speed
        self.rate_limiter = RateLimitedAPIClient(
            requests_per_second=10.0,  # VERY conservative: 10 req/sec (50% of limit)
            burst_capacity=15,  # Small burst
            circuit_breaker_threshold=3,  # Very strict: 3 failures max
            circuit_breaker_timeout=90  # Long recovery time
        )

        self.parser = OHLCVDataParser()

        # Thread-safe counters
        self._lock = threading.Lock()
        self._stats = {'total_processed': 0, 'successful': 0, 'failed': 0, 'total_records': 0, 'api_calls': 0, 'start_time': None}

        logger.info(f"Initialized CONSERVATIVE OHLCV Fetcher (workers={self.max_workers}, rate=10req/sec)")

    def _get_api_client(self) -> DhanAPIClient:
        """Get an API client from the pool"""
        return self.client_pool.get()

    def _return_api_client(self, client: DhanAPIClient):
        """Return API client to the pool"""
        self.client_pool.put(client)

    def fetch_historical_data_parallel(self, securities: List[Security], from_date: str = "2000-01-01", to_date: Optional[str] = None, progress_callback=None) -> Dict[str, Any]:
        """
        Fetch historical data for multiple securities using massive parallelization
        
        Args:
            securities: List of Security objects
            from_date: Start date
            to_date: End date
            progress_callback: Progress callback function
            
        Returns:
            Dict with comprehensive results
        """
        start_time = time.time()
        self._stats['start_time'] = start_time

        if not securities:
            return {'status': 'SUCCESS', 'message': 'No securities to process'}

        logger.info(f"Starting parallel historical fetch for {len(securities)} securities")

        # Filter valid securities
        valid_securities = [s for s in securities if s.security_type in ['STOCK', 'INDEX'] and s.is_active]

        # Group securities for batch processing
        security_batches = self._create_optimized_batches(valid_securities)

        # Process all batches in parallel
        all_results = []
        batch_records = []

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all batch jobs
                future_to_batch = {executor.submit(self._process_security_batch, batch, from_date, to_date): i for i, batch in enumerate(security_batches)}

                # Process results as they complete
                completed_batches = 0
                for future in as_completed(future_to_batch):
                    batch_idx = future_to_batch[future]

                    try:
                        batch_result = future.result()
                        all_results.extend(batch_result['results'])
                        batch_records.extend(batch_result['records'])

                        completed_batches += 1

                        # Update progress
                        if progress_callback:
                            progress = int((completed_batches / len(security_batches)) * 80)  # 80% for fetching
                            progress_callback(progress)

                        # Log progress
                        if completed_batches % 5 == 0:
                            logger.info(f"Completed {completed_batches}/{len(security_batches)} batches")

                    except Exception as e:
                        logger.error(f"Batch {batch_idx} failed: {e}")
                        continue

            # Bulk insert all records
            if progress_callback:
                progress_callback(85)

            inserted_count = self._bulk_insert_all_records(batch_records, progress_callback)

            # Update progress tracking in bulk
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
                'avg_api_response_time': f"{duration / max(self._stats['api_calls'], 1):.3f}s",
                'method': 'high_performance_parallel'
            }

            logger.info(f"Parallel historical fetch completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Parallel historical fetch failed: {e}")
            raise

    def _create_optimized_batches(self, securities: List[Security]) -> List[List[Security]]:
        """
        Create CONSERVATIVE batches - small and safe
        """
        # VERY small batches to be absolutely safe with rate limits
        batch_size = 2  # Only 2 securities per batch - ultra conservative

        # Separate stocks and indices
        stocks = [s for s in securities if s.security_type == 'STOCK']
        indices = [s for s in securities if s.security_type == 'INDEX']

        batches = []

        # Process stocks in tiny batches
        for i in range(0, len(stocks), batch_size):
            batches.append(stocks[i:i + batch_size])

        # Process indices in tiny batches
        for i in range(0, len(indices), batch_size):
            batches.append(indices[i:i + batch_size])

        logger.info(f"Created {len(batches)} CONSERVATIVE batches (size=2) for maximum success rate")
        return batches

    def _process_security_batch(self, securities: List[Security], from_date: str, to_date: Optional[str]) -> Dict[str, Any]:
        """
        Process a batch of securities with shared API client and optimized error handling
        """
        client = self._get_api_client()
        batch_results = []
        batch_records = []

        try:
            for security in securities:
                try:
                    result = self._fetch_single_security_optimized(client, security, from_date, to_date)
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

        finally:
            self._return_api_client(client)

        return {'results': batch_results, 'records': batch_records}

    def _fetch_single_security_optimized(self, client: DhanAPIClient, security: Security, from_date: str, to_date: Optional[str]) -> Dict[str, Any]:
        """
        CONSERVATIVE fetch with maximum delays and retries
        """
        try:
            # Get exchange segment and instrument type
            exchange_segment = self.parser.get_exchange_segment(security.security_type)
            instrument = self.parser.get_instrument_type(security.security_type)

            # LONG delay between requests - prioritize success over speed
            time.sleep(0.2)  # 200ms delay (very conservative)

            # Use rate limiter with very long timeout for stability
            response_data = self.rate_limiter.execute_request(
                client.fetch_historical_data,
                str(security.external_id),
                exchange_segment,
                instrument,
                from_date,
                to_date,
                timeout=60.0  # Very long timeout - let it retry
            )

            # Parse response data
            records = self.parser.parse_historical_response(security.id, response_data, source="dhan_api_conservative")

            return {'security_id': security.id, 'symbol': security.symbol, 'status': 'success', 'records_count': len(records), 'records': records}

        except RateLimitError as e:
            # LONG wait on rate limit - be very patient
            logger.warning(f"Rate limit hit for {security.symbol}, waiting 10 seconds...")
            time.sleep(10.0)  # 10 second pause on rate limit
            return {'security_id': security.id, 'symbol': security.symbol, 'status': 'rate_limited', 'error': str(e), 'records': []}
        except DhanAPIError as e:
            logger.warning(f"API error for {security.symbol}: {e}")
            return {'security_id': security.id, 'symbol': security.symbol, 'status': 'api_error', 'error': str(e), 'records': []}
        except Exception as e:
            logger.error(f"Unexpected error for {security.symbol}: {e}")
            return {'security_id': security.id, 'symbol': security.symbol, 'status': 'failed', 'error': str(e), 'records': []}

    def _bulk_insert_all_records(self, all_records: List[Dict[str, Any]], progress_callback=None) -> int:
        """
        ULTRA-FAST bulk insert for 20-30 minute target
        """
        if not all_records:
            return 0

        logger.info(f"ULTRA-FAST bulk inserting {len(all_records)} records")

        # LARGER chunks for maximum database throughput
        chunk_size = self.bulk_insert_size * 2  # Double the chunk size
        total_inserted = 0
        total_chunks = len(all_records) // chunk_size + (1 if len(all_records) % chunk_size else 0)

        # Process chunks with MORE parallel writers
        with ThreadPoolExecutor(max_workers=min(6, total_chunks)) as executor:  # 6 parallel DB writers
            chunk_futures = []

            for i in range(0, len(all_records), chunk_size):
                chunk = all_records[i:i + chunk_size]
                future = executor.submit(self._insert_chunk_optimized, chunk, i // chunk_size + 1, total_chunks)
                chunk_futures.append(future)

            # Collect results
            for future in as_completed(chunk_futures):
                try:
                    inserted = future.result()
                    total_inserted += inserted

                    # Update progress
                    if progress_callback:
                        progress = 85 + int((total_inserted / len(all_records)) * 10)  # 85-95%
                        progress_callback(min(progress, 95))

                except Exception as e:
                    logger.error(f"Chunk insertion failed: {e}")

        logger.info(f"ULTRA-FAST bulk insert completed: {total_inserted} records")
        return total_inserted

    def _insert_chunk_optimized(self, chunk: List[Dict[str, Any]], chunk_num: int, total_chunks: int) -> int:
        """
        Insert a single chunk with MAXIMUM PostgreSQL performance
        """
        try:
            with get_db() as db:
                # OPTIMIZED PostgreSQL settings for this session
                db.execute(text("SET synchronous_commit = OFF"))  # Faster commits
                db.execute(text("SET checkpoint_completion_target = 0.9"))  # Better checkpointing

                # Use PostgreSQL's ON CONFLICT for high-performance upsert
                stmt = insert(OHLCVDaily).values(chunk)
                stmt = stmt.on_conflict_do_update(index_elements=['time', 'security_id'], set_={'open': stmt.excluded.open, 'high': stmt.excluded.high, 'low': stmt.excluded.low, 'close': stmt.excluded.close, 'volume': stmt.excluded.volume, 'adjusted_close': stmt.excluded.adjusted_close, 'source': stmt.excluded.source})

                db.execute(stmt)
                db.commit()

                if chunk_num % 5 == 0:  # Log every 5 chunks
                    logger.info(f"ULTRA-FAST inserted chunk {chunk_num}/{total_chunks}")

                return len(chunk)

        except Exception as e:
            logger.error(f"Error inserting chunk {chunk_num}: {e}")
            return 0

    def _bulk_update_progress(self, results: List[Dict[str, Any]]):
        """
        Bulk update progress tracking for better performance
        """
        try:
            with get_db() as db:
                # Group results by status for batch processing
                success_ids = [r['security_id'] for r in results if r['status'] == 'success']
                failed_results = [(r['security_id'], r.get('error', 'Unknown error')) for r in results if r['status'] != 'success']

                today = date.today()

                # Batch update successful securities
                if success_ids:
                    # Use raw SQL for better performance
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

                # Batch update failed securities
                if failed_results:
                    # Insert/update failed records
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

                        db.execute(
                            update_failed_query,
                            {
                                'security_id': security_id,
                                'error_msg': error_msg[:500]  # Truncate long error messages
                            })

                db.commit()
                logger.info(f"Updated progress for {len(success_ids)} successful and {len(failed_results)} failed securities")

        except Exception as e:
            logger.error(f"Error updating progress tracking: {e}")

    def fetch_today_eod_data_optimized(self, securities: List[Security]) -> Dict[str, Any]:
        """
        Optimized today's EOD data fetching with better batching
        """
        if not securities:
            return {'status': 'SUCCESS', 'message': 'No securities to process'}

        try:
            # Filter and group securities more efficiently
            valid_securities = [s for s in securities if s.security_type in ['STOCK', 'INDEX'] and s.is_active]

            if not valid_securities:
                return {'status': 'SUCCESS', 'message': 'No valid securities to process'}

            # Group by segment with larger batches
            securities_by_segment = defaultdict(list)
            for security in valid_securities:
                segment = "NSE_EQ" if security.security_type == "STOCK" else "IDX_I"
                securities_by_segment[segment].append(str(security.external_id))

            # Create security mapping
            security_mapping = {str(sec.external_id): sec.id for sec in valid_securities}

            # Fetch with rate limiting
            client = self._get_api_client()
            try:
                response_data = self.rate_limiter.execute_request(client.fetch_today_eod_data, dict(securities_by_segment), timeout=20.0)
            finally:
                self._return_api_client(client)

            # Parse and insert
            records = self.parser.parse_today_eod_response(response_data, security_mapping, source="dhan_api_optimized")

            if records:
                inserted_count = self._bulk_insert_all_records(records)
                return {'status': 'SUCCESS', 'total_securities': len(valid_securities), 'processed': len(records), 'inserted': inserted_count}
            else:
                return {'status': 'SUCCESS', 'total_securities': len(valid_securities), 'processed': 0, 'inserted': 0, 'message': 'No valid EOD data received'}

        except Exception as e:
            logger.error(f"Optimized EOD fetch failed: {e}")
            return {'status': 'FAILED', 'error': str(e), 'total_securities': len(securities), 'processed': 0, 'inserted': 0}

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get real-time performance statistics"""
        with self._lock:
            current_time = time.time()
            duration = current_time - (self._stats['start_time'] or current_time)

            return {'total_processed': self._stats['total_processed'], 'successful': self._stats['successful'], 'failed': self._stats['failed'], 'success_rate': (self._stats['successful'] / max(self._stats['total_processed'], 1)) * 100, 'duration_seconds': round(duration, 2), 'securities_per_second': round(self._stats['total_processed'] / max(duration, 1), 2), 'api_calls_made': self._stats['api_calls'], 'avg_api_calls_per_second': round(self._stats['api_calls'] / max(duration, 1), 2)}

    def get_pending_securities(self, operation_type: str = 'historical') -> List[Security]:
        """Get securities that need data fetching based on progress tracking"""
        with get_db() as db:
            if operation_type == 'historical':
                securities = db.query(Security).outerjoin(OHLCVProgress, Security.id == OHLCVProgress.security_id).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']), (OHLCVProgress.last_historical_fetch.is_(None) | (OHLCVProgress.status == 'failed')))).all()
            else:  # daily
                today = date.today()
                securities = db.query(Security).outerjoin(OHLCVProgress, Security.id == OHLCVProgress.security_id).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']), (OHLCVProgress.last_daily_fetch.is_(None) | (OHLCVProgress.last_daily_fetch < today) | (OHLCVProgress.status == 'failed')))).all()

            logger.info(f"Found {len(securities)} securities pending {operation_type} processing")
            return securities

    def test_api_connection(self) -> bool:
        """Test API connection using one of the pooled clients"""
        try:
            client = self._get_api_client()
            try:
                return client.test_connection()
            finally:
                self._return_api_client(client)
        except:
            return False


# Factory function updated to use high-performance fetcher
def create_ohlcv_fetcher() -> HighPerformanceOHLCVFetcher:
    """Factory function to create high-performance OHLCV fetcher instance"""
    return HighPerformanceOHLCVFetcher()


# Legacy compatibility wrapper
class OHLCVFetcher(HighPerformanceOHLCVFetcher):
    """Legacy wrapper for backward compatibility"""

    def fetch_historical_data_for_security(self, security: Security, from_date: str = "2000-01-01", to_date: Optional[str] = None) -> Dict[str, Any]:
        """Legacy single security method - redirects to optimized batch processing"""
        result = self.fetch_historical_data_parallel([security], from_date, to_date)

        if result['processed'] > 0:
            return {'security_id': security.id, 'symbol': security.symbol, 'status': 'success' if result['successful'] > 0 else 'failed', 'records_inserted': result.get('total_records_inserted', 0), 'error': None if result['successful'] > 0 else 'Processing failed'}
        else:
            return {'security_id': security.id, 'symbol': security.symbol, 'status': 'failed', 'records_inserted': 0, 'error': 'No data processed'}

    def fetch_today_eod_data(self, securities: List[Security]) -> Dict[str, Any]:
        """Legacy EOD method - redirects to optimized version"""
        return self.fetch_today_eod_data_optimized(securities)

    def get_active_securities(self, security_types: List[str] = None) -> List[Security]:
        """Get active securities for OHLCV fetching"""
        if security_types is None:
            security_types = ['STOCK', 'INDEX']

        with get_db() as db:
            securities = db.query(Security).filter(and_(Security.is_active == True, Security.security_type.in_(security_types))).all()

            logger.info(f"Found {len(securities)} active securities")
            return securities

    def get_pending_securities(self, operation_type: str = 'historical') -> List[Security]:
        """Get securities that need data fetching based on progress tracking"""
        with get_db() as db:
            if operation_type == 'historical':
                securities = db.query(Security).outerjoin(OHLCVProgress, Security.id == OHLCVProgress.security_id).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']), (OHLCVProgress.last_historical_fetch.is_(None) | (OHLCVProgress.status == 'failed')))).all()
            else:  # daily
                today = date.today()
                securities = db.query(Security).outerjoin(OHLCVProgress, Security.id == OHLCVProgress.security_id).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']), (OHLCVProgress.last_daily_fetch.is_(None) | (OHLCVProgress.last_daily_fetch < today) | (OHLCVProgress.status == 'failed')))).all()

            logger.info(f"Found {len(securities)} securities pending {operation_type} processing")
            return securities

    def update_progress(self, security_id: uuid.UUID, operation_type: str, status: str, error_message: str = None):
        """Update progress tracking for a security"""
        try:
            with get_db() as db:
                progress = db.query(OHLCVProgress).filter(OHLCVProgress.security_id == security_id).first()

                if not progress:
                    progress = OHLCVProgress(security_id=security_id)
                    db.add(progress)

                if operation_type == 'historical' and status == 'success':
                    progress.last_historical_fetch = date.today()
                elif operation_type == 'daily' and status == 'success':
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
            logger.error(f"Error updating progress for {security_id}: {e}")

    def get_progress_summary(self) -> Dict[str, Any]:
        """Get overall progress summary"""
        with get_db() as db:
            total_securities = db.query(Security).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']))).count()

            historical_completed = db.query(OHLCVProgress).filter(and_(OHLCVProgress.last_historical_fetch.isnot(None), OHLCVProgress.status == 'success')).count()

            today = date.today()
            daily_completed = db.query(OHLCVProgress).filter(and_(OHLCVProgress.last_daily_fetch == today, OHLCVProgress.status == 'success')).count()

            failed_count = db.query(OHLCVProgress).filter(OHLCVProgress.status == 'failed').count()

            return {'total_securities': total_securities, 'historical_completed': historical_completed, 'historical_pending': total_securities - historical_completed, 'daily_completed_today': daily_completed, 'failed_securities': failed_count, 'historical_progress_percent': round((historical_completed / total_securities) * 100, 2) if total_securities > 0 else 0}
