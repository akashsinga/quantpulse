# app/services/data_fetchers/ohlcv_fetcher.py

import uuid
import time
import asyncio
import concurrent.futures
from datetime import datetime, date, timedelta
from typing import List, Dict, Tuple, Optional, Any, Union, Set
from sqlalchemy import func
from sqlalchemy.orm import joinedload, Session
from dataclasses import dataclass, asdict
import gc
import os
import threading
from contextlib import contextmanager

from app.db.session import get_db, SessionLocal
from app.db.models.security import Security
from app.db.models.exchange import Exchange
from app.config import settings
from utils.logger import get_logger
from .dhan_api_client import DhanAPIClient, CircuitBreakerOpenError, RateLimitException
from .exchange_mapper import ExchangeMapper
from ..repositories.ohlcv_repository import OHLCVRepository

logger = get_logger(__name__)


@dataclass
class SecurityData:
    """Lightweight security data to avoid session binding issues"""

    id: str
    external_id: str
    symbol: str
    security_type: str
    exchange_code: str

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


@dataclass
class BatchConfig:
    """Enhanced configuration for batch processing"""

    workers: int
    batch_size: int
    max_concurrent_requests: int
    request_delay: float
    max_retries: int
    bulk_insert_size: int
    memory_check_interval: int
    cache_clear_threshold: int

    @classmethod
    def from_settings(cls, workers: int = None, batch_size: int = None):
        """Create config from settings with parameter overrides"""
        return cls(workers=workers or settings.OHLCV_WORKERS, batch_size=batch_size or settings.OHLCV_BATCH_SIZE, max_concurrent_requests=settings.OHLCV_MAX_CONCURRENT, request_delay=settings.OHLCV_REQUEST_DELAY, max_retries=settings.OHLCV_MAX_RETRIES, bulk_insert_size=settings.OHLCV_BULK_INSERT_SIZE, memory_check_interval=settings.OHLCV_MEMORY_CHECK_INTERVAL, cache_clear_threshold=settings.OHLCV_CACHE_SIZE)


@dataclass
class OperationResult:
    """Result of an OHLCV operation"""

    operation_id: str
    status: str
    duration_seconds: float
    securities_processed: int
    securities_success: int
    securities_error: int
    total_records: int
    error_details: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        if result["error_details"] is None:
            del result["error_details"]
        return result


class PerformanceMonitor:
    """Performance monitoring for OHLCV operations"""

    def __init__(self):
        self.stats = {"operations_total": 0, "operations_success": 0, "operations_error": 0, "securities_processed": 0, "records_processed": 0, "avg_operation_time_seconds": 0.0, "cache_hits": 0, "cache_misses": 0, "memory_cleanups": 0}
        self._lock = threading.Lock()
        self.start_time = time.time()

    def record_operation(self, success: bool, securities: int, records: int, duration: float):
        """Record operation statistics"""
        with self._lock:
            self.stats["operations_total"] += 1
            self.stats["securities_processed"] += securities
            self.stats["records_processed"] += records

            if success:
                self.stats["operations_success"] += 1
            else:
                self.stats["operations_error"] += 1

            # Update average operation time
            total_ops = self.stats["operations_total"]
            current_avg = self.stats["avg_operation_time_seconds"]
            self.stats["avg_operation_time_seconds"] = (current_avg * (total_ops - 1) + duration) / total_ops

    def record_cache_hit(self):
        with self._lock:
            self.stats["cache_hits"] += 1

    def record_cache_miss(self):
        with self._lock:
            self.stats["cache_misses"] += 1

    def record_memory_cleanup(self):
        with self._lock:
            self.stats["memory_cleanups"] += 1

    def get_stats(self) -> Dict[str, Any]:
        """Get current performance statistics"""
        with self._lock:
            stats = self.stats.copy()

        # Calculate additional metrics
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            stats["operations_per_second"] = stats["operations_total"] / elapsed
            stats["records_per_second"] = stats["records_processed"] / elapsed
        else:
            stats["operations_per_second"] = 0
            stats["records_per_second"] = 0

        if stats["operations_total"] > 0:
            stats["success_rate_pct"] = (stats["operations_success"] / stats["operations_total"]) * 100
        else:
            stats["success_rate_pct"] = 0

        cache_total = stats["cache_hits"] + stats["cache_misses"]
        if cache_total > 0:
            stats["cache_hit_rate_pct"] = (stats["cache_hits"] / cache_total) * 100
        else:
            stats["cache_hit_rate_pct"] = 0

        # Round floating point values
        for key, value in stats.items():
            if isinstance(value, float):
                stats[key] = round(value, 2)

        stats["uptime_seconds"] = round(elapsed, 2)
        return stats


class OHLCVFetcher:
    """Enhanced main coordinator for OHLCV data fetching operations."""

    def __init__(self, api_client: Optional[DhanAPIClient] = None, mapper: Optional[ExchangeMapper] = None, repository: Optional[OHLCVRepository] = None):
        """Initialize the enhanced OHLCV fetcher."""
        self.api_client = api_client or DhanAPIClient()
        self.mapper = mapper or ExchangeMapper()
        self.repository = repository or OHLCVRepository()

        # Configuration
        self.config = BatchConfig.from_settings()

        # Performance monitoring
        self.performance_monitor = PerformanceMonitor()

        # Caching
        self._security_cache = {}
        self._id_mapping_cache = {}
        self._batch_counter = 0

        # Thread safety
        self._cache_lock = threading.Lock()

        logger.info(f"Initialized enhanced OHLCV fetcher")
        logger.info(f"Config: workers={self.config.workers}, batch_size={self.config.batch_size}")
        logger.info(f"Max concurrent: {self.config.max_concurrent_requests}, bulk_size={self.config.bulk_insert_size}")

    def fetch_historical_data(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, workers: int = None, batch_size: int = None, verbose: bool = False) -> Dict[str, Any]:
        """Enhanced historical data fetching with improved reliability."""
        operation_id = str(uuid.uuid4())
        start_time = time.time()

        # Update config if parameters provided
        if workers or batch_size:
            self.config = BatchConfig.from_settings(workers, batch_size)

        # Use defaults from settings
        start_date = start_date or settings.FROM_DATE
        end_date = end_date or (settings.TO_DATE or datetime.now().strftime("%Y-%m-%d"))

        logger.info(f"Starting enhanced historical data fetch operation {operation_id}")
        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info(f"Workers: {self.config.workers}, Batch size: {self.config.batch_size}")

        try:
            # Step 1: Select and prepare securities
            securities_data = self._select_and_prepare_securities(security_ids, exchanges, segments)

            if not securities_data:
                return self._create_error_result(operation_id, "No securities found", start_time)

            logger.info(f"Selected {len(securities_data)} securities for processing")

            # Step 2: Create optimized batches
            batches = self._create_optimized_batches(securities_data)
            logger.info(f"Created {len(batches)} batches for processing")

            # Step 3: Process batches with enhanced error handling
            batch_results = self._process_historical_batches_enhanced(batches, start_date, end_date, verbose)

            # Step 4: Compile final results
            result = self._compile_operation_result(operation_id, batch_results, len(securities_data), start_time)

            # Record performance metrics
            self.performance_monitor.record_operation(result["status"] == "completed", result["securities"]["total"], result["stats"]["total_records"], result["duration_seconds"])

            return result

        except Exception as e:
            logger.error(f"Critical error in historical data fetch: {str(e)}", exc_info=True)
            error_result = self._create_error_result(operation_id, str(e), start_time)
            self.performance_monitor.record_operation(False, 0, 0, error_result["duration_seconds"])
            return error_result
        finally:
            self._cleanup_resources()

    def fetch_current_day_data(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None, is_eod: bool = False, workers: int = None, batch_size: int = None, verbose: bool = False) -> Dict[str, Any]:
        """Enhanced current day data fetching."""
        operation_id = str(uuid.uuid4())
        start_time = time.time()

        # Update config if parameters provided
        if workers or batch_size:
            self.config = BatchConfig.from_settings(workers, batch_size)

        logger.info(f"Starting enhanced current day data fetch operation {operation_id}")
        logger.info(f"Mode: {'EOD' if is_eod else 'Regular'}")

        try:
            # Step 1: Select and prepare securities
            securities_data = self._select_and_prepare_securities(security_ids, exchanges, segments)

            if not securities_data:
                return self._create_error_result(operation_id, "No securities found", start_time)

            logger.info(f"Selected {len(securities_data)} securities for current day data")

            # Step 2: Group securities by segment for efficient API calls
            securities_by_segment = self._group_securities_by_segment(securities_data)

            # Step 3: Process current data with enhanced error handling
            process_result = self._process_current_data_enhanced(securities_by_segment, is_eod, verbose)

            # Step 4: Create final result
            duration_seconds = time.time() - start_time

            result = {
                "operation_id": operation_id,
                "status": "completed",
                "duration_seconds": round(duration_seconds, 2),
                "mode": "eod" if is_eod else "regular",
                "timestamp": datetime.now().isoformat(),
                "stats": {"securities_processed": sum(len(ids) for ids in securities_by_segment.values()), "securities_with_data": len(process_result.get("securities_with_data", [])), "securities_without_data": len(process_result.get("securities_without_data", [])), "securities_error": len(process_result.get("errors", {})), "total_records_stored": process_result.get("total_records_stored", 0)},
                "securities": {"total": len(securities_data), "with_data": len(process_result.get("securities_with_data", [])), "without_data": len(process_result.get("securities_without_data", [])), "error": len(process_result.get("errors", {}))},
            }

            # Record performance metrics
            self.performance_monitor.record_operation(True, result["securities"]["total"], result["stats"]["total_records_stored"], duration_seconds)

            return result

        except Exception as e:
            logger.error(f"Critical error in current day data fetch: {str(e)}", exc_info=True)
            error_result = self._create_error_result(operation_id, str(e), start_time)
            self.performance_monitor.record_operation(False, 0, 0, error_result["duration_seconds"])
            return error_result
        finally:
            self._cleanup_resources()

    def update_all_data(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None, days_back: int = 7, include_today: bool = True, workers: int = None, batch_size: int = None, verbose: bool = False, full_history: bool = False) -> Dict[str, Any]:
        """Enhanced comprehensive data update workflow."""
        operation_id = str(uuid.uuid4())
        start_time = time.time()

        logger.info(f"Starting enhanced comprehensive data update operation {operation_id}")
        logger.info(f"Parameters: days_back={days_back}, include_today={include_today}, full_history={full_history}")

        try:
            # Calculate date range
            if full_history:
                hist_start_date = datetime.strptime(settings.FROM_DATE, "%Y-%m-%d").date()
                hist_end_date = datetime.now().date() - timedelta(days=1)
                logger.info(f"Full history mode: {hist_start_date} to {hist_end_date}")
            else:
                hist_end_date = datetime.now().date() - timedelta(days=1)
                hist_start_date = hist_end_date - timedelta(days=days_back)
                logger.info(f"Recent data mode: {hist_start_date} to {hist_end_date}")

            historical_result = None
            current_result = None

            # Step 1: Fetch historical data
            if days_back > 0 or full_history:
                logger.info("Starting historical data fetch...")
                historical_result = self.fetch_historical_data(security_ids=security_ids, exchanges=exchanges, segments=segments, start_date=hist_start_date.strftime("%Y-%m-%d"), end_date=hist_end_date.strftime("%Y-%m-%d"), workers=workers, batch_size=batch_size, verbose=verbose)

            # Step 2: Fetch today's data if requested
            if include_today:
                logger.info("Starting current day data fetch...")
                # Use optimized settings for current data
                current_workers = min(self.config.workers, 6)  # Fewer workers for current data
                current_batch_size = self.config.batch_size * 4  # Larger batches

                current_result = self.fetch_current_day_data(security_ids=security_ids, exchanges=exchanges, segments=segments, is_eod=True, workers=current_workers, batch_size=current_batch_size, verbose=verbose)

            duration_seconds = time.time() - start_time

            result = {"operation_id": operation_id, "status": "completed", "duration_seconds": round(duration_seconds, 2), "historical": historical_result, "current": current_result, "days_back": days_back, "include_today": include_today, "full_history": full_history, "summary": self._create_update_summary(historical_result, current_result)}

            # Record comprehensive operation metrics
            total_securities = 0
            total_records = 0

            if historical_result:
                total_securities = max(total_securities, historical_result.get("securities", {}).get("total", 0))
                total_records += historical_result.get("stats", {}).get("total_records", 0)

            if current_result:
                total_securities = max(total_securities, current_result.get("securities", {}).get("total", 0))
                total_records += current_result.get("stats", {}).get("total_records_stored", 0)

            self.performance_monitor.record_operation(True, total_securities, total_records, duration_seconds)

            return result

        except Exception as e:
            logger.error(f"Critical error in comprehensive data update: {str(e)}", exc_info=True)
            error_result = self._create_error_result(operation_id, str(e), start_time)
            self.performance_monitor.record_operation(False, 0, 0, error_result["duration_seconds"])
            return error_result

    def _select_and_prepare_securities(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None) -> List[SecurityData]:
        """Enhanced security selection with better error handling."""
        logger.info("Selecting securities for processing (STOCKS and INDICES only)")

        try:
            with get_db() as db:
                # Build optimized query
                query = db.query(Security).options(joinedload(Security.exchange)).filter(Security.is_active == True, Security.security_type.in_(["STOCK", "INDEX"]))

                # Apply filters
                if security_ids:
                    # Convert string UUIDs to UUID objects if needed
                    uuid_list = []
                    for sid in security_ids:
                        try:
                            if isinstance(sid, str):
                                uuid_list.append(uuid.UUID(sid))
                            else:
                                uuid_list.append(sid)
                        except ValueError:
                            logger.warning(f"Invalid security ID format: {sid}")
                            continue

                    if uuid_list:
                        query = query.filter(Security.id.in_(uuid_list))
                    else:
                        logger.warning("No valid security IDs provided")
                        return []

                if exchanges:
                    query = query.join(Security.exchange).filter(Exchange.code.in_([code.upper() for code in exchanges]))

                if segments:
                    query = query.filter(Security.segment.in_(segments))

                # Execute query with ordering for consistent results
                query = query.order_by(Security.security_type, Security.symbol)
                securities = query.all()

                # Convert to lightweight data objects
                securities_data = []
                for security in securities:
                    try:
                        sec_data = SecurityData(id=str(security.id), external_id=str(security.external_id), symbol=security.symbol, security_type=security.security_type, exchange_code=security.exchange.code if security.exchange else "NSE")
                        securities_data.append(sec_data)

                        # Cache security data for later use
                        with self._cache_lock:
                            self._security_cache[sec_data.id] = sec_data

                    except Exception as e:
                        logger.warning(f"Error preparing security data for {security.symbol}: {e}")
                        continue

                # Clear session to prevent memory leaks
                db.expunge_all()

            stock_count = sum(1 for s in securities_data if s.security_type == "STOCK")
            index_count = sum(1 for s in securities_data if s.security_type == "INDEX")

            logger.info(f"Prepared {len(securities_data)} securities: {stock_count} stocks, {index_count} indices")
            return securities_data

        except Exception as e:
            logger.error(f"Error selecting securities: {str(e)}", exc_info=True)
            return []

    def _create_optimized_batches(self, securities_data: List[SecurityData]) -> List[List[SecurityData]]:
        """Create optimized batches with improved load balancing."""
        if not securities_data:
            return []

        # Separate by type for optimal processing
        stocks = [s for s in securities_data if s.security_type == "STOCK"]
        indices = [s for s in securities_data if s.security_type == "INDEX"]

        # Create balanced batches
        stock_batches = [stocks[i : i + self.config.batch_size] for i in range(0, len(stocks), self.config.batch_size)]

        index_batches = [indices[i : i + self.config.batch_size] for i in range(0, len(indices), self.config.batch_size)]

        # Interleave batches for better load distribution
        all_batches = []
        max_batches = max(len(stock_batches), len(index_batches))

        for i in range(max_batches):
            if i < len(index_batches):
                all_batches.append(index_batches[i])
            if i < len(stock_batches):
                all_batches.append(stock_batches[i])

        logger.info(f"Created {len(all_batches)} optimized batches")
        logger.info(f"Batch distribution: {len(index_batches)} index batches, {len(stock_batches)} stock batches")

        return all_batches

    def _process_historical_batches_enhanced(self, batches: List[List[SecurityData]], start_date: str, end_date: str, verbose: bool) -> List[Dict[str, Any]]:
        """Enhanced batch processing with better resource management."""
        results = []

        # Enhanced rate limiting with proper semaphore
        from threading import BoundedSemaphore

        api_semaphore = BoundedSemaphore(self.config.max_concurrent_requests)

        def process_batch_with_enhanced_error_handling(batch_data, batch_idx):
            """Process batch with comprehensive error handling."""
            try:
                with api_semaphore:
                    return self._process_single_historical_batch(batch_data, start_date, end_date, verbose, batch_idx)
            except CircuitBreakerOpenError as e:
                logger.error(f"Circuit breaker open - stopping batch {batch_idx}: {e}")
                raise  # Re-raise to stop processing
            except RateLimitException as e:
                logger.warning(f"Rate limit hit in batch {batch_idx}: {e}")
                # Return partial result instead of failing completely
                return {"status": "rate_limited", "batch_idx": batch_idx, "securities_processed": len(batch_data), "securities_success": 0, "securities_error": len(batch_data), "securities_skipped": 0, "total_records": 0, "error": str(e)}
            except Exception as e:
                logger.error(f"Batch {batch_idx} processing failed: {str(e)}")
                return {"status": "error", "batch_idx": batch_idx, "securities_processed": len(batch_data), "securities_success": 0, "securities_error": len(batch_data), "securities_skipped": 0, "total_records": 0, "error": str(e)}

        # Process batches with controlled concurrency
        max_workers = min(self.config.workers, 32)  # System stability cap

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all batch jobs
            future_to_batch = {executor.submit(process_batch_with_enhanced_error_handling, batch, idx): idx for idx, batch in enumerate(batches)}

            completed_count = 0
            circuit_breaker_triggered = False

            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                completed_count += 1

                try:
                    batch_result = future.result()
                    results.append(batch_result)

                    # Memory management check
                    self._batch_counter += 1
                    if self._batch_counter % self.config.memory_check_interval == 0:
                        self._periodic_memory_cleanup()

                    # Progress logging
                    if completed_count % max(1, len(batches) // 10) == 0 or completed_count == len(batches):
                        progress_pct = (completed_count / len(batches)) * 100
                        logger.info(f"Enhanced progress: {completed_count}/{len(batches)} batches ({progress_pct:.1f}%)")

                        # Log performance stats periodically
                        if completed_count % 50 == 0:
                            api_stats = self.api_client.get_performance_stats()
                            logger.info(f"API performance: {api_stats['success_rate_pct']:.1f}% success, " f"{api_stats['current_delay_ms']:.1f}ms delay")

                except CircuitBreakerOpenError:
                    circuit_breaker_triggered = True
                    logger.error("Circuit breaker triggered - stopping all batch processing")
                    break
                except Exception as e:
                    logger.error(f"Future execution error for batch {batch_idx}: {str(e)}")
                    results.append({"status": "execution_error", "batch_idx": batch_idx, "securities_processed": 0, "securities_success": 0, "securities_error": 1, "securities_skipped": 0, "total_records": 0, "error": str(e)})

            # Cancel remaining futures if circuit breaker was triggered
            if circuit_breaker_triggered:
                for future in future_to_batch:
                    if not future.done():
                        future.cancel()

        logger.info(f"Enhanced batch processing completed: {len(results)} results collected")
        return results

    def _process_single_historical_batch(self, batch: List[SecurityData], start_date: str, end_date: str, verbose: bool, batch_idx: int) -> Dict[str, Any]:
        """Process a single historical batch with optimized database operations."""
        batch_result = {"batch_idx": batch_idx, "status": "completed", "securities_processed": len(batch), "securities_success": 0, "securities_error": 0, "securities_skipped": 0, "total_records": 0, "security_results": {}, "errors": {}}

        if verbose:
            logger.debug(f"Processing enhanced batch {batch_idx} with {len(batch)} securities")

        # Collect all records for bulk insertion
        bulk_records_by_security = {}

        for security_data in batch:
            try:
                # Get API parameters
                params = self._get_api_parameters_enhanced(security_data, start_date, end_date)

                if verbose:
                    logger.debug(f"API params for {security_data.symbol}: {params}")

                # Make API call
                response = self.api_client.fetch_historical_data(**params)

                if response and response.get("status") == "success" and response.get("data"):
                    records = response.get("data", [])

                    # Enhanced validation
                    valid_records = self._validate_ohlcv_records_enhanced(records, security_data.symbol)

                    if valid_records:
                        bulk_records_by_security[security_data.id] = valid_records

                        batch_result["securities_success"] += 1
                        batch_result["total_records"] += len(valid_records)
                        batch_result["security_results"][security_data.id] = {"symbol": security_data.symbol, "type": security_data.security_type, "records": len(valid_records), "date_range": {"start": valid_records[0]["time"].isoformat() if valid_records else None, "end": valid_records[-1]["time"].isoformat() if valid_records else None}}
                    else:
                        batch_result["securities_error"] += 1
                        batch_result["errors"][security_data.id] = {"symbol": security_data.symbol, "type": security_data.security_type, "error": "No valid records after enhanced validation"}
                else:
                    error_msg = self._extract_error_message(response)
                    batch_result["securities_error"] += 1
                    batch_result["errors"][security_data.id] = {"symbol": security_data.symbol, "type": security_data.security_type, "error": error_msg, "response_status": response.get("status") if response else None}

            except Exception as e:
                batch_result["securities_error"] += 1
                batch_result["errors"][security_data.id] = {"symbol": security_data.symbol, "type": security_data.security_type, "error": f"Processing exception: {str(e)}"}
                logger.error(f"Error processing {security_data.symbol}: {str(e)}")

        # Perform bulk database insertion
        if bulk_records_by_security:
            try:
                bulk_results = self.repository.bulk_upsert_optimized(bulk_records_by_security, source="dhan_historical_api")

                # Update results with database operation details
                for security_id, (inserted, updated, total) in bulk_results.items():
                    if security_id in batch_result["security_results"]:
                        batch_result["security_results"][security_id].update({"db_inserted": inserted, "db_updated": updated, "db_total": total})

                if verbose:
                    logger.info(f"Enhanced batch {batch_idx} bulk insert: {len(bulk_records_by_security)} securities")

            except Exception as e:
                logger.error(f"Bulk insert failed for batch {batch_idx}: {str(e)}")
                # Mark all successful securities as errors due to DB failure
                for sec_id in bulk_records_by_security.keys():
                    if sec_id in batch_result["security_results"]:
                        batch_result["errors"][sec_id] = batch_result["security_results"].pop(sec_id)
                        batch_result["errors"][sec_id]["error"] = f"Database insert failed: {str(e)}"

                batch_result["securities_error"] = batch_result["securities_success"]
                batch_result["securities_success"] = 0
                batch_result["status"] = "db_error"

        if verbose:
            logger.info(f"Enhanced batch {batch_idx} completed: " f"{batch_result['securities_success']} success, " f"{batch_result['securities_error']} errors, " f"{batch_result['total_records']} records")

        return batch_result

    def _get_api_parameters_enhanced(self, security_data: SecurityData, start_date: str, end_date: str) -> Dict[str, Any]:
        """Get enhanced API parameters with better mapping."""
        base_params = {"securityId": security_data.external_id, "fromDate": start_date, "toDate": end_date, "expiryCode": 0, "oi": False}

        # Enhanced mapping based on security type and exchange
        if security_data.security_type == "STOCK":
            if security_data.exchange_code == "NSE":
                base_params.update({"exchangeSegment": "NSE_EQ", "instrument": "EQUITY"})
            elif security_data.exchange_code == "BSE":
                base_params.update({"exchangeSegment": "BSE_EQ", "instrument": "EQUITY"})
            else:
                # Default to NSE
                base_params.update({"exchangeSegment": "NSE_EQ", "instrument": "EQUITY"})
        elif security_data.security_type == "INDEX":
            base_params.update({"exchangeSegment": "IDX_I", "instrument": "INDEX"})
        else:
            # Fallback to stock settings
            base_params.update({"exchangeSegment": "NSE_EQ", "instrument": "EQUITY"})

        return base_params

    def _validate_ohlcv_records_enhanced(self, records: List[Dict[str, Any]], symbol: str) -> List[Dict[str, Any]]:
        """Enhanced OHLCV record validation with comprehensive checks."""
        if not records:
            return []

        valid_records = []
        validation_stats = {"total": len(records), "invalid": 0, "duplicates": 0}

        seen_timestamps = set()

        for i, record in enumerate(records):
            try:
                # Required fields check
                required_fields = ["time", "open", "high", "low", "close", "volume"]
                if not all(field in record and record[field] is not None for field in required_fields):
                    validation_stats["invalid"] += 1
                    continue

                # Type validation and conversion
                try:
                    open_val = float(record["open"])
                    high_val = float(record["high"])
                    low_val = float(record["low"])
                    close_val = float(record["close"])
                    volume_val = int(record["volume"])
                except (ValueError, TypeError):
                    validation_stats["invalid"] += 1
                    continue

                # Business logic validation
                if any(val <= 0 for val in [open_val, high_val, low_val, close_val]):
                    validation_stats["invalid"] += 1
                    continue

                if high_val < low_val:
                    validation_stats["invalid"] += 1
                    continue

                if volume_val < 0:
                    validation_stats["invalid"] += 1
                    continue

                # OHLC consistency check (high should be >= open,close and low should be <= open,close)
                if not (low_val <= open_val <= high_val and low_val <= close_val <= high_val):
                    validation_stats["invalid"] += 1
                    continue

                # Duplicate timestamp check
                time_key = str(record["time"])
                if time_key in seen_timestamps:
                    validation_stats["duplicates"] += 1
                    continue
                seen_timestamps.add(time_key)

                # Create validated record
                validated_record = {"time": record["time"], "open": open_val, "high": high_val, "low": low_val, "close": close_val, "volume": volume_val}

                # Add adjusted_close if present
                if "adjusted_close" in record and record["adjusted_close"] is not None:
                    try:
                        validated_record["adjusted_close"] = float(record["adjusted_close"])
                    except (ValueError, TypeError):
                        pass  # Skip adjusted_close if invalid

                valid_records.append(validated_record)

            except Exception as e:
                logger.debug(f"Record validation error for {symbol} at index {i}: {e}")
                validation_stats["invalid"] += 1
                continue

        # Log validation stats for debugging
        if validation_stats["invalid"] > 0 or validation_stats["duplicates"] > 0:
            logger.debug(f"Validation for {symbol}: {len(valid_records)}/{validation_stats['total']} valid " f"({validation_stats['invalid']} invalid, {validation_stats['duplicates']} duplicates)")

        return valid_records

    def _extract_error_message(self, response: Optional[Dict[str, Any]]) -> str:
        """Extract meaningful error message from API response."""
        if response is None:
            return "No response received from API"

        if isinstance(response, dict):
            # Check for explicit error field
            if "error" in response:
                return str(response["error"])

            # Check for status field
            status = response.get("status")
            if status and status != "success":
                return f"API returned status: {status}"

            # Check if data is empty
            data = response.get("data")
            if data is not None and len(data) == 0:
                return "API returned empty data"

        return "API returned unexpected response format"

    def _process_current_data_enhanced(self, securities_by_segment: Dict[str, List[str]], is_eod: bool, verbose: bool) -> Dict[str, Any]:
        """Enhanced current data processing with better error handling."""
        result = {"status": "completed", "securities_with_data": [], "securities_without_data": [], "errors": {}, "total_records_stored": 0, "segment_results": {}}

        try:
            for segment, security_ids in securities_by_segment.items():
                segment_result = {"processed": 0, "success": 0, "errors": 0}

                # Process in optimized batches
                batch_size = min(1000, len(security_ids))

                for i in range(0, len(security_ids), batch_size):
                    batch = security_ids[i : i + batch_size]
                    segment_result["processed"] += len(batch)

                    try:
                        # Prepare API request
                        request_data = {segment: batch}

                        if verbose:
                            logger.debug(f"Fetching current data for segment {segment}, batch size {len(batch)}")

                        # Make API call
                        response = self.api_client.fetch_current_data(request_data)

                        if response:
                            # Get optimized ID mapping
                            id_mapping = self._get_security_id_mapping_optimized(batch)

                            # Process response data
                            bulk_records = {}
                            batch_success = 0
                            batch_errors = 0

                            for ext_id, data in response.items():
                                try:
                                    int_id = id_mapping.get(ext_id)
                                    if not int_id:
                                        logger.debug(f"No internal ID mapping for external ID {ext_id}")
                                        continue

                                    # Create and validate record
                                    record = {"time": data.get("timestamp"), "open": data.get("open"), "high": data.get("high"), "low": data.get("low"), "close": data.get("close"), "volume": data.get("volume", 0)}

                                    if self._validate_current_data_record_enhanced(record):
                                        bulk_records[int_id] = [record]
                                        result["securities_with_data"].append(int_id)
                                        batch_success += 1
                                    else:
                                        result["securities_without_data"].append(int_id)
                                        if verbose:
                                            logger.debug(f"Invalid current data for security {int_id}")

                                except Exception as e:
                                    result["errors"][ext_id] = f"Record processing error: {str(e)}"
                                    batch_errors += 1

                            # Bulk insert valid records
                            if bulk_records:
                                try:
                                    bulk_results = self.repository.bulk_upsert_optimized(bulk_records, source="dhan_quote_api")
                                    result["total_records_stored"] += len(bulk_records)
                                    segment_result["success"] += batch_success

                                    if verbose:
                                        logger.info(f"Segment {segment} batch: {len(bulk_records)} records stored")

                                except Exception as e:
                                    logger.error(f"Bulk insert failed for segment {segment}: {str(e)}")
                                    segment_result["errors"] += len(bulk_records)
                                    for int_id in bulk_records.keys():
                                        result["errors"][int_id] = f"Database insert failed: {str(e)}"

                            segment_result["errors"] += batch_errors

                        else:
                            # No response from API
                            segment_result["errors"] += len(batch)
                            for ext_id in batch:
                                result["errors"][ext_id] = "No API response"

                    except Exception as e:
                        logger.error(f"Error processing segment {segment} batch: {str(e)}")
                        segment_result["errors"] += len(batch)
                        for ext_id in batch:
                            result["errors"][ext_id] = f"Batch processing error: {str(e)}"

                result["segment_results"][segment] = segment_result

                if verbose:
                    logger.info(f"Segment {segment} completed: " f"{segment_result['success']} success, " f"{segment_result['errors']} errors")

        except Exception as e:
            logger.error(f"Critical error in current data processing: {str(e)}")
            result["status"] = "error"
            result["error"] = str(e)

        return result

    def _get_security_id_mapping_optimized(self, external_ids: List[str]) -> Dict[str, str]:
        """Get optimized security ID mapping with enhanced caching."""
        mapping = {}

        # Check cache first
        uncached_ids = []
        with self._cache_lock:
            for ext_id in external_ids:
                if ext_id in self._id_mapping_cache:
                    mapping[ext_id] = self._id_mapping_cache[ext_id]
                    self.performance_monitor.record_cache_hit()
                else:
                    uncached_ids.append(ext_id)
                    self.performance_monitor.record_cache_miss()

        # Fetch uncached mappings in optimized batches
        if uncached_ids:
            try:
                with get_db() as db:
                    # Convert to integers for database query
                    int_external_ids = []
                    for ext_id in uncached_ids:
                        try:
                            int_external_ids.append(int(ext_id))
                        except ValueError:
                            logger.warning(f"Invalid external ID format: {ext_id}")
                            continue

                    if int_external_ids:
                        # Optimized batch query
                        securities = db.query(Security.id, Security.external_id).filter(Security.external_id.in_(int_external_ids), Security.security_type.in_(["STOCK", "INDEX"]), Security.is_active == True).all()

                        # Update mapping and cache
                        with self._cache_lock:
                            for security_id, external_id in securities:
                                str_ext_id = str(external_id)
                                str_sec_id = str(security_id)
                                mapping[str_ext_id] = str_sec_id
                                self._id_mapping_cache[str_ext_id] = str_sec_id

            except Exception as e:
                logger.error(f"Error fetching security ID mappings: {e}")

        return mapping

    def _validate_current_data_record_enhanced(self, record: Dict[str, Any]) -> bool:
        """Enhanced validation for current day data records."""
        try:
            # Required fields check
            required_fields = ["time", "open", "high", "low", "close"]
            if not all(field in record and record[field] is not None for field in required_fields):
                return False

            # Type and value validation
            try:
                open_val = float(record["open"])
                high_val = float(record["high"])
                low_val = float(record["low"])
                close_val = float(record["close"])
                volume_val = int(record.get("volume", 0))
            except (ValueError, TypeError):
                return False

            # Business logic validation
            if any(val <= 0 for val in [open_val, high_val, low_val, close_val]):
                return False

            if high_val < low_val:
                return False

            if volume_val < 0:
                return False

            # OHLC consistency check
            if not (low_val <= open_val <= high_val and low_val <= close_val <= high_val):
                return False

            # Time validation
            if not record["time"]:
                return False

            return True

        except Exception as e:
            logger.debug(f"Current data validation error: {e}")
            return False

    def _group_securities_by_segment(self, securities_data: List[SecurityData]) -> Dict[str, List[str]]:
        """Enhanced grouping of securities by exchange segment."""
        result = {}

        for security_data in securities_data:
            try:
                # Determine exchange segment based on security type and exchange
                if security_data.security_type == "STOCK":
                    if security_data.exchange_code == "BSE":
                        exchange_segment = "BSE_EQ"
                    else:  # Default to NSE
                        exchange_segment = "NSE_EQ"
                elif security_data.security_type == "INDEX":
                    exchange_segment = "IDX_I"
                else:
                    logger.warning(f"Unknown security type: {security_data.security_type} for {security_data.symbol}")
                    continue

                # Add to appropriate group
                if exchange_segment not in result:
                    result[exchange_segment] = []

                result[exchange_segment].append(security_data.external_id)

            except Exception as e:
                logger.error(f"Error grouping security {security_data.symbol}: {str(e)}")
                continue

        # Log grouping results
        for segment, ids in result.items():
            logger.info(f"Segment {segment}: {len(ids)} securities")

        return result

    def _periodic_memory_cleanup(self):
        """Enhanced periodic memory cleanup."""
        try:
            with self._cache_lock:
                # Clear caches if they exceed threshold
                if len(self._security_cache) > self.config.cache_clear_threshold:
                    self._security_cache.clear()
                    logger.debug("Cleared security cache")

                if len(self._id_mapping_cache) > self.config.cache_clear_threshold:
                    # Keep only the most recently used half
                    items = list(self._id_mapping_cache.items())
                    self._id_mapping_cache = dict(items[-self.config.cache_clear_threshold // 2 :])
                    logger.debug(f"Trimmed ID mapping cache to {len(self._id_mapping_cache)} items")

            # Force garbage collection
            collected = gc.collect()
            if collected > 0:
                logger.debug(f"Garbage collection freed {collected} objects")

            self.performance_monitor.record_memory_cleanup()

        except Exception as e:
            logger.warning(f"Error during memory cleanup: {e}")

    def _cleanup_resources(self):
        """Enhanced resource cleanup."""
        try:
            # Clear all caches
            with self._cache_lock:
                self._security_cache.clear()
                self._id_mapping_cache.clear()

            # Force garbage collection
            gc.collect()

            logger.debug("Resources cleaned up successfully")

        except Exception as e:
            logger.warning(f"Error during resource cleanup: {e}")

    def _create_error_result(self, operation_id: str, error_message: str, start_time: float) -> Dict[str, Any]:
        """Create standardized error result."""
        return {"operation_id": operation_id, "status": "error", "error": error_message, "duration_seconds": round(time.time() - start_time, 2), "stats": {"securities_processed": 0, "securities_success": 0, "securities_error": 0, "total_records": 0}, "securities": {"total": 0, "success": 0, "error": 0}}

    def _compile_operation_result(self, operation_id: str, batch_results: List[Dict[str, Any]], total_securities: int, start_time: float) -> Dict[str, Any]:
        """Compile comprehensive operation result."""
        # Aggregate statistics
        stats = {"batches_total": len(batch_results), "batches_completed": sum(1 for r in batch_results if r.get("status") == "completed"), "batches_error": sum(1 for r in batch_results if r.get("status") != "completed"), "securities_processed": sum(r.get("securities_processed", 0) for r in batch_results), "securities_success": sum(r.get("securities_success", 0) for r in batch_results), "securities_error": sum(r.get("securities_error", 0) for r in batch_results), "securities_skipped": sum(r.get("securities_skipped", 0) for r in batch_results), "total_records": sum(r.get("total_records", 0) for r in batch_results)}

        duration_seconds = time.time() - start_time

        # Determine overall status
        overall_status = "completed"
        if stats["batches_error"] > 0:
            if stats["batches_completed"] == 0:
                overall_status = "failed"
            else:
                overall_status = "partial"

        result = {"operation_id": operation_id, "status": overall_status, "duration_seconds": round(duration_seconds, 2), "stats": stats, "securities": {"total": total_securities, "success": stats["securities_success"], "error": stats["securities_error"], "skipped": stats["securities_skipped"]}, "performance": {"records_per_second": round(stats["total_records"] / duration_seconds, 2) if duration_seconds > 0 else 0, "securities_per_second": round(stats["securities_processed"] / duration_seconds, 2) if duration_seconds > 0 else 0, "avg_records_per_security": round(stats["total_records"] / stats["securities_success"], 2) if stats["securities_success"] > 0 else 0}}

        # Add error details if there were failures
        if stats["batches_error"] > 0:
            error_details = {}
            for batch_result in batch_results:
                if batch_result.get("status") != "completed" and "errors" in batch_result:
                    error_details.update(batch_result["errors"])

            if error_details:
                result["error_details"] = error_details

        return result

    def _create_update_summary(self, historical_result: Optional[Dict[str, Any]], current_result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Create summary for comprehensive update operation."""
        summary = {"total_operations": 0, "successful_operations": 0, "total_records": 0, "total_securities": 0, "operations": []}

        if historical_result:
            summary["total_operations"] += 1
            if historical_result.get("status") == "completed":
                summary["successful_operations"] += 1
            summary["total_records"] += historical_result.get("stats", {}).get("total_records", 0)
            summary["total_securities"] = max(summary["total_securities"], historical_result.get("securities", {}).get("total", 0))
            summary["operations"].append({"type": "historical", "status": historical_result.get("status"), "records": historical_result.get("stats", {}).get("total_records", 0)})

        if current_result:
            summary["total_operations"] += 1
            if current_result.get("status") == "completed":
                summary["successful_operations"] += 1
            summary["total_records"] += current_result.get("stats", {}).get("total_records_stored", 0)
            summary["total_securities"] = max(summary["total_securities"], current_result.get("securities", {}).get("total", 0))
            summary["operations"].append({"type": "current", "status": current_result.get("status"), "records": current_result.get("stats", {}).get("total_records_stored", 0)})

        return summary

    # Health and monitoring methods

    def get_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status of the fetcher."""
        try:
            # Get API client health
            api_health = asyncio.run(self.api_client.health_check())

            # Get repository health
            repo_stats = self.repository.get_performance_stats()

            # Get performance stats
            perf_stats = self.performance_monitor.get_stats()

            # Determine overall health
            overall_status = "healthy"
            if api_health.get("status") != "healthy":
                overall_status = "degraded"

            return {"status": overall_status, "timestamp": datetime.now().isoformat(), "components": {"api_client": api_health, "repository": {"status": "healthy" if repo_stats.get("success_rate_pct", 0) > 90 else "degraded", "stats": repo_stats}, "fetcher": {"status": "healthy" if perf_stats.get("success_rate_pct", 0) > 90 else "degraded", "stats": perf_stats}}, "cache_status": {"security_cache_size": len(self._security_cache), "id_mapping_cache_size": len(self._id_mapping_cache), "cache_hit_rate_pct": perf_stats.get("cache_hit_rate_pct", 0)}}

        except Exception as e:
            return {"status": "unhealthy", "error": str(e), "timestamp": datetime.now().isoformat()}

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics."""
        return {"fetcher": self.performance_monitor.get_stats(), "api_client": self.api_client.get_performance_stats(), "repository": self.repository.get_performance_stats()}

    def reset_performance_stats(self):
        """Reset all performance statistics."""
        self.performance_monitor = PerformanceMonitor()
        self.api_client.reset_performance_stats()
        self.repository.reset_performance_stats()
        logger.info("All performance statistics reset")

    async def close(self):
        """Close the fetcher and cleanup resources."""
        try:
            await self.api_client.close()
            self._cleanup_resources()
            logger.info("OHLCV Fetcher closed successfully")
        except Exception as e:
            logger.error(f"Error closing OHLCV Fetcher: {e}")

    def __del__(self):
        """Cleanup on deletion."""
        try:
            self._cleanup_resources()
        except:
            pass  # Best effort cleanup
