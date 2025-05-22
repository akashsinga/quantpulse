# app/services/data_fetchers/ohlcv_fetcher.py

import uuid
import time
import concurrent.futures
from datetime import datetime, date, timedelta
from typing import List, Dict, Tuple, Optional, Any, Union, Set
from sqlalchemy import func
from sqlalchemy.orm import joinedload, Session

from app.db.session import get_db, SessionLocal
from app.db.models.security import Security
from app.db.models.exchange import Exchange
from app.config import settings
from utils.logger import get_logger
from .dhan_api_client import DhanAPIClient
from .exchange_mapper import ExchangeMapper
from ..repositories.ohlcv_repository import OHLCVRepository

logger = get_logger(__name__)


class OHLCVFetcher:
    """Main coordinator for OHLCV data fetching operations - Stocks and Indices only."""

    def __init__(self, api_client: Optional[DhanAPIClient] = None, mapper: Optional[ExchangeMapper] = None, repository: Optional[OHLCVRepository] = None):
        """Initialize the OHLCV fetcher with its dependencies."""
        self.api_client = api_client or DhanAPIClient()
        self.mapper = mapper or ExchangeMapper()
        self.repository = repository or OHLCVRepository()

        # Maximum performance parameters
        self.default_workers = 24  # Increased for maximum throughput
        self.default_batch_size = 200  # Larger batches for efficiency

        # Async processing settings
        self.max_concurrent_requests = 50  # Maximum concurrent API calls
        self.request_delay = 0.01  # Minimal delay between requests (10ms)

        # Cache for security metadata to reduce DB queries
        self._security_type_cache = {}
        self._security_exchange_cache = {}  # New cache for exchange mapping
        self._bulk_insert_cache = []  # Cache for bulk operations

        logger.info("Initialized OHLCV fetcher - MAXIMUM PERFORMANCE MODE (24 workers, 50 concurrent requests)")

    def fetch_historical_data(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, workers: int = 24, batch_size: int = 200, verbose: bool = False) -> Dict[str, Any]:
        """Fetch historical OHLCV data for stocks and indices only."""
        operation_id = str(uuid.uuid4())
        start_time = time.time()

        # Use defaults from settings if not specified
        start_date = start_date or settings.FROM_DATE
        end_date = end_date or (settings.TO_DATE or datetime.now().strftime("%Y-%m-%d"))

        logger.info(f"Starting historical data fetch operation {operation_id}")
        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info("Fetching data for STOCKS and INDICES only (excluding derivatives)")

        # Step 1: Select securities to process (stocks and indices only)
        securities = self._select_securities_stocks_indices_only(security_ids, exchanges, segments)

        if not securities:
            msg = "No stocks or indices found matching the specified criteria"
            logger.warning(msg)
            return {"operation_id": operation_id, "status": "error", "message": msg, "stats": {"securities_processed": 0, "total_records": 0}}

        logger.info(f"Selected {len(securities)} securities for processing (stocks: {sum(1 for s in securities if s.security_type == 'STOCK')}, indices: {sum(1 for s in securities if s.security_type == 'INDEX')})")

        # Pre-populate security type cache for better performance
        self._preload_security_cache(securities)

        # Step 2: Create batches for parallel processing
        batches = self._create_optimized_batches(securities, batch_size)
        logger.info(f"Created {len(batches)} batches for processing")

        # Step 3: Process batches in parallel with improved reliability
        results = self._process_historical_batches_improved(batches, start_date, end_date, workers, verbose)

        # Step 4: Compile statistics
        stats = self._compile_statistics(results)

        # Calculate duration
        duration_seconds = time.time() - start_time

        # Prepare result
        operation_result = {"operation_id": operation_id, "status": "completed", "duration_seconds": duration_seconds, "start_date": start_date, "end_date": end_date, "stats": stats, "securities": {"total": len(securities), "success": stats["securities_success"], "error": stats["securities_error"], "skipped": stats.get("securities_skipped", 0)}}

        logger.info(f"Completed historical data fetch operation {operation_id} in {duration_seconds:.2f} seconds")
        logger.info(f"Processed {stats['securities_processed']} securities with {stats['total_records']} records")

        # Clear caches to free memory
        self._clear_caches()

        return operation_result

    def fetch_current_day_data(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None, is_eod: bool = False, workers: int = 20, batch_size: int = 1000, verbose: bool = False) -> Dict[str, Any]:
        """Fetch current day OHLCV data for stocks and indices only."""
        operation_id = str(uuid.uuid4())
        start_time = time.time()

        logger.info(f"Starting current day data fetch operation {operation_id}")
        logger.info(f"Mode: {'EOD' if is_eod else 'Regular'}")
        logger.info("Fetching data for STOCKS and INDICES only (excluding derivatives)")

        # Step 1: Select securities to process (stocks and indices only)
        securities = self._select_securities_stocks_indices_only(security_ids, exchanges, segments)

        if not securities:
            msg = "No stocks or indices found matching the specified criteria"
            logger.warning(msg)
            return {"operation_id": operation_id, "status": "error", "message": msg, "stats": {"securities_processed": 0}}

        logger.info(f"Selected {len(securities)} securities for current day data")

        # Step 2: Group securities by exchange segment for efficient API calls
        securities_by_segment = self._group_securities_by_segment_optimized(securities)

        # Step 3: Process securities by segment
        results = self._process_current_data(securities_by_segment, is_eod, verbose)

        # Step 4: Compile statistics
        stats = {"securities_processed": sum(len(ids) for ids in securities_by_segment.values()), "securities_with_data": len(results.get("securities_with_data", [])), "securities_without_data": len(results.get("securities_without_data", [])), "securities_error": len(results.get("errors", {})), "total_records_stored": results.get("total_records_stored", 0)}

        # Calculate duration
        duration_seconds = time.time() - start_time

        # Prepare result
        operation_result = {"operation_id": operation_id, "status": "completed", "duration_seconds": duration_seconds, "mode": "eod" if is_eod else "regular", "timestamp": datetime.now().isoformat(), "stats": stats, "securities": {"total": len(securities), "with_data": stats["securities_with_data"], "without_data": stats["securities_without_data"], "error": stats["securities_error"]}}

        logger.info(f"Completed current day data fetch operation {operation_id} in {duration_seconds:.2f} seconds")
        logger.info(f"Processed {stats['securities_processed']} securities, stored data for {stats['securities_with_data']}")

        # Clear caches to free memory
        self._clear_caches()

        return operation_result

    def update_all_data(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None, days_back: int = 7, include_today: bool = True, workers: int = 24, batch_size: int = 200, verbose: bool = False, full_history: bool = False) -> Dict[str, Any]:
        """Combined workflow to update both historical and current day data for stocks and indices only."""
        operation_id = str(uuid.uuid4())
        start_time = time.time()

        logger.info(f"Starting comprehensive data update operation {operation_id}")
        logger.info(f"Checking {days_back} days back, include_today={include_today}, full_history={full_history}")
        logger.info("Processing STOCKS and INDICES only (excluding derivatives)")

        # Calculate date range for historical data
        if full_history:
            # Use the default FROM_DATE from settings for full history
            start_date = datetime.strptime(settings.FROM_DATE, "%Y-%m-%d").date()
            end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
            logger.info(f"Using full history date range: {start_date} to {end_date}")
        else:
            # Use the days_back parameter for recent data only
            end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
            start_date = end_date - timedelta(days=days_back)
            logger.info(f"Using recent date range: {start_date} to {end_date}")

        historical_result = None
        current_result = None

        # Step 1: Fetch historical data
        if days_back > 0 or full_history:
            historical_result = self.fetch_historical_data(security_ids=security_ids, exchanges=exchanges, segments=segments, start_date=start_date.strftime("%Y-%m-%d"), end_date=end_date.strftime("%Y-%m-%d"), workers=workers, batch_size=batch_size, verbose=verbose)

        # Step 2: Fetch today's data if requested
        if include_today:
            current_result = self.fetch_current_day_data(security_ids=security_ids, exchanges=exchanges, segments=segments, is_eod=True, workers=min(workers, 6), batch_size=batch_size * 4, verbose=verbose)  # Use fewer workers for current data  # Larger batches for current data

        # Calculate duration
        duration_seconds = time.time() - start_time

        # Prepare result
        operation_result = {"operation_id": operation_id, "status": "completed", "duration_seconds": duration_seconds, "historical": historical_result, "current": current_result, "days_back": days_back, "include_today": include_today, "full_history": full_history}

        logger.info(f"Completed comprehensive data update operation {operation_id} in {duration_seconds:.2f} seconds")

        return operation_result

    def _select_securities_stocks_indices_only(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None) -> List[Security]:
        """Select securities based on provided criteria - STOCKS and INDICES only."""
        logger.info("Selecting securities for processing (STOCKS and INDICES only)")

        with get_db() as db:
            # Use joinedload to eagerly load relationships, but exclude derivatives
            query = db.query(Security).options(joinedload(Security.exchange)).filter(Security.is_active == True, Security.security_type.in_(["STOCK", "INDEX"]))  # Only stocks and indices

            # Apply filters...
            if security_ids:
                query = query.filter(Security.id.in_(security_ids))

            if exchanges:
                query = query.join(Security.exchange).filter(Exchange.code.in_([code.upper() for code in exchanges]))

            if segments:
                query = query.filter(Security.segment.in_(segments))

            query = query.order_by(Security.security_type, Security.symbol)  # Order by type first, then symbol
            securities = query.all()

            # Create copies with relevant attributes to detach from session
            detached_securities = []
            for security in securities:
                # Pre-cache security type
                self._security_type_cache[str(security.id)] = security.security_type
                detached_securities.append(security)

            db.expunge_all()

            stock_count = sum(1 for s in detached_securities if s.security_type == "STOCK")
            index_count = sum(1 for s in detached_securities if s.security_type == "INDEX")

            logger.info(f"Selected {len(detached_securities)} securities: {stock_count} stocks, {index_count} indices")
            return detached_securities

    def _create_optimized_batches(self, securities: List[Security], batch_size: int) -> List[List[Security]]:
        """Create optimized batches separating stocks and indices for better processing."""
        # Separate stocks and indices for optimized processing
        stocks = [s for s in securities if s.security_type == "STOCK"]
        indices = [s for s in securities if s.security_type == "INDEX"]

        # Create batches for each type
        stock_batches = [stocks[i : i + batch_size] for i in range(0, len(stocks), batch_size)]
        index_batches = [indices[i : i + batch_size] for i in range(0, len(indices), batch_size)]

        # Process indices first (typically fewer and faster), then stocks
        all_batches = index_batches + stock_batches

        logger.info(f"Created {len(all_batches)} optimized batches ({len(index_batches)} index batches, {len(stock_batches)} stock batches)")

        return all_batches

    def _process_historical_batches_improved(self, batches: List[List[Security]], start_date: str, end_date: str, workers: int, verbose: bool) -> List[Dict[str, Any]]:
        """Process batches with improved reliability and rate limiting."""
        results = []

        # Enhanced rate limiting with semaphore
        from concurrent.futures import ThreadPoolExecutor
        from threading import BoundedSemaphore
        import time

        # Maximum throughput with aggressive rate limiting
        api_semaphore = BoundedSemaphore(self.max_concurrent_requests)  # Use class setting

        def process_with_maximum_speed(batch, batch_idx):
            with api_semaphore:
                # No delay - maximum speed processing
                return self._process_historical_batch_improved(batch, start_date, end_date, verbose, batch_idx)

        # Use maximum worker count with proper resource management
        max_workers = min(workers, 32)  # Cap at 32 for system stability
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all batches for processing
            futures = [executor.submit(process_with_maximum_speed, batch, batch_idx) for batch_idx, batch in enumerate(batches)]

            # Collect results as they complete with minimal logging overhead
            for future in concurrent.futures.as_completed(futures):
                try:
                    batch_result = future.result()
                    results.append(batch_result)

                    # Minimal progress logging to avoid slowing down
                    completed = len(results)
                    if completed % max(1, len(batches) // 5) == 0 or completed == len(batches):  # Every 20%
                        progress_pct = (completed / len(batches)) * 100
                        logger.info(f"⚡ Speed: {completed}/{len(batches)} batches ({progress_pct:.0f}%)")

                except Exception as e:
                    logger.error(f"Batch error: {str(e)}")
                    results.append({"status": "error", "error": str(e), "securities_processed": 0, "securities_success": 0, "securities_error": 1, "total_records": 0})

        return results

    def _process_historical_batch_improved(self, batch: List[Security], start_date: str, end_date: str, verbose: bool, batch_idx: int) -> Dict[str, Any]:
        """Process a single batch with improved logic for stocks and indices only."""
        batch_results = {"batch_idx": batch_idx, "status": "completed", "securities_processed": len(batch), "securities_success": 0, "securities_error": 0, "securities_skipped": 0, "total_records": 0, "security_results": {}, "errors": {}}

        if verbose:
            logger.debug(f"Processing batch {batch_idx} with {len(batch)} securities (date range: {start_date} to {end_date})")

        # Create a single session for the entire batch
        with get_db() as db:
            # Determine batch type for optimized processing
            batch_types = set(s.security_type for s in batch)
            is_homogeneous_batch = len(batch_types) == 1
            batch_type = next(iter(batch_types)) if is_homogeneous_batch else "MIXED"

            if verbose:
                logger.debug(f"Batch {batch_idx} type: {batch_type}")

            for security in batch:
                try:
                    # Get security type from cache
                    security_type = self._security_type_cache.get(str(security.id), security.security_type)

                    # Skip if somehow a derivative got through (safety check)
                    if security_type not in ["STOCK", "INDEX"]:
                        logger.warning(f"Skipping non-stock/index security: {security.symbol} (type: {security_type})")
                        batch_results["securities_skipped"] += 1
                        continue

                    # Prepare API parameters based on security type
                    if security_type == "STOCK":
                        params = {"securityId": str(security.external_id), "exchangeSegment": "NSE_EQ", "instrument": "EQUITY", "fromDate": start_date, "toDate": end_date, "expiryCode": 0, "oi": False}
                    elif security_type == "INDEX":
                        params = {"securityId": str(security.external_id), "exchangeSegment": "IDX_I", "instrument": "INDEX", "fromDate": start_date, "toDate": end_date, "expiryCode": 0, "oi": False}
                    else:
                        # Fallback (should not reach here due to earlier check)
                        logger.warning(f"Unknown security type for {security.symbol}: {security_type}")
                        batch_results["securities_error"] += 1
                        continue

                    if verbose:
                        logger.debug(f"{security_type} params for {security.symbol}: {params}")

                    # Make API call
                    response = self.api_client.fetch_historical_data(**params)

                    # Process response
                    if response and response.get("status") == "success" and response.get("data"):
                        records = response.get("data", [])

                        # Fast validation - only essential checks
                        if records and len(records) > 0:
                            # Quick validation without full record check for speed
                            valid_records = [r for r in records if r.get("open", 0) > 0 and r.get("close", 0) > 0]

                        if valid_records:
                            # Store data in the repository
                            result = self.repository.upsert_daily_data(str(security.id), valid_records, source="dhan_historical_api")

                            # Update batch results
                            batch_results["securities_success"] += 1
                            batch_results["total_records"] += len(valid_records)
                            batch_results["security_results"][str(security.id)] = {"symbol": security.symbol, "type": security_type, "records": len(valid_records), "inserted": result[0], "updated": result[1]}

                            if verbose:
                                logger.debug(f"Processed {security.symbol} ({security_type}): {len(valid_records)} valid records")
                        else:
                            batch_results["securities_error"] += 1
                            batch_results["errors"][str(security.id)] = {"symbol": security.symbol, "type": security_type, "error": "No valid records after validation"}
                    else:
                        # Handle empty or error response
                        error_msg = "No response received" if response is None else response.get("error", "No data returned or API error")
                        batch_results["securities_error"] += 1
                        batch_results["errors"][str(security.id)] = {"symbol": security.symbol, "type": security_type, "error": error_msg, "response_status": None if response is None else response.get("status")}

                        if verbose:
                            logger.warning(f"No data for {security.symbol} ({security_type}): {error_msg}")

                except Exception as e:
                    # Handle any exceptions during processing
                    batch_results["securities_error"] += 1
                    batch_results["errors"][str(security.id)] = {"symbol": security.symbol, "type": getattr(security, "security_type", "unknown"), "error": str(e)}

                    logger.error(f"Error processing {security.symbol}: {str(e)}")

            if verbose:
                logger.info(f"Completed batch {batch_idx}: {batch_results['securities_success']} success, {batch_results['securities_error']} error, {batch_results['securities_skipped']} skipped")

        return batch_results

    def _validate_ohlcv_records(self, records: List[Dict[str, Any]], symbol: str) -> List[Dict[str, Any]]:
        """Validate OHLCV records for data quality."""
        valid_records = []

        for record in records:
            try:
                # Check required fields exist and are not None
                required_fields = ["time", "open", "high", "low", "close", "volume"]
                if not all(field in record and record[field] is not None for field in required_fields):
                    continue

                # Check for reasonable price values
                ohlc_values = [record["open"], record["high"], record["low"], record["close"]]
                if any(val <= 0 for val in ohlc_values):
                    continue

                # Check high >= low
                if record["high"] < record["low"]:
                    continue

                # Check volume is non-negative
                if record["volume"] < 0:
                    continue

                valid_records.append(record)

            except Exception as e:
                logger.debug(f"Record validation error for {symbol}: {str(e)}")
                continue

        return valid_records

    def _group_securities_by_segment_optimized(self, securities: List[Security]) -> Dict[str, List[str]]:
        """Group securities by exchange segment with optimized mapping for stocks and indices."""
        result = {}

        for security in securities:
            try:
                # Simplified mapping for stocks and indices only
                if security.security_type == "STOCK":
                    exchange_segment = "NSE_EQ"
                elif security.security_type == "INDEX":
                    exchange_segment = "IDX_I"  # Corrected from NSE_IDX to IDX_I
                else:
                    # Skip derivatives (safety check)
                    logger.warning(f"Skipping non-stock/index security in grouping: {security.symbol}")
                    continue

                # Get Dhan's security ID
                security_id = str(security.external_id)

                # Add to the appropriate group
                if exchange_segment not in result:
                    result[exchange_segment] = []

                result[exchange_segment].append(security_id)

            except Exception as e:
                logger.error(f"Error grouping security {security.symbol}: {str(e)}")

        logger.info(f"Grouped securities: NSE_EQ={len(result.get('NSE_EQ', []))}, IDX_I={len(result.get('IDX_I', []))}")
        return result

    def _preload_security_cache(self, securities: List[Security]):
        """Preload security metadata into caches for better performance."""
        # Update cache with security types and exchange info
        for security in securities:
            sec_id = str(security.id)
            self._security_type_cache[sec_id] = security.security_type

            # Cache exchange mapping for faster lookup
            if hasattr(security, "exchange") and security.exchange:
                self._security_exchange_cache[sec_id] = security.exchange.code
            elif hasattr(security, "exchange_id"):
                # Will be resolved later if needed
                self._security_exchange_cache[sec_id] = None

        logger.info(f"Preloaded metadata for {len(securities)} securities with exchange info")

    def _clear_caches(self):
        """Clear internal caches to free memory."""
        self._security_type_cache.clear()
        self._security_exchange_cache.clear()
        self._bulk_insert_cache.clear()

    def _process_current_data(self, securities_by_segment: Dict[str, List[str]], is_eod: bool, verbose: bool) -> Dict[str, Any]:
        """Process current day data for securities grouped by segment."""
        result = {"status": "completed", "securities_with_data": [], "securities_without_data": [], "errors": {}, "total_records_stored": 0}

        # Create a session for the entire process
        with get_db() as db:
            # Process each segment
            for segment, security_ids in securities_by_segment.items():
                try:
                    # Fetch current data for this segment
                    if verbose:
                        logger.debug(f"Fetching current data for segment {segment} with {len(security_ids)} securities")

                    # Process in larger batches of 1000 for speed
                    batch_size = 1000
                    for i in range(0, len(security_ids), batch_size):
                        batch = security_ids[i : i + batch_size]

                        # Prepare request
                        request_data = {segment: batch}

                        # Fetch data from API
                        response = self.api_client.fetch_current_data(request_data)

                        # Process response
                        if response:
                            # Get internal security IDs for external IDs
                            ext_to_int_map = self._get_security_id_mapping(batch, db)

                            # Process each security's data
                            for ext_id, data in response.items():
                                try:
                                    # Get internal security ID
                                    int_id = ext_to_int_map.get(ext_id)

                                    if not int_id:
                                        logger.warning(f"No internal ID mapping for external ID {ext_id}")
                                        continue

                                    # Create OHLCV record
                                    record = {"time": data.get("timestamp"), "open": data.get("open"), "high": data.get("high"), "low": data.get("low"), "close": data.get("close"), "volume": data.get("volume", 0)}

                                    # Validate record
                                    if self._validate_current_data_record(record):
                                        # Store in repository
                                        self.repository.upsert_daily_data(int_id, [record], source="dhan_quote_api")

                                        result["securities_with_data"].append(int_id)
                                        result["total_records_stored"] += 1

                                        if verbose:
                                            logger.debug(f"Stored current data for security {int_id}")
                                    else:
                                        result["securities_without_data"].append(int_id)

                                        if verbose:
                                            logger.debug(f"Invalid or empty data for security {int_id}")

                                except Exception as e:
                                    logger.error(f"Error processing current data for security {ext_id}: {str(e)}")
                                    result["errors"][ext_id] = str(e)

                        # Log batch progress
                        if verbose:
                            logger.debug(f"Processed batch of {len(batch)} securities for segment {segment}")

                except Exception as e:
                    logger.error(f"Error processing segment {segment}: {str(e)}")
                    for id in security_ids:
                        result["errors"][id] = f"Segment error: {str(e)}"

        return result

    def _get_security_id_mapping(self, external_ids: List[str], session: Optional[Session] = None) -> Dict[str, str]:
        """Get mapping from external security IDs to internal UUIDs."""
        mapping = {}

        # Use provided session or create a new one
        should_close_session = False
        if session is None:
            session = SessionLocal()
            should_close_session = True

        try:
            # Performance optimization: Use a single query with IN clause
            try:
                # Convert all external IDs to integers
                int_external_ids = [int(id) for id in external_ids]

                # Query in larger batches of 1000 for better performance
                batch_size = 1000
                for i in range(0, len(int_external_ids), batch_size):
                    batch = int_external_ids[i : i + batch_size]
                    securities = session.query(Security.id, Security.external_id).filter(Security.external_id.in_(batch), Security.security_type.in_(["STOCK", "INDEX"])).all()  # Only stocks and indices

                    # Update mapping
                    for security_id, external_id in securities:
                        mapping[str(external_id)] = str(security_id)
            except Exception as e:
                logger.error(f"Error in bulk security ID mapping: {e}")
                # Fallback to individual queries if bulk query fails
                for ext_id in external_ids:
                    try:
                        security = session.query(Security.id).filter(Security.external_id == int(ext_id), Security.security_type.in_(["STOCK", "INDEX"])).first()  # Only stocks and indices
                        if security:
                            mapping[ext_id] = str(security.id)
                    except:
                        pass

            return mapping

        finally:
            # Only close if we created it
            if should_close_session:
                session.close()

    def _validate_current_data_record(self, record: Dict[str, Any]) -> bool:
        """Validate a current day data record."""
        # Check for required fields
        for field in ["time", "open", "high", "low", "close"]:
            if field not in record or record[field] is None:
                return False

        # Check for reasonable values
        if record["high"] < record["low"]:
            return False

        if record["open"] < 0 or record["high"] < 0 or record["low"] < 0 or record["close"] < 0:
            return False

        # Volume can be zero on some exchanges
        if record["volume"] < 0:
            return False

        return True

    def _compile_statistics(self, batch_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Compile statistics from batch processing results."""
        stats = {"batches_total": len(batch_results), "batches_completed": 0, "batches_error": 0, "securities_processed": 0, "securities_success": 0, "securities_error": 0, "securities_skipped": 0, "total_records": 0}

        for result in batch_results:
            if result.get("status") == "completed":
                stats["batches_completed"] += 1
            else:
                stats["batches_error"] += 1

            stats["securities_processed"] += result.get("securities_processed", 0)
            stats["securities_success"] += result.get("securities_success", 0)
            stats["securities_error"] += result.get("securities_error", 0)
            stats["securities_skipped"] += result.get("securities_skipped", 0)
            stats["total_records"] += result.get("total_records", 0)

        return stats
