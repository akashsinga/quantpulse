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
from app.db.models.derivatives import Future
from app.config import settings
from utils.logger import get_logger
from .dhan_api_client import DhanAPIClient
from .exchange_mapper import ExchangeMapper
from ..repositories.ohlcv_repository import OHLCVRepository

logger = get_logger(__name__)


class OHLCVFetcher:
    """Main coordinator for OHLCV data fetching operations."""

    def __init__(self, api_client: Optional[DhanAPIClient] = None, mapper: Optional[ExchangeMapper] = None, repository: Optional[OHLCVRepository] = None):
        """Initialize the OHLCV fetcher with its dependencies."""
        self.api_client = api_client or DhanAPIClient()
        self.mapper = mapper or ExchangeMapper()
        self.repository = repository or OHLCVRepository()

        # Operational parameters - increased for better performance
        self.default_workers = 12  # Increased from 8
        self.default_batch_size = 100  # Increased from 50

        # Cache for security metadata to reduce DB queries
        self._security_type_cache = {}
        self._futures_cache = {}

        logger.info("Initialized OHLCV fetcher with optimized parameters")

    def fetch_historical_data(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, workers: int = 12, batch_size: int = 100, verbose: bool = False) -> Dict[str, Any]:
        """Fetch historical OHLCV data for specified securities."""
        operation_id = str(uuid.uuid4())
        start_time = time.time()

        # Use defaults from settings if not specified
        start_date = start_date or settings.FROM_DATE
        end_date = end_date or (settings.TO_DATE or datetime.now().strftime("%Y-%m-%d"))

        logger.info(f"Starting historical data fetch operation {operation_id}")
        logger.info(f"Date range: {start_date} to {end_date}")

        # Step 1: Select securities to process
        securities = self._select_securities(security_ids, exchanges, segments)

        if not securities:
            msg = "No securities found matching the specified criteria"
            logger.warning(msg)
            return {"operation_id": operation_id, "status": "error", "message": msg, "stats": {"securities_processed": 0, "total_records": 0}}

        logger.info(f"Selected {len(securities)} securities for processing")

        # Pre-populate security type cache for better performance
        self._preload_security_cache(securities)

        # Step 2: Create batches for parallel processing
        batches = self._create_batches(securities, batch_size)
        logger.info(f"Created {len(batches)} batches for processing")

        # Step 3: Process batches in parallel
        results = self._process_historical_batches(batches, start_date, end_date, workers, verbose)

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

    def fetch_current_day_data(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None, is_eod: bool = False, workers: int = 8, batch_size: int = 400, verbose: bool = False) -> Dict[str, Any]:
        """Fetch current day OHLCV data for specified securities."""
        operation_id = str(uuid.uuid4())
        start_time = time.time()

        logger.info(f"Starting current day data fetch operation {operation_id}")
        logger.info(f"Mode: {'EOD' if is_eod else 'Regular'}")

        # Step 1: Select securities to process
        securities = self._select_securities(security_ids, exchanges, segments)

        if not securities:
            msg = "No securities found matching the specified criteria"
            logger.warning(msg)
            return {"operation_id": operation_id, "status": "error", "message": msg, "stats": {"securities_processed": 0}}

        logger.info(f"Selected {len(securities)} securities for current day data")

        # Step 2: Group securities by exchange segment for efficient API calls
        securities_by_segment = self._group_securities_by_segment(securities)

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

    def update_all_data(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None, days_back: int = 7, include_today: bool = True, workers: int = 12, batch_size: int = 100, verbose: bool = False, full_history: bool = False) -> Dict[str, Any]:
        """Combined workflow to update both historical and current day data."""
        operation_id = str(uuid.uuid4())
        start_time = time.time()

        logger.info(f"Starting comprehensive data update operation {operation_id}")
        logger.info(f"Checking {days_back} days back, include_today={include_today}, full_history={full_history}")

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
            current_result = self.fetch_current_day_data(security_ids=security_ids, exchanges=exchanges, segments=segments, is_eod=True, workers=min(workers, 6), batch_size=batch_size * 2, verbose=verbose)  # Use fewer workers for current data  # Larger batches for current data

        # Calculate duration
        duration_seconds = time.time() - start_time

        # Prepare result
        operation_result = {"operation_id": operation_id, "status": "completed", "duration_seconds": duration_seconds, "historical": historical_result, "current": current_result, "days_back": days_back, "include_today": include_today, "full_history": full_history}

        logger.info(f"Completed comprehensive data update operation {operation_id} in {duration_seconds:.2f} seconds")

        return operation_result

    def _select_securities(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None) -> List[Security]:
        """Select securities based on provided criteria with eager loading."""
        logger.info("Selecting securities for processing")

        with get_db() as db:
            # Use joinedload to eagerly load relationships
            query = db.query(Security).options(joinedload(Security.exchange), joinedload(Security.futures)).filter(Security.is_active == True)

            # Apply filters...
            if security_ids:
                query = query.filter(Security.id.in_(security_ids))

            if exchanges:
                query = query.join(Security.exchange).filter(Exchange.code.in_([code.upper() for code in exchanges]))

            if segments:
                query = query.filter(Security.segment.in_(segments))

            query = query.order_by(Security.symbol)
            securities = query.all()

            # Create copies with relevant attributes to detach from session
            detached_securities = []
            for security in securities:
                # Pre-cache security type
                self._security_type_cache[str(security.id)] = security.security_type

                # Pre-cache futures relationship if available
                if security.futures:
                    self._futures_cache[str(security.id)] = {"underlying_id": str(security.futures.underlying_id), "expiration_date": security.futures.expiration_date}

                detached_securities.append(security)

            db.expunge_all()

            logger.info(f"Selected {len(detached_securities)} securities")
            return detached_securities

    def _preload_security_cache(self, securities: List[Security]):
        """Preload security metadata into caches for better performance."""
        # This method is called after select_securities and before processing batches
        # to ensure we have all metadata needed without making individual DB calls later

        # Update existing cache with security types
        for security in securities:
            self._security_type_cache[str(security.id)] = security.security_type

        logger.info(f"Preloaded metadata for {len(securities)} securities")

    def _clear_caches(self):
        """Clear internal caches to free memory."""
        self._security_type_cache.clear()
        self._futures_cache.clear()

    def _create_batches(self, securities: List[Security], batch_size: int) -> List[List[Security]]:
        """Split securities into processing batches."""
        # Performance optimization: Group securities by type in batches
        # This creates more focused batches that can be processed similarly
        stocks = [s for s in securities if getattr(s, "security_type", None) == "STOCK"]
        indices = [s for s in securities if getattr(s, "security_type", None) == "INDEX"]
        derivatives = [s for s in securities if getattr(s, "security_type", None) == "DERIVATIVE"]
        others = [s for s in securities if getattr(s, "security_type", None) not in ("STOCK", "INDEX", "DERIVATIVE")]

        # Create batches for each type
        stock_batches = [stocks[i : i + batch_size] for i in range(0, len(stocks), batch_size)]
        index_batches = [indices[i : i + batch_size] for i in range(0, len(indices), batch_size)]
        derivative_batches = [derivatives[i : i + batch_size] for i in range(0, len(derivatives), batch_size)]
        other_batches = [others[i : i + batch_size] for i in range(0, len(others), batch_size)]

        # Combine all batches
        all_batches = stock_batches + index_batches + derivative_batches + other_batches

        logger.info(f"Created {len(all_batches)} batches ({len(stock_batches)} stock, {len(index_batches)} index, {len(derivative_batches)} derivative)")

        return all_batches

    def _group_securities_by_segment(self, securities: List[Security]) -> Dict[str, List[str]]:
        """Group securities by exchange segment for efficient API calls."""
        result = {}

        # Create a session for consistent mapping
        with get_db() as db:
            for security in securities:
                try:
                    # Get Dhan's exchange segment with session
                    exchange_segment = self.mapper.map_exchange_segment(security, db)

                    # Get Dhan's security ID
                    security_id = str(security.external_id)

                    # Add to the appropriate group
                    if exchange_segment not in result:
                        result[exchange_segment] = []

                    result[exchange_segment].append(security_id)
                except Exception as e:
                    logger.error(f"Error grouping security {security.symbol}: {str(e)}")

        return result

    def _process_historical_batches(self, batches: List[List[Security]], start_date: str, end_date: str, workers: int, verbose: bool) -> List[Dict[str, Any]]:
        """Process batches of securities for historical data in parallel."""
        results = []

        # Performance optimization: Use bounded semaphore to limit concurrent API calls
        # This avoids overwhelming the API while still maintaining good throughput
        from concurrent.futures import ThreadPoolExecutor
        from threading import BoundedSemaphore

        # Limit concurrent API calls to avoid rate limits while maintaining throughput
        api_semaphore = BoundedSemaphore(min(8, workers))

        def process_with_semaphore(batch, batch_idx):
            with api_semaphore:
                return self._process_historical_batch(batch, start_date, end_date, verbose, batch_idx)

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all batches for processing
            futures = [executor.submit(process_with_semaphore, batch, batch_idx) for batch_idx, batch in enumerate(batches)]

            # Collect results as they complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    batch_result = future.result()
                    results.append(batch_result)

                    # Log progress
                    completed = len(results)
                    if completed % max(1, len(batches) // 10) == 0 or completed == len(batches):
                        logger.info(f"Processed {completed}/{len(batches)} batches ({completed*100/len(batches):.1f}%)")

                except Exception as e:
                    logger.error(f"Error processing batch: {str(e)}")
                    results.append({"status": "error", "error": str(e)})

        return results

    def _fetch_derivative_data(self, security_id: str, external_id: str, symbol: str, params: Dict[str, Any], verbose: bool = False) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Special method to fetch derivative data with multiple fallback strategies."""
        today = datetime.now().date()

        # Strategy 1: Try with current month and 30-day window
        thirty_days_ago = (today - timedelta(days=30)).strftime("%Y-%m-%d")

        test_params = params.copy()
        test_params["fromDate"] = thirty_days_ago
        test_params["toDate"] = today.strftime("%Y-%m-%d")
        test_params["expiryCode"] = 0  # Current expiry

        if verbose:
            logger.debug(f"Strategy 1: Trying derivative {symbol} with 30-day window and expiryCode=0")

        response = self.api_client.fetch_historical_data(**test_params)
        if response.get("status") == "success" and response.get("data"):
            if verbose:
                logger.debug(f"Strategy 1 succeeded for {symbol}")
            return response, test_params

        # Strategy 2: Try with next month expiry
        test_params["expiryCode"] = 1  # Next expiry

        if verbose:
            logger.debug(f"Strategy 2: Trying derivative {symbol} with 30-day window and expiryCode=1")

        response = self.api_client.fetch_historical_data(**test_params)
        if response.get("status") == "success" and response.get("data"):
            if verbose:
                logger.debug(f"Strategy 2 succeeded for {symbol}")
            return response, test_params

        # Strategy 3: Try with shorter window (7 days)
        seven_days_ago = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        test_params["fromDate"] = seven_days_ago
        test_params["expiryCode"] = 0  # Back to current expiry

        if verbose:
            logger.debug(f"Strategy 3: Trying derivative {symbol} with 7-day window and expiryCode=0")

        response = self.api_client.fetch_historical_data(**test_params)
        if response.get("status") == "success" and response.get("data"):
            if verbose:
                logger.debug(f"Strategy 3 succeeded for {symbol}")
            return response, test_params

        # Strategy 4: Try with far month expiry
        test_params["expiryCode"] = 2  # Far month expiry

        if verbose:
            logger.debug(f"Strategy 4: Trying derivative {symbol} with 7-day window and expiryCode=2")

        response = self.api_client.fetch_historical_data(**test_params)
        if response.get("status") == "success" and response.get("data"):
            if verbose:
                logger.debug(f"Strategy 4 succeeded for {symbol}")
            return response, test_params

        # Strategy 5: Progressive fetching in small chunks
        all_data = []
        base_params = params.copy()
        base_params["expiryCode"] = 0  # Start with current expiry

        if verbose:
            logger.debug(f"Strategy 5: Progressive fetching for {symbol} in small chunks")

        # Try fetching data in 3-day chunks, going back 30 days maximum
        for i in range(0, 30, 3):
            end_date = today - timedelta(days=i)
            start_date = end_date - timedelta(days=3)

            chunk_params = base_params.copy()
            chunk_params["fromDate"] = start_date.strftime("%Y-%m-%d")
            chunk_params["toDate"] = end_date.strftime("%Y-%m-%d")

            if verbose:
                logger.debug(f"Fetching chunk {i//3+1} for {symbol}: {chunk_params['fromDate']} to {chunk_params['toDate']}")

            chunk_response = self.api_client.fetch_historical_data(**chunk_params)
            if chunk_response.get("status") == "success" and chunk_response.get("data"):
                all_data.extend(chunk_response.get("data", []))
                if verbose:
                    logger.debug(f"Chunk {i//3+1} fetched {len(chunk_response.get('data', []))} records")

            # Stop if we've collected enough data or hit too many errors
            if len(all_data) > 30 or i >= 15:
                break

        # If progressive fetching found any data, return it
        if all_data:
            if verbose:
                logger.debug(f"Strategy 5 succeeded for {symbol} with {len(all_data)} records")
            combined_response = {"status": "success", "data": all_data}
            return combined_response, chunk_params

        # All strategies failed
        if verbose:
            logger.warning(f"All strategies failed for derivative {symbol}")

        # Return the last attempted response
        if not response:
            response = {"status": "error", "error": "All fetching strategies failed"}
        return response, test_params

    def _process_historical_batch(self, batch: List[Security], start_date: str, end_date: str, verbose: bool, batch_idx: int) -> Dict[str, Any]:
        """Process a single batch of securities with session open throughout."""
        batch_results = {"batch_idx": batch_idx, "status": "completed", "securities_processed": len(batch), "securities_success": 0, "securities_error": 0, "securities_skipped": 0, "total_records": 0, "security_results": {}, "errors": {}}

        # Log the original date range for debugging
        if verbose:
            logger.debug(f"Batch {batch_idx} using date range: {start_date} to {end_date}")

        # Create a single session for the entire batch to improve performance
        with get_db() as db:
            if verbose:
                logger.info(f"Processing batch {batch_idx} with {len(batch)} securities")

            # Performance optimization: Check if all securities in this batch are of same type
            # This allows us to optimize some processing logic
            batch_types = set(getattr(security, "security_type", None) for security in batch)
            is_homogeneous_batch = len(batch_types) == 1
            batch_type = next(iter(batch_types)) if is_homogeneous_batch else None

            if verbose and is_homogeneous_batch:
                logger.debug(f"Homogeneous batch of type: {batch_type}")

            for security in batch:
                try:
                    # Initialize response variable
                    response = None

                    # Get security type from cache if possible, otherwise from object
                    security_type = self._security_type_cache.get(str(security.id), security.security_type)

                    # Process differently based on security type
                    if security_type == "INDEX":
                        # For indices, use standard parameters and date range
                        params = {"securityId": str(security.external_id), "exchangeSegment": "IDX_I", "instrument": "INDEX", "fromDate": start_date, "toDate": end_date, "expiryCode": 0, "oi": False}  # Use original date range

                        if verbose:
                            logger.debug(f"INDEX params for {security.symbol}: {params}")

                        response = self.api_client.fetch_historical_data(**params)

                    elif security_type == "STOCK":
                        # For stocks, always use full date range
                        params = {"securityId": str(security.external_id), "exchangeSegment": "NSE_EQ", "instrument": "EQUITY", "fromDate": start_date, "toDate": end_date, "expiryCode": 0, "oi": False}  # Use original date range

                        if verbose:
                            logger.debug(f"STOCK params for {security.symbol}: {params}")

                        response = self.api_client.fetch_historical_data(**params)

                    elif security_type == "DERIVATIVE":
                        # For derivatives, use special adaptive strategy
                        # First, prepare base parameters
                        params = {"securityId": str(security.external_id), "exchangeSegment": "NSE_FNO", "oi": True, "expiryCode": 0}

                        # Check if we have futures data in cache
                        futures_info = self._futures_cache.get(str(security.id))

                        # If not in cache, query from database
                        if not futures_info:
                            futures_contract = db.query(Future).filter(Future.security_id == security.id).first()
                        else:
                            futures_contract = True  # Just a flag to know we have futures info

                        if futures_contract:
                            # Determine if it's index future or stock future
                            # If using cached data
                            if futures_info:
                                underlying_id = futures_info.get("underlying_id")
                                underlying_type = self._security_type_cache.get(underlying_id)

                                # If not in cache, query from DB
                                if not underlying_type and underlying_id:
                                    underlying = db.query(Security).filter(Security.id == underlying_id).first()
                                    underlying_type = underlying.security_type if underlying else None

                                expiration_date = futures_info.get("expiration_date")
                            else:
                                # Get from freshly queried futures_contract
                                underlying = db.query(Security).filter(Security.id == futures_contract.underlying_id).first()
                                underlying_type = underlying.security_type if underlying else None
                                expiration_date = futures_contract.expiration_date

                            # Set instrument type
                            if underlying_type == "INDEX":
                                params["instrument"] = "FUTIDX"
                            else:
                                params["instrument"] = "FUTSTK"

                            # Skip if contract has already expired
                            today = datetime.now().date()
                            if expiration_date and expiration_date < today:
                                if verbose:
                                    logger.info(f"Skipping expired futures contract {security.symbol}, expiry: {expiration_date}")
                                batch_results["securities_skipped"] += 1
                                continue

                            # Use our special fetching method with multiple strategies for futures
                            if verbose:
                                logger.debug(f"Using special futures strategy for {security.symbol}")

                            response, used_params = self._fetch_derivative_data(str(security.id), str(security.external_id), security.symbol, params, verbose)
                        else:
                            # Not a futures contract or missing relationship
                            params["instrument"] = "FUTSTK"  # Default

                            # Use conservative dates for derivatives without futures info
                            today = datetime.now().date()
                            thirty_days_ago = (today - timedelta(days=30)).strftime("%Y-%m-%d")
                            params["fromDate"] = thirty_days_ago
                            params["toDate"] = end_date

                            if verbose:
                                logger.debug(f"Using 30-day window for non-futures derivative {security.symbol}")

                            response = self.api_client.fetch_historical_data(**params)
                    else:
                        # Other security types - use mapper
                        if verbose:
                            logger.debug(f"Using mapper for unknown type {security.symbol} ({security_type})")

                        # Get params from mapper but ensure we use our date range
                        params = self.mapper.get_dhan_request_params(security, db)
                        params["fromDate"] = start_date
                        params["toDate"] = end_date

                        response = self.api_client.fetch_historical_data(**params)

                    # Check if we have valid data
                    if response and response.get("status") == "success" and response.get("data"):
                        # Store data in the repository
                        records = response.get("data", [])
                        result = self.repository.upsert_daily_data(str(security.id), records, source="dhan_historical_api")

                        # Update batch results
                        batch_results["securities_success"] += 1
                        batch_results["total_records"] += len(records)
                        batch_results["security_results"][str(security.id)] = {"symbol": security.symbol, "records": len(records), "inserted": result[0], "updated": result[1]}

                        if verbose:
                            logger.debug(f"Processed {security.symbol}: {len(records)} records")
                    else:
                        # Handle empty or error response
                        error_msg = "No response received" if response is None else response.get("error", "No data returned or API error")
                        batch_results["securities_error"] += 1
                        batch_results["errors"][str(security.id)] = {"symbol": security.symbol, "error": error_msg, "response_status": None if response is None else response.get("status")}

                        if verbose:
                            logger.warning(f"No data returned for {security.symbol}: {error_msg}")

                except Exception as e:
                    # Handle any exceptions during processing
                    batch_results["securities_error"] += 1
                    batch_results["errors"][str(security.id)] = {"symbol": security.symbol, "error": str(e)}

                    logger.error(f"Error processing {security.symbol}: {str(e)}")

            if verbose:
                logger.info(f"Completed batch {batch_idx}: {batch_results['securities_success']} success, {batch_results['securities_error']} error, {batch_results['securities_skipped']} skipped")

        return batch_results

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

                    # Process in batches of 1000 (API limit)
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

                # Query in batches of 500 to avoid potential database limits
                batch_size = 500
                for i in range(0, len(int_external_ids), batch_size):
                    batch = int_external_ids[i : i + batch_size]
                    securities = session.query(Security.id, Security.external_id).filter(Security.external_id.in_(batch)).all()

                    # Update mapping
                    for security_id, external_id in securities:
                        mapping[str(external_id)] = str(security_id)
            except Exception as e:
                logger.error(f"Error in bulk security ID mapping: {e}")
                # Fallback to individual queries if bulk query fails
                for ext_id in external_ids:
                    try:
                        security = session.query(Security.id).filter(Security.external_id == int(ext_id)).first()
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
