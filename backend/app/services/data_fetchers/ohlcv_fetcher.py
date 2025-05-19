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
    """Main coordinator for OHLCV data fetching operations."""

    def __init__(self, api_client: Optional[DhanAPIClient] = None, mapper: Optional[ExchangeMapper] = None, repository: Optional[OHLCVRepository] = None):
        """Initialize the OHLCV fetcher with its dependencies.

        Args:
            api_client: Optional pre-configured API client
            mapper: Optional pre-configured exchange mapper
            repository: Optional pre-configured OHLCV repository
        """
        self.api_client = api_client or DhanAPIClient()
        self.mapper = mapper or ExchangeMapper()
        self.repository = repository or OHLCVRepository()

        # Operational parameters
        self.default_workers = 8
        self.default_batch_size = 50

        logger.info("Initialized OHLCV fetcher")

    def fetch_historical_data(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, workers: int = 8, batch_size: int = 50, verbose: bool = False) -> Dict[str, Any]:
        """Fetch historical OHLCV data for specified securities.

        If no security_ids are provided, process all active securities.

        Args:
            security_ids: Optional list of security UUIDs to process
            exchanges: Optional list of exchanges to filter by
            segments: Optional list of segments to filter by
            start_date: Start date for data (YYYY-MM-DD format)
            end_date: End date for data (YYYY-MM-DD format)
            workers: Number of parallel worker threads
            batch_size: Batch size for processing
            verbose: Enable verbose logging

        Returns:
            Dict with operation results and statistics
        """
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
        operation_result = {"operation_id": operation_id, "status": "completed", "duration_seconds": duration_seconds, "start_date": start_date, "end_date": end_date, "stats": stats, "securities": {"total": len(securities), "success": stats["securities_success"], "error": stats["securities_error"]}}

        logger.info(f"Completed historical data fetch operation {operation_id} in {duration_seconds:.2f} seconds")
        logger.info(f"Processed {stats['securities_processed']} securities with {stats['total_records']} records")

        return operation_result

    def fetch_current_day_data(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None, is_eod: bool = False, workers: int = 4, batch_size: int = 200, verbose: bool = False) -> Dict[str, Any]:
        """Fetch current day OHLCV data for specified securities.

        If no security_ids are provided, process all active securities.

        Args:
            security_ids: Optional list of security UUIDs to process
            exchanges: Optional list of exchanges to filter by
            segments: Optional list of segments to filter by
            is_eod: Whether this is an end-of-day run (more comprehensive)
            workers: Number of parallel worker threads
            batch_size: Batch size for processing
            verbose: Enable verbose logging

        Returns:
            Dict with operation results and statistics
        """
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

        return operation_result

    def update_all_data(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None, days_back: int = 7, include_today: bool = True, workers: int = 8, batch_size: int = 50, verbose: bool = False) -> Dict[str, Any]:
        """Combined workflow to update both historical and current day data.

        Args:
            security_ids: Optional list of security UUIDs to process
            exchanges: Optional list of exchanges to filter by
            segments: Optional list of segments to filter by
            days_back: Number of days back to check for gaps
            include_today: Whether to include today's data
            workers: Number of parallel worker threads
            batch_size: Batch size for processing
            verbose: Enable verbose logging

        Returns:
            Dict with operation results and statistics
        """
        operation_id = str(uuid.uuid4())
        start_time = time.time()

        logger.info(f"Starting comprehensive data update operation {operation_id}")
        logger.info(f"Checking {days_back} days back, include_today={include_today}")

        # Calculate date range for historical data
        end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=days_back)

        historical_result = None
        current_result = None

        # Step 1: Fetch historical data
        if days_back > 0:
            historical_result = self.fetch_historical_data(security_ids=security_ids, exchanges=exchanges, segments=segments, start_date=start_date.strftime("%Y-%m-%d"), end_date=end_date.strftime("%Y-%m-%d"), workers=workers, batch_size=batch_size, verbose=verbose)

        # Step 2: Fetch today's data if requested
        if include_today:
            current_result = self.fetch_current_day_data(security_ids=security_ids, exchanges=exchanges, segments=segments, is_eod=True, workers=min(workers, 4), batch_size=batch_size * 2, verbose=verbose)  # More comprehensive for update_all  # Use fewer workers for current data  # Larger batches for current data

        # Calculate duration
        duration_seconds = time.time() - start_time

        # Prepare result
        operation_result = {"operation_id": operation_id, "status": "completed", "duration_seconds": duration_seconds, "historical": historical_result, "current": current_result, "days_back": days_back, "include_today": include_today}

        logger.info(f"Completed comprehensive data update operation {operation_id} in {duration_seconds:.2f} seconds")

        return operation_result

    def _select_securities(self, security_ids: Optional[List[str]] = None, exchanges: Optional[List[str]] = None, segments: Optional[List[str]] = None) -> List[Security]:
        """Select securities based on provided criteria with eager loading.

        If no criteria are provided, select all active securities.

        Args:
            security_ids: Optional list of security UUIDs
            exchanges: Optional list of exchange codes
            segments: Optional list of segment types

        Returns:
            List of Security objects
        """
        logger.info("Selecting securities for processing")

        with get_db() as db:
            # Use joinedload to eagerly load the exchange relationship
            query = db.query(Security).options(joinedload(Security.exchange)).filter(Security.is_active == True)

            # Apply security_ids filter if provided
            if security_ids:
                query = query.filter(Security.id.in_(security_ids))

            # Apply exchanges filter if provided
            if exchanges:
                query = query.join(Security.exchange).filter(Exchange.code.in_([code.upper() for code in exchanges]))

            # Apply segments filter if provided
            if segments:
                query = query.filter(Security.segment.in_(segments))

            # Order by priority (in a real impl, would use trading volume or importance)
            query = query.order_by(Security.symbol)

            # Execute query
            securities = query.all()

            # Create a copy of all relevant attributes to detach from session
            detached_securities = []
            for security in securities:
                # Ensure exchange is accessed to load the relationship
                if security.exchange:
                    # Access to load
                    exchange_code = security.exchange.code

                # Add to detached list
                detached_securities.append(security)

            # Expunge all objects from session
            db.expunge_all()

            logger.info(f"Selected {len(detached_securities)} securities")
            return detached_securities

    def _create_batches(self, securities: List[Security], batch_size: int) -> List[List[Security]]:
        """Split securities into processing batches.

        Args:
            securities: List of securities to process
            batch_size: Size of each batch

        Returns:
            List of security batches
        """
        return [securities[i : i + batch_size] for i in range(0, len(securities), batch_size)]

    def _group_securities_by_segment(self, securities: List[Security]) -> Dict[str, List[str]]:
        """Group securities by exchange segment for efficient API calls.

        Args:
            securities: List of securities to process

        Returns:
            Dict mapping exchange segments to lists of security IDs
        """
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
        """Process batches of securities for historical data in parallel.

        Args:
            batches: List of security batches
            start_date: Start date string
            end_date: End date string
            workers: Number of worker threads
            verbose: Enable verbose logging

        Returns:
            List of batch processing results
        """
        results = []

        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all batches for processing
            futures = [executor.submit(self._process_historical_batch, batch, start_date, end_date, verbose, batch_idx) for batch_idx, batch in enumerate(batches)]

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

    def _process_historical_batch(self, batch: List[Security], start_date: str, end_date: str, verbose: bool, batch_idx: int) -> Dict[str, Any]:
        """Process a single batch of securities with session open throughout.

        Args:
            batch: List of securities in the batch
            start_date: Start date string
            end_date: End date string
            verbose: Enable verbose logging
            batch_idx: Batch index for tracking

        Returns:
            Dict with batch processing results
        """
        batch_results = {"batch_idx": batch_idx, "status": "completed", "securities_processed": len(batch), "securities_success": 0, "securities_error": 0, "total_records": 0, "security_results": {}, "errors": {}}

        # Create a session for the entire batch
        with get_db() as db:
            if verbose:
                logger.info(f"Processing batch {batch_idx} with {len(batch)} securities")

            for security in batch:
                try:
                    # Get Dhan API parameters for this security, passing the session
                    params = self.mapper.get_dhan_request_params(security, db)

                    # Add date range to parameters
                    params["fromDate"] = start_date
                    params["toDate"] = end_date

                    if verbose:
                        logger.debug(f"Fetching historical data for {security.symbol} with params: {params}")

                    # Fetch historical data from API
                    response = self.api_client.fetch_historical_data(**params)

                    # Check if we have valid data
                    if response.get("status") == "success" and response.get("data"):
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
                        batch_results["securities_error"] += 1
                        batch_results["errors"][str(security.id)] = {"symbol": security.symbol, "error": "No data returned or API error", "response_status": response.get("status")}

                        if verbose:
                            logger.warning(f"No data returned for {security.symbol}")

                except Exception as e:
                    # Handle any exceptions during processing
                    batch_results["securities_error"] += 1
                    batch_results["errors"][str(security.id)] = {"symbol": security.symbol, "error": str(e)}

                    logger.error(f"Error processing {security.symbol}: {str(e)}")

        if verbose:
            logger.info(f"Completed batch {batch_idx}: {batch_results['securities_success']} success, {batch_results['securities_error']} error")

        return batch_results

    def _process_current_data(self, securities_by_segment: Dict[str, List[str]], is_eod: bool, verbose: bool) -> Dict[str, Any]:
        """Process current day data for securities grouped by segment.

        Args:
            securities_by_segment: Dict mapping exchange segments to security IDs
            is_eod: Whether this is an end-of-day run
            verbose: Enable verbose logging

        Returns:
            Dict with processing results
        """
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
        """Get mapping from external security IDs to internal UUIDs.

        Args:
            external_ids: List of external security IDs
            session: Optional SQLAlchemy session

        Returns:
            Dict mapping external IDs to internal UUIDs
        """
        mapping = {}

        # Use provided session or create a new one
        should_close_session = False
        if session is None:
            session = SessionLocal()
            should_close_session = True

        try:
            # Query securities by external IDs
            securities = session.query(Security).filter(Security.external_id.in_([int(id) for id in external_ids])).all()

            # Create mapping
            for security in securities:
                mapping[str(security.external_id)] = str(security.id)

            return mapping

        finally:
            # Only close if we created it
            if should_close_session:
                session.close()

    def _validate_current_data_record(self, record: Dict[str, Any]) -> bool:
        """Validate a current day data record.

        Args:
            record: OHLCV data record

        Returns:
            True if record is valid, False otherwise
        """
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
        """Compile statistics from batch processing results.

        Args:
            batch_results: List of batch processing results

        Returns:
            Dict with compiled statistics
        """
        stats = {"batches_total": len(batch_results), "batches_completed": 0, "batches_error": 0, "securities_processed": 0, "securities_success": 0, "securities_error": 0, "total_records": 0}

        for result in batch_results:
            if result.get("status") == "completed":
                stats["batches_completed"] += 1
            else:
                stats["batches_error"] += 1

            stats["securities_processed"] += result.get("securities_processed", 0)
            stats["securities_success"] += result.get("securities_success", 0)
            stats["securities_error"] += result.get("securities_error", 0)
            stats["total_records"] += result.get("total_records", 0)

        return stats
