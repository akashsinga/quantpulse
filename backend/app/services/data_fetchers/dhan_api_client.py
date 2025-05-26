# app/services/data_fetchers/dhan_api_client.py

import json
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import requests
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DhanAPIError(Exception):
    """Custom exception for Dhan API errors"""
    pass


class RateLimitError(DhanAPIError):
    """Exception for rate limit errors"""
    pass


class DhanAPIClient:
    """
    Dhan API client for fetching OHLCV data
    Note: Internal rate limiting DISABLED - use DistributedRateLimitedDhanClient instead
    """

    def __init__(self):
        self.access_token = settings.DHAN_ACCESS_TOKEN
        self.client_id = settings.DHAN_CLIENT_ID
        self.historical_url = settings.DHAN_CHARTS_HISTORICAL_URL
        self.today_eod_url = settings.DHAN_TODAY_EOD_URL

        if not self.access_token or not self.client_id:
            raise ValueError("DHAN_ACCESS_TOKEN and DHAN_CLIENT_ID must be set in environment")

        self.session = requests.Session()
        self.session.headers.update({'Accept': 'application/json', 'Content-Type': 'application/json', 'access-token': self.access_token, 'client-id': self.client_id})

        logger.info(f"Initialized Dhan API client (rate limiting handled externally)")

    def _handle_response(self, response: requests.Response, request_type: str) -> Dict[str, Any]:
        """Handle API response and raise appropriate exceptions"""
        try:
            response.raise_for_status()
            data = response.json()

            # Check for Dhan API specific errors
            if data.get('status') == 'error':
                error_code = data.get('errorCode', 'Unknown')
                error_message = data.get('errorMessage', 'Unknown error')

                # Handle rate limiting
                if error_code in ['DH-904', '805']:
                    logger.warning(f"Rate limit response from API for {request_type}: {error_message}")
                    raise RateLimitError(f"API rate limit: {error_message}")

                # Handle authentication errors
                if error_code in ['DH-901', 'DH-808', 'DH-809']:
                    logger.error(f"Authentication error for {request_type}: {error_message}")
                    raise DhanAPIError(f"Authentication failed: {error_message}")

                # Handle other API errors
                raise DhanAPIError(f"API Error {error_code}: {error_message}")

            return data

        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                raise RateLimitError(f"HTTP 429 Rate limit exceeded")
            elif response.status_code in [401, 403]:
                raise DhanAPIError(f"Authentication error: {e}")
            else:
                # Log the response content for debugging
                logger.error(f"HTTP {response.status_code} error for {request_type}. Response: {response.text[:500]}")
                raise DhanAPIError(f"HTTP error {response.status_code}: {e}")

        except requests.exceptions.RequestException as e:
            raise DhanAPIError(f"Request failed: {e}")

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response for {request_type}. Response: {response.text[:500]}")
            raise DhanAPIError(f"Invalid JSON response: {e}")

    def fetch_historical_data(self, security_id: str, exchange_segment: str, instrument: str, from_date: str = "2000-01-01", to_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch historical OHLCV data for a single security
        
        WARNING: This method has NO internal rate limiting.
        Use DistributedRateLimitedDhanClient for proper coordination.
        
        Args:
            security_id: Dhan security ID
            exchange_segment: NSE_EQ, IDX_I, etc.
            instrument: EQUITY, INDEX, etc.
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD), defaults to today
            
        Returns:
            Dict containing OHLCV arrays and timestamps
        """
        if to_date is None:
            to_date = date.today().strftime("%Y-%m-%d")

        payload = {"securityId": security_id, "exchangeSegment": exchange_segment, "instrument": instrument, "expiryCode": 0, "oi": False, "fromDate": from_date, "toDate": to_date}

        logger.debug(f"Fetching historical data: {security_id} ({exchange_segment}) from {from_date} to {to_date}")

        try:
            response = self.session.post(self.historical_url, json=payload, timeout=30)
            data = self._handle_response(response, f"Historical-{security_id}")

            # Validate response structure
            required_fields = ['open', 'high', 'low', 'close', 'volume', 'timestamp']
            missing_fields = [field for field in required_fields if field not in data]

            if missing_fields:
                raise DhanAPIError(f"Missing fields in response: {missing_fields}")

            # Validate data arrays have same length
            lengths = [len(data[field]) for field in required_fields]
            if len(set(lengths)) > 1:
                raise DhanAPIError(f"Inconsistent array lengths in response: {dict(zip(required_fields, lengths))}")

            logger.info(f"Successfully fetched {len(data['timestamp'])} historical records for {security_id}")
            return data

        except Exception as e:
            logger.error(f"Failed to fetch historical data for {security_id}: {e}")
            raise

    def fetch_today_eod_data(self, securities_by_segment: Dict[str, List[str]]) -> Dict[str, Any]:
        """
        Fetch today's EOD data for multiple securities using Dhan's quote API
        
        FIXED: Implements batch processing to handle ALL securities (not just first 1000)
        The quote API provides OHLC + volume data which we need for OHLCV
        
        Args:
            securities_by_segment: Dict mapping exchange segments to security ID lists (as strings)
            Example: {"NSE_EQ": ["1333", "11536"], "IDX_I": ["13", "25"]}
            
        Returns:
            Dict containing OHLC + volume data organized by segment and security
        """
        total_securities = sum(len(ids) for ids in securities_by_segment.values())
        logger.info(f"Fetching today EOD quote data for {total_securities} securities using batch processing")

        try:
            # FIXED: Convert string security IDs to integers as required by Dhan API
            int_securities_by_segment = {}
            for segment, security_ids in securities_by_segment.items():
                try:
                    int_securities_by_segment[segment] = [int(sid) for sid in security_ids]
                except (ValueError, TypeError) as e:
                    logger.error(f"Error converting security IDs to integers for segment {segment}: {e}")
                    continue

            # FIXED: Implement batch processing to handle ALL securities
            batch_size = 1000  # Dhan API limit
            combined_data = {'data': {}}

            if total_securities <= batch_size:
                # Single batch - process all at once
                logger.debug(f"Single batch processing for {total_securities} securities")
                return self._fetch_single_batch(int_securities_by_segment)

            # Multiple batches needed
            batches = self._create_batches(int_securities_by_segment, batch_size)
            logger.info(f"Processing {total_securities} securities in {len(batches)} batches of max {batch_size} each")

            total_received = 0
            for batch_idx, batch_payload in enumerate(batches):
                try:
                    logger.info(f"Processing batch {batch_idx + 1}/{len(batches)} with {sum(len(ids) for ids in batch_payload.values())} securities")

                    batch_data = self._fetch_single_batch(batch_payload)

                    # Merge batch data into combined result
                    for segment, securities in batch_data.get('data', {}).items():
                        if segment not in combined_data['data']:
                            combined_data['data'][segment] = {}
                        combined_data['data'][segment].update(securities)
                        total_received += len(securities)

                    # Add delay between batches to respect rate limits
                    if batch_idx < len(batches) - 1:  # Don't delay after last batch
                        time.sleep(0.2)  # 200ms delay between batches

                except Exception as e:
                    logger.error(f"Error processing batch {batch_idx + 1}: {e}")
                    # Continue with next batch instead of failing completely
                    continue

            # Add status to combined response
            combined_data['status'] = 'success'

            logger.info(f"Successfully fetched quote data for {total_received} securities across {len(batches)} batches")
            return combined_data

        except Exception as e:
            logger.error(f"Failed to fetch today EOD quote data: {e}")
            raise

    def _fetch_single_batch(self, securities_by_segment: Dict[str, List[int]]) -> Dict[str, Any]:
        """
        Fetch a single batch of securities from the quote API
        
        Args:
            securities_by_segment: Dict with integer security IDs
            
        Returns:
            API response data
        """
        batch_size = sum(len(ids) for ids in securities_by_segment.values())
        logger.debug(f"Fetching single batch: {batch_size} securities")

        response = self.session.post(self.today_eod_url, json=securities_by_segment, timeout=30)
        data = self._handle_response(response, f"TodayEOD-Batch-{batch_size}")

        # Validate response structure
        if 'data' not in data:
            raise DhanAPIError("Missing 'data' field in quote API response")

        return data

    def _create_batches(self, securities_by_segment: Dict[str, List[int]], batch_size: int) -> List[Dict[str, List[int]]]:
        """
        Create batches of securities respecting the API limit
        
        Args:
            securities_by_segment: All securities organized by segment
            batch_size: Maximum securities per batch
            
        Returns:
            List of batch payloads
        """
        batches = []
        current_batch = {}
        current_count = 0

        # Flatten all securities with their segments
        all_securities = []
        for segment, security_ids in securities_by_segment.items():
            for security_id in security_ids:
                all_securities.append((segment, security_id))

        # Create batches
        for segment, security_id in all_securities:
            if current_count >= batch_size:
                # Start new batch
                batches.append(current_batch)
                current_batch = {}
                current_count = 0

            if segment not in current_batch:
                current_batch[segment] = []

            current_batch[segment].append(security_id)
            current_count += 1

        # Add final batch if it has data
        if current_batch:
            batches.append(current_batch)

        return batches

    def test_connection(self) -> bool:
        """
        Test API connection and authentication
        
        WARNING: This method has NO internal rate limiting.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Test with a simple request for a known security
            test_payload = {
                "securityId": "1333",  # Reliance Industries
                "exchangeSegment": "NSE_EQ",
                "instrument": "EQUITY",
                "expiryCode": 0,
                "oi": False,
                "fromDate": "2024-01-01",
                "toDate": "2024-01-02"
            }
            response = self.session.post(self.historical_url, json=test_payload, timeout=10)
            self._handle_response(response, "ConnectionTest")
            logger.info("Dhan API connection test successful")
            return True

        except Exception as e:
            logger.error(f"Dhan API connection test failed: {e}")
            return False

    def get_rate_limit_info(self) -> Dict[str, Any]:
        """
        Get rate limit information - now returns warning
        
        Returns:
            Dict with warning about external rate limiting
        """
        return {"warning": "Internal rate limiting disabled", "message": "Use DistributedRateLimitedDhanClient for proper rate limiting", "requests_per_second": "Handled externally", "daily_limit": "Handled externally", "current_usage": "Unknown"}
