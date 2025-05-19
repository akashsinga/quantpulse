# app/services/data_fetchers/dhan_api_client.py

import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class DhanAPIClient:
    """Client for interacting with Dhan API endpoints."""

    def __init__(self):
        """Initialize the Dhan API client with proper configuration."""
        # Parse headers from settings
        self.headers = json.loads(settings.DHAN_API_HEADERS)

        # API endpoints
        self.historical_url = settings.DHAN_CHARTS_HISTORICAL_URL
        self.quote_url = settings.DHAN_TODAY_EOD_URL

        # Rate limiting configuration
        self.rate_limit_sleep = settings.RATE_LIMIT_SLEEP
        self.last_request_time = 0

        # Retry configuration
        self.max_retries = settings.API_MAX_RETRIES
        self.backoff_factor = settings.RETRY_BACKOFF_FACTOR
        self.initial_wait = settings.RETRY_INITIAL_WAIT

        logger.info(f"Initialized Dhan API client with endpoints: {self.historical_url}, {self.quote_url}")

    def _respect_rate_limit(self):
        """Implement rate limiting to avoid API throttling."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time

        if elapsed < self.rate_limit_sleep:
            sleep_time = self.rate_limit_sleep - elapsed
            logger.debug(f"Rate limiting: Sleeping for {sleep_time:.3f}s")
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    @retry(retry=retry_if_exception_type((requests.exceptions.RequestException, json.JSONDecodeError)), stop=stop_after_attempt(settings.API_MAX_RETRIES), wait=wait_exponential(multiplier=settings.RETRY_INITIAL_WAIT, factor=settings.RETRY_BACKOFF_FACTOR), reraise=True)
    def _make_api_request(self, url: str, data: Dict[str, Any], method: str = "POST") -> Dict[str, Any]:
        """Make an API request with retry logic and rate limiting."""
        self._respect_rate_limit()

        logger.debug(f"Making {method} request to {url}")

        try:
            if method.upper() == "POST":
                response = requests.post(url, json=data, headers=self.headers, timeout=30)
            else:
                response = requests.get(url, params=data, headers=self.headers, timeout=30)

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse API response: {str(e)}")
            raise

    def fetch_historical_data(self, security_id: str, exchange_segment: str, instrument: str, expiry_code: int = 0, from_date: Optional[str] = None, to_date: Optional[str] = None, oi: bool = False) -> Dict[str, Any]:
        """Fetch historical OHLCV data from Dhan API.

        Args:
            security_id: Dhan's security ID
            exchange_segment: Exchange segment code
            instrument: Instrument type
            expiry_code: Expiry code for derivatives
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            oi: Whether to include open interest data

        Returns:
            Structured OHLCV data with timestamps
        """
        # Use defaults from settings if not specified
        from_date = from_date or settings.FROM_DATE
        to_date = to_date or (settings.TO_DATE or datetime.now().strftime("%Y-%m-%d"))

        request_data = {"securityId": security_id, "exchangeSegment": exchange_segment, "instrument": instrument, "expiryCode": expiry_code, "fromDate": from_date, "toDate": to_date, "oi": oi}

        logger.info(f"Fetching historical data for {security_id} from {from_date} to {to_date}")

        response = self._make_api_request(self.historical_url, request_data)

        # Parse and validate response
        return self._parse_historical_response(response)

    def fetch_current_data(self, securities_by_segment: Dict[str, List[str]]) -> Dict[str, Dict[str, Any]]:
        """Fetch current day OHLCV data for multiple securities.

        Args:
            securities_by_segment: Dict mapping exchange segments to lists of security IDs
                Example: {"NSE_EQ": ["1333", "11536"], "NSE_FNO": ["49081"]}

        Returns:
            Dict mapping security IDs to their current OHLCV data
        """
        logger.info(f"Fetching current data for {sum(len(ids) for ids in securities_by_segment.values())} securities")

        response = self._make_api_request(self.quote_url, securities_by_segment)

        # Parse and validate response
        return self._parse_quote_response(response)

    def _parse_historical_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Parse the historical data response into a structured format.

        Args:
            response: Raw API response

        Returns:
            Structured OHLCV data with standardized format
        """
        if not response or "open" not in response:
            logger.warning("Historical data response is empty or missing required fields")
            return {"data": [], "status": "empty"}

        try:
            # Extract arrays from response
            timestamps = response.get("timestamp", [])
            opens = response.get("open", [])
            highs = response.get("high", [])
            lows = response.get("low", [])
            closes = response.get("close", [])
            volumes = response.get("volume", [])
            open_interests = response.get("open_interest", [])

            # Validate arrays have matching lengths
            min_length = min(len(timestamps), len(opens), len(highs), len(lows), len(closes), len(volumes))

            # Construct structured records
            records = []
            for i in range(min_length):
                # Convert timestamp (epoch seconds) to datetime
                ts = datetime.fromtimestamp(timestamps[i], tz=settings.INDIA_TZ)

                record = {"time": ts, "open": opens[i], "high": highs[i], "low": lows[i], "close": closes[i], "volume": volumes[i]}

                # Add open interest if available
                if open_interests and len(open_interests) > i:
                    record["open_interest"] = open_interests[i]

                records.append(record)

            return {"data": records, "status": "success", "count": len(records)}

        except Exception as e:
            logger.error(f"Error parsing historical data: {str(e)}")
            return {"data": [], "status": "error", "error": str(e)}

    def _parse_quote_response(self, response: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Parse the quote/current day data response.

        Args:
            response: Raw API response

        Returns:
            Dict mapping security IDs to their OHLCV data
        """
        if not response or "data" not in response or response.get("status") != "success":
            logger.warning("Quote data response is empty or has error status")
            return {}

        result = {}
        try:
            data = response.get("data", {})

            # Process each exchange segment
            for segment, securities in data.items():
                for security_id, security_data in securities.items():
                    # Extract OHLC data
                    ohlc_data = security_data.get("ohlc", {})

                    # Create standardized record
                    record = {"security_id": security_id, "exchange_segment": segment, "last_price": security_data.get("last_price"), "open": ohlc_data.get("open"), "high": ohlc_data.get("high"), "low": ohlc_data.get("low"), "close": ohlc_data.get("close"), "volume": security_data.get("volume", 0), "timestamp": datetime.now(tz=settings.INDIA_TZ), "source": "dhan_quote_api"}

                    result[security_id] = record

            return result

        except Exception as e:
            logger.error(f"Error parsing quote data: {str(e)}")
            return {}
