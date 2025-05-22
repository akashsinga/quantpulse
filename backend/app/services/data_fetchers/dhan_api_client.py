# app/services/data_fetchers/dhan_api_client.py

import json
import time
import asyncio
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Union, Any
import aiohttp
import requests
import os
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class RateLimitException(Exception):
    """Exception raised when rate limit is hit."""

    pass


class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts based on API responses."""

    def __init__(self, initial_delay: float = 0.01):
        self.delay = initial_delay
        self.consecutive_errors = 0
        self.success_count = 0
        self.last_adjustment = time.time()
        self.min_delay = 0.005  # 5ms minimum
        self.max_delay = 2.0  # 2s maximum

    async def wait(self):
        """Wait with current delay."""
        if self.delay > 0:
            await asyncio.sleep(self.delay)

    def on_success(self):
        """Called on successful API response."""
        self.success_count += 1
        self.consecutive_errors = 0

        # Reduce delay after multiple successes
        if self.success_count >= 10 and time.time() - self.last_adjustment > 10:
            old_delay = self.delay
            self.delay = max(self.min_delay, self.delay * 0.95)
            if self.delay != old_delay:
                logger.debug(f"Rate limit improved: {old_delay*1000:.1f}ms → {self.delay*1000:.1f}ms")
                self.last_adjustment = time.time()
                self.success_count = 0

    def on_rate_limit(self):
        """Called when rate limit is hit."""
        self.consecutive_errors += 1
        old_delay = self.delay

        # Exponential backoff
        self.delay = min(self.max_delay, self.delay * (1.5 + self.consecutive_errors * 0.1))

        logger.warning(f"Rate limit hit! Delay: {old_delay*1000:.1f}ms → {self.delay*1000:.1f}ms")
        self.last_adjustment = time.time()
        self.success_count = 0

    def on_error(self):
        """Called on other errors."""
        self.consecutive_errors += 1
        if self.consecutive_errors >= 3:
            old_delay = self.delay
            self.delay = min(self.max_delay, self.delay * 1.2)
            if self.delay != old_delay:
                logger.debug(f"Error backoff: {old_delay*1000:.1f}ms → {self.delay*1000:.1f}ms")


class CircuitBreaker:
    """Circuit breaker to prevent cascade failures."""

    def __init__(self, failure_threshold: int = 10, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    def can_execute(self) -> bool:
        """Check if request can be executed."""
        if self.state == "CLOSED":
            return True

        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "HALF_OPEN"
                logger.info("Circuit breaker: OPEN → HALF_OPEN")
                return True
            return False

        return True  # HALF_OPEN

    def on_success(self):
        """Record successful request."""
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            logger.info("Circuit breaker: HALF_OPEN → CLOSED")
        self.failure_count = 0

    def on_failure(self):
        """Record failed request."""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.failure_threshold and self.state == "CLOSED":
            self.state = "OPEN"
            logger.error(f"Circuit breaker: CLOSED → OPEN (failures: {self.failure_count})")


class DhanAPIClient:
    """High-performance async Dhan API client with adaptive rate limiting."""

    def __init__(self):
        """Initialize the async Dhan API client."""

        self.api_key = os.getenv("API_KEY")
        self.client_id = os.getenv("CLIENT_ID")

        # Headers
        self.headers = {"Content-Type": "application/json", "access-token": self.api_key, "client-id": self.client_id}

        # Log warning if credentials are missing
        if not self.api_key or self.client_id:
            logger.warning("API credentials incomplete! API_KEY or CLIENT_ID environment variable is missing.")

        # API endpoints
        self.historical_url = settings.DHAN_CHARTS_HISTORICAL_URL
        self.quote_url = settings.DHAN_TODAY_EOD_URL

        # Adaptive components
        self.rate_limiter = AdaptiveRateLimiter(initial_delay=0.01)
        self.circuit_breaker = CircuitBreaker(failure_threshold=10, timeout=60)

        # Session will be created when needed
        self._session = None
        self._session_lock = asyncio.Lock()

        # Performance tracking
        self.request_count = 0
        self.error_count = 0
        self.start_time = time.time()

        logger.info("Initialized ASYNC Dhan API client with adaptive rate limiting and circuit breaker")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session with connection pooling."""
        if self._session is None or self._session.closed:
            async with self._session_lock:
                if self._session is None or self._session.closed:
                    # Create optimized connector
                    connector = aiohttp.TCPConnector(limit=100, limit_per_host=50, keepalive_timeout=30, enable_cleanup_closed=True, use_dns_cache=True, ttl_dns_cache=300)  # Total connection pool size  # Per-host limit

                    # Create session with timeout
                    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)  # Total timeout  # Connection timeout  # Socket read timeout

                    self._session = aiohttp.ClientSession(connector=connector, timeout=timeout, headers=self.headers)

                    logger.debug("Created new aiohttp session with connection pooling")

        return self._session

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("Closed aiohttp session")

    async def _make_async_request(self, url: str, data: Dict[str, Any], method: str = "POST") -> Dict[str, Any]:
        """Make async API request with adaptive rate limiting and circuit breaking."""

        # Check circuit breaker
        if not self.circuit_breaker.can_execute():
            raise Exception("Circuit breaker is OPEN - too many failures")

        # Apply rate limiting
        await self.rate_limiter.wait()

        session = await self._get_session()

        try:
            self.request_count += 1

            # Make request
            if method.upper() == "POST":
                async with session.post(url, json=data) as response:
                    return await self._process_response(response)
            else:
                async with session.get(url, params=data) as response:
                    return await self._process_response(response)

        except Exception as e:
            self.error_count += 1
            self.circuit_breaker.on_failure()
            self.rate_limiter.on_error()
            logger.debug(f"Async request failed: {str(e)}")
            raise

    async def _process_response(self, response: aiohttp.ClientResponse) -> Dict[str, Any]:
        """Process aiohttp response."""

        # Handle rate limiting
        if response.status == 429:
            self.rate_limiter.on_rate_limit()

            # Check for Retry-After header
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                wait_time = int(retry_after)
                logger.warning(f"Rate limited - waiting {wait_time}s")
                await asyncio.sleep(wait_time)

            raise RateLimitException("Rate limit exceeded")

        # Handle other HTTP errors
        if response.status >= 400:
            error_text = await response.text()
            logger.error(f"HTTP {response.status}: {error_text[:200]}")
            response.raise_for_status()

        # Success
        self.circuit_breaker.on_success()
        self.rate_limiter.on_success()

        return await response.json()

    # Sync wrapper for backwards compatibility
    def fetch_historical_data(self, securityId: str, exchangeSegment: str, instrument: str, expiryCode: int = 0, fromDate: Optional[str] = None, toDate: Optional[str] = None, oi: bool = False) -> Dict[str, Any]:
        """Sync wrapper for async historical data fetch."""
        return asyncio.run(self.fetch_historical_data_async(securityId, exchangeSegment, instrument, expiryCode, fromDate, toDate, oi))

    async def fetch_historical_data_async(self, securityId: str, exchangeSegment: str, instrument: str, expiryCode: int = 0, fromDate: Optional[str] = None, toDate: Optional[str] = None, oi: bool = False) -> Dict[str, Any]:
        """Fetch historical OHLCV data asynchronously."""

        # Use defaults from settings if not specified
        fromDate = fromDate or settings.FROM_DATE
        toDate = toDate or (settings.TO_DATE or datetime.now().strftime("%Y-%m-%d"))

        request_data = {"securityId": str(securityId), "exchangeSegment": exchangeSegment, "instrument": instrument, "expiryCode": expiryCode, "fromDate": fromDate, "toDate": toDate, "oi": oi}

        try:
            response = await self._make_async_request(self.historical_url, request_data)
            return self._parse_historical_response(response)
        except Exception as e:
            logger.debug(f"Historical data fetch failed: {str(e)}")
            return {"status": "error", "error": str(e), "data": []}

    # Sync wrapper for current data
    def fetch_current_data(self, securities_by_segment: Dict[str, List[str]]) -> Dict[str, Dict[str, Any]]:
        """Sync wrapper for async current data fetch."""
        return asyncio.run(self.fetch_current_data_async(securities_by_segment))

    async def fetch_current_data_async(self, securities_by_segment: Dict[str, List[str]]) -> Dict[str, Dict[str, Any]]:
        """Fetch current day OHLCV data asynchronously."""
        try:
            response = await self._make_async_request(self.quote_url, securities_by_segment)
            return self._parse_quote_response(response)
        except Exception as e:
            logger.debug(f"Current data fetch failed: {str(e)}")
            return {}

    def _parse_historical_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Parse historical data response."""
        if not response or "open" not in response:
            return {"data": [], "status": "empty"}

        try:
            # Extract arrays from response
            timestamps = response.get("timestamp", [])
            opens = response.get("open", [])
            highs = response.get("high", [])
            lows = response.get("low", [])
            closes = response.get("close", [])
            volumes = response.get("volume", [])

            # Validate arrays have matching lengths
            min_length = min(len(timestamps), len(opens), len(highs), len(lows), len(closes), len(volumes))

            if min_length == 0:
                return {"data": [], "status": "empty"}

            # Construct structured records
            records = []
            for i in range(min_length):
                # Convert timestamp (epoch seconds) to datetime
                ts = datetime.fromtimestamp(timestamps[i], tz=settings.INDIA_TZ)

                records.append({"time": ts, "open": opens[i], "high": highs[i], "low": lows[i], "close": closes[i], "volume": volumes[i]})

            return {"data": records, "status": "success", "count": len(records)}

        except Exception as e:
            logger.debug(f"Parse historical error: {str(e)}")
            return {"data": [], "status": "error", "error": str(e)}

    def _parse_quote_response(self, response: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Parse quote/current day data response."""
        if not response or "data" not in response or response.get("status") != "success":
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
                    result[security_id] = {"security_id": security_id, "exchange_segment": segment, "last_price": security_data.get("last_price"), "open": ohlc_data.get("open"), "high": ohlc_data.get("high"), "low": ohlc_data.get("low"), "close": ohlc_data.get("close"), "volume": security_data.get("volume", 0), "timestamp": datetime.now(tz=settings.INDIA_TZ), "source": "dhan_quote_api"}

            return result

        except Exception as e:
            logger.debug(f"Parse quote error: {str(e)}")
            return {}

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get client performance statistics."""
        elapsed = time.time() - self.start_time
        success_rate = ((self.request_count - self.error_count) / self.request_count * 100) if self.request_count > 0 else 0

        return {"requests_total": self.request_count, "requests_success": self.request_count - self.error_count, "requests_error": self.error_count, "success_rate_pct": round(success_rate, 1), "requests_per_second": round(self.request_count / elapsed, 1) if elapsed > 0 else 0, "current_delay_ms": round(self.rate_limiter.delay * 1000, 1), "circuit_breaker_state": self.circuit_breaker.state, "elapsed_seconds": round(elapsed, 1)}

    def __del__(self):
        """Cleanup on deletion."""
        if self._session and not self._session.closed:
            # Can't await in __del__, so we schedule it
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.close())
            except:
                pass  # Best effort cleanup
