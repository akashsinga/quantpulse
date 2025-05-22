# app/services/data_fetchers/dhan_api_client.py

import json
import time
import asyncio
import threading
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Union, Any
import aiohttp
import requests
import os
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dataclasses import dataclass

from app.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class RateLimitException(Exception):
    """Exception raised when rate limit is hit."""

    pass


class CircuitBreakerOpenError(Exception):
    """Exception raised when circuit breaker is open."""

    pass


@dataclass
class PerformanceMetrics:
    """Performance metrics for monitoring"""

    requests_total: int = 0
    requests_success: int = 0
    requests_error: int = 0
    rate_limit_hits: int = 0
    circuit_breaker_trips: int = 0
    avg_response_time_ms: float = 0.0
    current_delay_ms: float = 0.0

    @property
    def success_rate_pct(self) -> float:
        if self.requests_total == 0:
            return 0.0
        return (self.requests_success / self.requests_total) * 100

    def to_dict(self) -> Dict[str, Any]:
        return {"requests_total": self.requests_total, "requests_success": self.requests_success, "requests_error": self.requests_error, "success_rate_pct": round(self.success_rate_pct, 2), "rate_limit_hits": self.rate_limit_hits, "circuit_breaker_trips": self.circuit_breaker_trips, "avg_response_time_ms": round(self.avg_response_time_ms, 2), "current_delay_ms": round(self.current_delay_ms, 2)}


class AdaptiveRateLimiter:
    """Enhanced adaptive rate limiter with better performance tracking."""

    def __init__(self, initial_delay: float = 0.01):
        self.delay = initial_delay
        self.consecutive_errors = 0
        self.success_count = 0
        self.last_adjustment = time.time()
        self.min_delay = float(os.getenv("RATE_LIMITER_MIN_DELAY", "0.005"))  # 5ms minimum
        self.max_delay = float(os.getenv("RATE_LIMITER_MAX_DELAY", "2.0"))  # 2s maximum
        self.success_threshold = int(os.getenv("RATE_LIMITER_SUCCESS_THRESHOLD", "10"))
        self.adjustment_window = int(os.getenv("RATE_LIMITER_ADJUSTMENT_WINDOW", "10"))
        self._lock = threading.Lock()

    async def wait(self):
        """Wait with current delay."""
        if self.delay > 0:
            await asyncio.sleep(self.delay)

    def on_success(self):
        """Called on successful API response."""
        with self._lock:
            self.success_count += 1
            self.consecutive_errors = 0

            # Reduce delay after multiple successes
            if self.success_count >= self.success_threshold and time.time() - self.last_adjustment > self.adjustment_window:
                old_delay = self.delay
                self.delay = max(self.min_delay, self.delay * 0.9)
                if abs(self.delay - old_delay) > 0.001:  # Only log significant changes
                    logger.debug(f"Rate limit improved: {old_delay*1000:.1f}ms → {self.delay*1000:.1f}ms")
                    self.last_adjustment = time.time()
                    self.success_count = 0

    def on_rate_limit(self):
        """Called when rate limit is hit."""
        with self._lock:
            self.consecutive_errors += 1
            old_delay = self.delay

            # Exponential backoff with jitter
            backoff_factor = 1.8 + (self.consecutive_errors * 0.2)
            self.delay = min(self.max_delay, self.delay * backoff_factor)

            logger.warning(f"Rate limit hit! Delay: {old_delay*1000:.1f}ms → {self.delay*1000:.1f}ms (errors: {self.consecutive_errors})")
            self.last_adjustment = time.time()
            self.success_count = 0

    def on_error(self):
        """Called on other errors."""
        with self._lock:
            self.consecutive_errors += 1
            if self.consecutive_errors >= 3:
                old_delay = self.delay
                self.delay = min(self.max_delay, self.delay * 1.3)
                if abs(self.delay - old_delay) > 0.001:
                    logger.debug(f"Error backoff: {old_delay*1000:.1f}ms → {self.delay*1000:.1f}ms")

    def get_current_delay_ms(self) -> float:
        """Get current delay in milliseconds."""
        return self.delay * 1000


class CircuitBreaker:
    """Enhanced circuit breaker with better state management."""

    def __init__(self, failure_threshold: int = None, timeout: int = None):
        self.failure_threshold = failure_threshold or settings.CIRCUIT_BREAKER_FAILURE_THRESHOLD
        self.timeout = timeout or settings.CIRCUIT_BREAKER_TIMEOUT
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.success_count_in_half_open = 0
        self.required_successes_in_half_open = 3
        self._lock = threading.Lock()

    def can_execute(self) -> bool:
        """Check if request can be executed."""
        with self._lock:
            if self.state == "CLOSED":
                return True

            if self.state == "OPEN":
                if time.time() - self.last_failure_time > self.timeout:
                    self.state = "HALF_OPEN"
                    self.success_count_in_half_open = 0
                    logger.info("Circuit breaker: OPEN → HALF_OPEN")
                    return True
                return False

            # HALF_OPEN state
            return True

    def on_success(self):
        """Record successful request."""
        with self._lock:
            if self.state == "HALF_OPEN":
                self.success_count_in_half_open += 1
                if self.success_count_in_half_open >= self.required_successes_in_half_open:
                    self.state = "CLOSED"
                    self.failure_count = 0
                    logger.info("Circuit breaker: HALF_OPEN → CLOSED")
            elif self.state == "CLOSED":
                # Reset failure count on success
                if self.failure_count > 0:
                    self.failure_count = max(0, self.failure_count - 1)

    def on_failure(self):
        """Record failed request."""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state == "HALF_OPEN":
                self.state = "OPEN"
                logger.warning(f"Circuit breaker: HALF_OPEN → OPEN (failure during test)")
            elif self.failure_count >= self.failure_threshold and self.state == "CLOSED":
                self.state = "OPEN"
                logger.error(f"Circuit breaker: CLOSED → OPEN (failures: {self.failure_count})")

    def get_state(self) -> str:
        """Get current circuit breaker state."""
        return self.state


class DhanAPIClient:
    """Enhanced high-performance async Dhan API client with thread-safe session management."""

    def __init__(self):
        """Initialize the enhanced async Dhan API client."""

        # API Credentials
        self.api_key = os.getenv("API_KEY")
        self.client_id = os.getenv("CLIENT_ID")

        # Headers
        self.headers = {"Content-Type": "application/json", "access-token": self.api_key or "", "client-id": self.client_id or "", "User-Agent": "QuantPulse/1.0"}

        # Validate credentials
        if not self.api_key or not self.client_id:
            logger.warning("API credentials incomplete! Set API_KEY and CLIENT_ID environment variables.")

        # API endpoints
        self.historical_url = settings.DHAN_CHARTS_HISTORICAL_URL
        self.quote_url = settings.DHAN_TODAY_EOD_URL

        # Enhanced components
        self.rate_limiter = AdaptiveRateLimiter(initial_delay=settings.OHLCV_REQUEST_DELAY)
        self.circuit_breaker = CircuitBreaker()

        # Performance metrics
        self.metrics = PerformanceMetrics()
        self.start_time = time.time()
        self.response_times = []
        self.max_response_times_tracked = 1000  # Limit memory usage

        # Thread-local session storage to avoid event loop conflicts
        self._local = threading.local()

        logger.info(f"Initialized enhanced Dhan API client")
        logger.info(f"Rate limiter config: min={self.rate_limiter.min_delay*1000:.1f}ms, max={self.rate_limiter.max_delay*1000:.1f}ms")
        logger.info(f"Circuit breaker config: threshold={self.circuit_breaker.failure_threshold}, timeout={self.circuit_breaker.timeout}s")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session with thread-local storage."""
        # Use thread-local storage to avoid event loop conflicts
        if not hasattr(self._local, "session") or self._local.session.closed:
            # Use ThreadedResolver to avoid aiodns dependency
            resolver = aiohttp.ThreadedResolver()
            logger.debug("Creating new thread-local session with ThreadedResolver")

            # Enhanced connector configuration - REMOVED tcp_keepalive
            connector = aiohttp.TCPConnector(
                limit=50,  # Reduced for thread-local sessions
                limit_per_host=25,  # Reduced for thread-local sessions
                keepalive_timeout=60,
                enable_cleanup_closed=True,
                use_dns_cache=True,
                ttl_dns_cache=600,
                resolver=resolver,
                verify_ssl=True,
                # REMOVED: tcp_keepalive=True  # This parameter doesn't exist in older aiohttp versions
            )

            # Enhanced timeout configuration
            timeout = aiohttp.ClientTimeout(total=45, connect=15, sock_read=30, sock_connect=15)

            self._local.session = aiohttp.ClientSession(connector=connector, timeout=timeout, headers=self.headers, raise_for_status=False)

            logger.debug("Created thread-local aiohttp session")

        return self._local.session

    async def close(self):
        """Close the aiohttp session."""
        if hasattr(self._local, "session") and not self._local.session.closed:
            await self._local.session.close()
            logger.debug("Closed thread-local aiohttp session")

    async def _make_async_request(self, url: str, data: Dict[str, Any], method: str = "POST") -> Dict[str, Any]:
        """Make async API request with enhanced error handling."""

        logger.info(f"_make_async_request called with URL: {url}")
        logger.info(f"Request method: {method}")
        logger.info(f"Request data: {data}")

        # Check circuit breaker
        if not self.circuit_breaker.can_execute():
            self.metrics.circuit_breaker_trips += 1
            logger.error("Circuit breaker is OPEN")
            raise CircuitBreakerOpenError("Circuit breaker is OPEN - too many failures")

        # Apply adaptive rate limiting
        await self.rate_limiter.wait()

        try:
            session = await self._get_session()
            logger.info("Session obtained successfully")
        except Exception as e:
            logger.error(f"Failed to get session: {str(e)}")
            raise

        request_start = time.time()

        try:
            self.metrics.requests_total += 1
            logger.info(f"Making {method} request to {url}")

            # Make request
            if method.upper() == "POST":
                async with session.post(url, json=data) as response:
                    logger.info(f"POST response status: {response.status}")
                    result = await self._process_response(response)
            else:
                async with session.get(url, params=data) as response:
                    logger.info(f"GET response status: {response.status}")
                    result = await self._process_response(response)

            # Track response time
            response_time = (time.time() - request_start) * 1000  # Convert to ms
            self._track_response_time(response_time)

            logger.info(f"Request completed in {response_time:.2f}ms")
            return result

        except aiohttp.ClientConnectorError as e:
            logger.error(f"Connection error: {str(e)}")
            self.metrics.requests_error += 1
            self.circuit_breaker.on_failure()
            self.rate_limiter.on_error()
            raise
        except RateLimitException:
            logger.warning("Rate limit exception")
            self.metrics.rate_limit_hits += 1
            raise
        except Exception as e:
            logger.error(f"Request failed with exception: {str(e)}")
            logger.error(f"Exception type: {type(e)}")
            self.metrics.requests_error += 1
            self.circuit_breaker.on_failure()
            self.rate_limiter.on_error()
            raise

    async def _process_response(self, response: aiohttp.ClientResponse) -> Dict[str, Any]:
        """Process aiohttp response with enhanced error handling."""

        # Handle rate limiting
        if response.status == 429:
            self.rate_limiter.on_rate_limit()

            # Check for Retry-After header
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    wait_time = int(retry_after)
                    logger.warning(f"Rate limited - waiting {wait_time}s")
                    await asyncio.sleep(wait_time)
                except ValueError:
                    pass

            raise RateLimitException("Rate limit exceeded")

        # Handle other HTTP errors
        if response.status >= 400:
            error_text = await response.text()
            logger.error(f"HTTP {response.status}: {error_text[:200]}")

            # Don't raise for 4xx errors, return error response instead
            if 400 <= response.status < 500:
                return {"status": "error", "error": f"Client error {response.status}: {error_text[:100]}", "data": []}

            # Raise for 5xx errors (server errors)
            response.raise_for_status()

        # Success case
        self.metrics.requests_success += 1
        self.circuit_breaker.on_success()
        self.rate_limiter.on_success()

        try:
            return await response.json()
        except Exception as e:
            logger.error(f"Failed to parse JSON response: {str(e)}")
            return {"status": "error", "error": "Invalid JSON response", "data": []}

    def _track_response_time(self, response_time_ms: float):
        """Track response times for performance monitoring."""
        self.response_times.append(response_time_ms)

        # Limit memory usage by keeping only recent response times
        if len(self.response_times) > self.max_response_times_tracked:
            self.response_times = self.response_times[-self.max_response_times_tracked // 2 :]

        # Update average response time
        if self.response_times:
            self.metrics.avg_response_time_ms = sum(self.response_times) / len(self.response_times)

    # Sync wrapper for backwards compatibility
    def fetch_historical_data(self, securityId: str, exchangeSegment: str, instrument: str, expiryCode: int = 0, fromDate: Optional[str] = None, toDate: Optional[str] = None, oi: bool = False) -> Dict[str, Any]:
        """Sync wrapper for async historical data fetch."""
        return asyncio.run(self.fetch_historical_data_async(securityId, exchangeSegment, instrument, expiryCode, fromDate, toDate, oi))

    async def fetch_historical_data_async(self, securityId: str, exchangeSegment: str, instrument: str, expiryCode: int = 0, fromDate: Optional[str] = None, toDate: Optional[str] = None, oi: bool = False) -> Dict[str, Any]:
        """Fetch historical OHLCV data asynchronously with enhanced error handling."""

        # Use defaults from settings if not specified
        fromDate = fromDate or settings.FROM_DATE
        toDate = toDate or (settings.TO_DATE or datetime.now().strftime("%Y-%m-%d"))

        request_data = {"securityId": str(securityId), "exchangeSegment": exchangeSegment, "instrument": instrument, "expiryCode": expiryCode, "fromDate": fromDate, "toDate": toDate, "oi": oi}

        # Add detailed debugging
        logger.info(f"Making API request for security {securityId}")
        logger.info(f"URL: {self.historical_url}")
        logger.info(f"Request data: {request_data}")
        logger.info(f"Headers: {self.headers}")

        try:
            response = await self._make_async_request(self.historical_url, request_data)
            logger.info(f"API response for {securityId}: {response}")
            return self._parse_historical_response(response)
        except CircuitBreakerOpenError:
            logger.error(f"Circuit breaker open for {securityId}")
            raise  # Re-raise circuit breaker errors
        except RateLimitException:
            logger.error(f"Rate limit hit for {securityId}")
            raise  # Re-raise rate limit errors
        except Exception as e:
            logger.error(f"API call failed for {securityId}: {str(e)}")
            logger.error(f"Exception type: {type(e)}")
            return {"status": "error", "error": str(e), "data": []}

    # Sync wrapper for current data
    def fetch_current_data(self, securities_by_segment: Dict[str, List[str]]) -> Dict[str, Dict[str, Any]]:
        """Sync wrapper for async current data fetch."""
        return asyncio.run(self.fetch_current_data_async(securities_by_segment))

    async def fetch_current_data_async(self, securities_by_segment: Dict[str, List[str]]) -> Dict[str, Dict[str, Any]]:
        """Fetch current day OHLCV data asynchronously with enhanced error handling."""
        try:
            response = await self._make_async_request(self.quote_url, securities_by_segment)
            return self._parse_quote_response(response)
        except CircuitBreakerOpenError:
            raise  # Re-raise circuit breaker errors
        except RateLimitException:
            raise  # Re-raise rate limit errors
        except Exception as e:
            logger.debug(f"Current data fetch failed: {str(e)}")
            return {}

    def _parse_historical_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Parse historical data response with enhanced validation."""
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
            arrays = [timestamps, opens, highs, lows, closes, volumes]
            if not all(arrays) or len(set(len(arr) for arr in arrays)) > 1:
                logger.warning("Mismatched array lengths in historical response")
                return {"data": [], "status": "error", "error": "Mismatched data arrays"}

            min_length = len(timestamps)
            if min_length == 0:
                return {"data": [], "status": "empty"}

            # Construct structured records with validation
            records = []
            for i in range(min_length):
                try:
                    # Convert timestamp (epoch seconds) to datetime
                    ts = datetime.fromtimestamp(timestamps[i], tz=settings.INDIA_TZ)

                    # Basic data validation
                    ohlc = [opens[i], highs[i], lows[i], closes[i]]
                    if any(val <= 0 for val in ohlc) or highs[i] < lows[i] or volumes[i] < 0:
                        continue  # Skip invalid records

                    records.append({"time": ts, "open": float(opens[i]), "high": float(highs[i]), "low": float(lows[i]), "close": float(closes[i]), "volume": int(volumes[i])})

                except (ValueError, TypeError, OverflowError) as e:
                    logger.debug(f"Skipping invalid record at index {i}: {e}")
                    continue

            return {"data": records, "status": "success", "count": len(records)}

        except Exception as e:
            logger.debug(f"Parse historical error: {str(e)}")
            return {"data": [], "status": "error", "error": str(e)}

    def _parse_quote_response(self, response: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Parse quote/current day data response with enhanced validation."""
        if not response or "data" not in response or response.get("status") != "success":
            return {}

        result = {}
        try:
            data = response.get("data", {})

            # Process each exchange segment
            for segment, securities in data.items():
                if not isinstance(securities, dict):
                    continue

                for security_id, security_data in securities.items():
                    try:
                        # Extract OHLC data
                        ohlc_data = security_data.get("ohlc", {})

                        # Validate required fields
                        if not ohlc_data or not security_data.get("last_price"):
                            continue

                        # Create standardized record with validation
                        open_price = ohlc_data.get("open")
                        high_price = ohlc_data.get("high")
                        low_price = ohlc_data.get("low")
                        close_price = ohlc_data.get("close") or security_data.get("last_price")
                        volume = security_data.get("volume", 0)

                        # Basic validation
                        if not all(isinstance(x, (int, float)) and x > 0 for x in [open_price, high_price, low_price, close_price]):
                            continue

                        if high_price < low_price:
                            continue

                        result[security_id] = {"security_id": security_id, "exchange_segment": segment, "last_price": float(security_data.get("last_price")), "open": float(open_price), "high": float(high_price), "low": float(low_price), "close": float(close_price), "volume": int(volume), "timestamp": datetime.now(tz=settings.INDIA_TZ), "source": "dhan_quote_api"}

                    except (ValueError, TypeError, KeyError) as e:
                        logger.debug(f"Skipping invalid quote data for {security_id}: {e}")
                        continue

            return result

        except Exception as e:
            logger.debug(f"Parse quote error: {str(e)}")
            return {}

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get enhanced client performance statistics."""
        elapsed = time.time() - self.start_time

        # Update current metrics
        self.metrics.current_delay_ms = self.rate_limiter.get_current_delay_ms()

        stats = self.metrics.to_dict()
        stats.update({"requests_per_second": round(self.metrics.requests_total / elapsed, 2) if elapsed > 0 else 0, "circuit_breaker_state": self.circuit_breaker.get_state(), "elapsed_seconds": round(elapsed, 1), "response_time_p95_ms": self._get_percentile_response_time(0.95), "response_time_p99_ms": self._get_percentile_response_time(0.99)})

        return stats

    def _get_percentile_response_time(self, percentile: float) -> float:
        """Calculate response time percentile."""
        if not self.response_times:
            return 0.0

        sorted_times = sorted(self.response_times)
        index = int(len(sorted_times) * percentile)
        if index >= len(sorted_times):
            index = len(sorted_times) - 1

        return round(sorted_times[index], 2)

    def reset_performance_stats(self):
        """Reset performance statistics."""
        self.metrics = PerformanceMetrics()
        self.start_time = time.time()
        self.response_times = []
        logger.info("Performance statistics reset")

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check of the API client."""
        health_status = {"status": "healthy", "timestamp": datetime.now().isoformat(), "circuit_breaker_state": self.circuit_breaker.get_state(), "current_delay_ms": self.rate_limiter.get_current_delay_ms(), "session_status": "thread_local"}

        # Perform a simple connectivity test
        try:
            # Use a minimal request to test connectivity
            test_data = {"securityId": "1", "exchangeSegment": "NSE_EQ", "instrument": "EQUITY", "fromDate": "2024-01-01", "toDate": "2024-01-02"}

            response = await asyncio.wait_for(self._make_async_request(self.historical_url, test_data), timeout=5.0)

            if response.get("status") in ["success", "error"]:  # Any response is good for health check
                health_status["connectivity"] = "ok"
            else:
                health_status["connectivity"] = "degraded"
                health_status["status"] = "degraded"

        except asyncio.TimeoutError:
            health_status["connectivity"] = "timeout"
            health_status["status"] = "unhealthy"
        except CircuitBreakerOpenError:
            health_status["connectivity"] = "circuit_breaker_open"
            health_status["status"] = "unhealthy"
        except Exception as e:
            health_status["connectivity"] = f"error: {str(e)}"
            health_status["status"] = "degraded"

        return health_status

    def __del__(self):
        """Enhanced cleanup on deletion."""
        # For thread-local sessions, we can't easily clean them up in __del__
        # They will be cleaned up when the thread ends
        pass
