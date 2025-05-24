# app/services/data_fetchers/rate_limiter.py

import time
import asyncio
from threading import Lock
from typing import Optional
from datetime import datetime, timedelta

from app.utils.logger import get_logger

logger = get_logger(__name__)


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter for API requests
    Thread-safe implementation for concurrent request handling
    """

    def __init__(self, requests_per_second: float = 20.0, burst_capacity: Optional[int] = None):
        """
        Initialize rate limiter
        
        Args:
            requests_per_second: Maximum requests per second
            burst_capacity: Maximum burst capacity (defaults to 2x rate)
        """
        self.rate = requests_per_second
        self.capacity = burst_capacity or int(requests_per_second * 2)
        self.tokens = float(self.capacity)
        self.last_refill = time.time()
        self.lock = Lock()

        logger.info(f"Initialized rate limiter: {requests_per_second} req/sec, capacity: {self.capacity}")

    def acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """
        Acquire tokens from the bucket
        
        Args:
            tokens: Number of tokens to acquire
            timeout: Maximum time to wait for tokens (None = no timeout)
            
        Returns:
            True if tokens acquired, False if timeout
        """
        start_time = time.time()

        while True:
            with self.lock:
                self._refill_bucket()

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    logger.debug(f"Acquired {tokens} tokens, {self.tokens:.2f} remaining")
                    return True

            # Check timeout
            if timeout is not None and (time.time() - start_time) >= timeout:
                logger.warning(f"Rate limiter timeout after {timeout}s waiting for {tokens} tokens")
                return False

            # Sleep before retrying
            sleep_time = min(tokens / self.rate, 1.0)  # Don't sleep more than 1 second
            time.sleep(sleep_time)

    def _refill_bucket(self):
        """Refill the token bucket based on elapsed time"""
        now = time.time()
        elapsed = now - self.last_refill

        # Add tokens based on elapsed time
        tokens_to_add = elapsed * self.rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_refill = now

    def get_wait_time(self, tokens: int = 1) -> float:
        """
        Get estimated wait time for tokens
        
        Args:
            tokens: Number of tokens needed
            
        Returns:
            Estimated wait time in seconds
        """
        with self.lock:
            self._refill_bucket()

            if self.tokens >= tokens:
                return 0.0

            tokens_needed = tokens - self.tokens
            return tokens_needed / self.rate

    def get_status(self) -> dict:
        """Get current rate limiter status"""
        with self.lock:
            self._refill_bucket()

            return {'tokens_available': round(self.tokens, 2), 'capacity': self.capacity, 'rate_per_second': self.rate, 'utilization_percent': round((1 - self.tokens / self.capacity) * 100, 2)}


class AsyncTokenBucketRateLimiter:
    """
    Async version of token bucket rate limiter
    For use with asyncio-based applications
    """

    def __init__(self, requests_per_second: float = 20.0, burst_capacity: Optional[int] = None):
        self.rate = requests_per_second
        self.capacity = burst_capacity or int(requests_per_second * 2)
        self.tokens = float(self.capacity)
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

        logger.info(f"Initialized async rate limiter: {requests_per_second} req/sec")

    async def acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """Async version of token acquisition"""
        start_time = time.time()

        while True:
            async with self._lock:
                self._refill_bucket()

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True

            if timeout is not None and (time.time() - start_time) >= timeout:
                return False

            sleep_time = min(tokens / self.rate, 1.0)
            await asyncio.sleep(sleep_time)

    def _refill_bucket(self):
        """Same refill logic as sync version"""
        now = time.time()
        elapsed = now - self.last_refill
        tokens_to_add = elapsed * self.rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_refill = now


class CircuitBreaker:
    """
    Circuit breaker for API failure protection
    Prevents cascade failures when API is down
    """

    def __init__(self, failure_threshold: int = 10, recovery_timeout: int = 60, expected_exception: Exception = Exception):
        """
        Initialize circuit breaker
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            expected_exception: Exception type that triggers the circuit
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        self.lock = Lock()

        logger.info(f"Initialized circuit breaker: threshold={failure_threshold}, timeout={recovery_timeout}s")

    def call(self, func, *args, **kwargs):
        """
        Execute function with circuit breaker protection
        
        Args:
            func: Function to execute
            *args, **kwargs: Function arguments
            
        Returns:
            Function result
            
        Raises:
            CircuitBreakerOpenError: When circuit is open
        """
        with self.lock:
            if self.state == 'OPEN':
                if self._should_attempt_reset():
                    self.state = 'HALF_OPEN'
                    logger.info("Circuit breaker moving to HALF_OPEN state")
                else:
                    raise CircuitBreakerOpenError("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result

        except self.expected_exception as e:
            self._on_failure()
            raise

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset"""
        return (self.last_failure_time and time.time() - self.last_failure_time >= self.recovery_timeout)

    def _on_success(self):
        """Handle successful call"""
        with self.lock:
            self.failure_count = 0
            if self.state == 'HALF_OPEN':
                self.state = 'CLOSED'
                logger.info("Circuit breaker reset to CLOSED state")

    def _on_failure(self):
        """Handle failed call"""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'
                logger.warning(f"Circuit breaker OPENED after {self.failure_count} failures")

    def get_status(self) -> dict:
        """Get circuit breaker status"""
        with self.lock:
            return {'state': self.state, 'failure_count': self.failure_count, 'failure_threshold': self.failure_threshold, 'last_failure_time': self.last_failure_time, 'time_until_retry': max(0, self.recovery_timeout - (time.time() - (self.last_failure_time or 0))) if self.last_failure_time else 0}


class CircuitBreakerOpenError(Exception):
    """Exception raised when circuit breaker is open"""
    pass


class RateLimitedAPIClient:
    """
    Combines rate limiting and circuit breaker for robust API access
    """

    def __init__(self, requests_per_second: float = 20.0, burst_capacity: Optional[int] = None, circuit_breaker_threshold: int = 10, circuit_breaker_timeout: int = 60):
        """
        Initialize rate limited API client
        
        Args:
            requests_per_second: Rate limit
            burst_capacity: Burst capacity
            circuit_breaker_threshold: Circuit breaker failure threshold
            circuit_breaker_timeout: Circuit breaker recovery timeout
        """
        self.rate_limiter = TokenBucketRateLimiter(requests_per_second, burst_capacity)
        self.circuit_breaker = CircuitBreaker(circuit_breaker_threshold, circuit_breaker_timeout)

        logger.info("Initialized rate limited API client with circuit breaker")

    def execute_request(self, func, *args, timeout: Optional[float] = None, **kwargs):
        """
        Execute API request with rate limiting and circuit breaker
        
        Args:
            func: Function to execute
            timeout: Timeout for rate limiter
            *args, **kwargs: Function arguments
            
        Returns:
            Function result
        """
        # Acquire rate limit token
        if not self.rate_limiter.acquire(timeout=timeout):
            raise TimeoutError("Rate limiter timeout")

        # Execute with circuit breaker protection
        return self.circuit_breaker.call(func, *args, **kwargs)

    def get_status(self) -> dict:
        """Get combined status of rate limiter and circuit breaker"""
        return {'rate_limiter': self.rate_limiter.get_status(), 'circuit_breaker': self.circuit_breaker.get_status()}
