# app/services/data_fetchers/rate_limiter.py

import time
import redis
from typing import Optional, Dict, Any

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class RedisDistributedRateLimiter:
    """
    Redis-based distributed rate limiter using sliding window algorithm
    Coordinates API calls across all workers/processes
    """

    def __init__(
            self,
            redis_url: str = None,
            requests_per_second: float = 15.0,  # Conservative: 15/sec vs Dhan's 20/sec limit
            window_size: int = 60,  # 60 second sliding window
            key_prefix: str = "dhan_api_rate_limit"):
        """
        Initialize distributed rate limiter
        
        Args:
            redis_url: Redis connection URL
            requests_per_second: Maximum requests per second across all workers
            window_size: Sliding window size in seconds
            key_prefix: Redis key prefix for rate limiting data
        """
        self.redis_url = redis_url or settings.REDIS_URL
        self.requests_per_second = requests_per_second
        self.window_size = window_size
        self.key_prefix = key_prefix

        # Redis keys
        self.calls_key = f"{key_prefix}:calls"
        self.stats_key = f"{key_prefix}:stats"
        self.circuit_key = f"{key_prefix}:circuit"

        # Connect to Redis
        try:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True, socket_connect_timeout=5, socket_timeout=5, retry_on_timeout=True, health_check_interval=30)
            # Test connection
            self.redis_client.ping()
            logger.info(f"Redis distributed rate limiter initialized: {requests_per_second} req/sec")
        except Exception as e:
            logger.error(f"Failed to connect to Redis for rate limiting: {e}")
            raise

        # Circuit breaker state
        self.circuit_failure_threshold = 50  # Increased from 10 to 50
        self.circuit_timeout = 120  # Reduced from 300 to 120 seconds (2 minutes)

    def acquire(self, tokens: int = 1, timeout: Optional[float] = None, client_id: str = None) -> bool:
        """
        Acquire permission to make API calls
        
        Args:
            tokens: Number of API calls requested
            timeout: Maximum wait time (None = block indefinitely)
            client_id: Optional client identifier for debugging
            
        Returns:
            True if permission granted, False if timeout
        """
        start_time = time.time()
        client_id = client_id or f"worker_{id(self)}"

        logger.info(f"[RATE_LIMIT] {client_id} requesting {tokens} tokens (timeout={timeout})")

        while True:
            try:
                # Test Redis connection first
                try:
                    self.redis_client.ping()
                except redis.RedisError as e:
                    logger.error(f"[RATE_LIMIT] Redis connection failed for {client_id}: {e}")
                    # CRITICAL: Do NOT fallback - fail the request
                    return False

                # Check circuit breaker first
                if self._is_circuit_open():
                    logger.warning(f"[RATE_LIMIT] Circuit breaker is OPEN, denying API call for {client_id}")
                    return False

                # Try to acquire tokens
                if self._try_acquire_tokens(tokens, client_id):
                    duration = time.time() - start_time
                    logger.info(f"[RATE_LIMIT] ✅ {client_id} acquired {tokens} tokens in {duration:.3f}s")
                    return True

                # Check timeout
                if timeout is not None:
                    elapsed = time.time() - start_time
                    if elapsed >= timeout:
                        logger.error(f"[RATE_LIMIT] ❌ {client_id} timeout after {elapsed:.2f}s")
                        return False

                # Calculate wait time and sleep
                wait_time = self._calculate_wait_time(tokens)
                sleep_time = min(wait_time, 2.0)  # Max 2 second sleep

                logger.info(f"[RATE_LIMIT] 🔄 {client_id} waiting {sleep_time:.2f}s (calculated wait: {wait_time:.2f}s)")
                time.sleep(sleep_time)

            except redis.RedisError as e:
                logger.error(f"[RATE_LIMIT] Redis error for {client_id}: {e}")
                # CRITICAL: Do NOT fallback - this was the problem
                return False
            except Exception as e:
                logger.error(f"[RATE_LIMIT] Unexpected error for {client_id}: {e}")
                return False

    def _try_acquire_tokens(self, tokens: int, client_id: str) -> bool:
        """
        Try to acquire tokens using Redis sliding window algorithm
        """
        current_time = time.time()
        window_start = current_time - self.window_size

        logger.debug(f"[RATE_LIMIT] {client_id} checking window: {window_start:.3f} to {current_time:.3f}")

        # Use Redis pipeline for atomic operations
        pipe = self.redis_client.pipeline()

        try:
            # Remove expired entries from sliding window
            pipe.zremrangebyscore(self.calls_key, 0, window_start)

            # Count current requests in window
            pipe.zcard(self.calls_key)

            # Execute pipeline
            results = pipe.execute()
            current_count = results[1]

            # Calculate how many requests we can make
            max_requests_in_window = int(self.requests_per_second * self.window_size)
            available_slots = max_requests_in_window - current_count

            logger.info(f"[RATE_LIMIT] {client_id} window status: {current_count}/{max_requests_in_window} used, {available_slots} available")

            if available_slots >= tokens:
                # Add new timestamps for the tokens we're taking
                pipe = self.redis_client.pipeline()

                for i in range(tokens):
                    # Use microsecond precision to avoid collisions
                    timestamp = current_time + (i * 0.000001)
                    pipe.zadd(self.calls_key, {f"{client_id}:{timestamp}": timestamp})

                # Set expiry on the key
                pipe.expire(self.calls_key, self.window_size * 2)

                # Update stats
                pipe.hincrby(self.stats_key, "total_requests", tokens)
                pipe.hincrby(self.stats_key, "successful_acquisitions", 1)
                pipe.expire(self.stats_key, 3600)  # 1 hour stats retention

                pipe.execute()

                logger.info(f"[RATE_LIMIT] ✅ {client_id} granted {tokens} tokens ({available_slots} slots were available)")
                return True
            else:
                # Update rejection stats
                self.redis_client.hincrby(self.stats_key, "rejections", 1)
                logger.info(f"[RATE_LIMIT] ❌ {client_id} denied {tokens} tokens (only {available_slots} available)")
                return False

        except redis.RedisError as e:
            logger.error(f"[RATE_LIMIT] Redis error in token acquisition for {client_id}: {e}")
            # CRITICAL: Do NOT fallback - this was allowing requests through
            raise

    def _calculate_wait_time(self, tokens: int) -> float:
        """
        Calculate how long to wait before next attempt
        """
        try:
            current_time = time.time()
            window_start = current_time - self.window_size

            # Get oldest timestamp in current window
            oldest_entries = self.redis_client.zrangebyscore(self.calls_key, window_start, current_time, start=0, num=1, withscores=True)

            if oldest_entries:
                oldest_timestamp = oldest_entries[0][1]
                # Wait until the oldest entry expires from the window
                wait_time = (oldest_timestamp + self.window_size) - current_time
                return max(wait_time, 0.1)  # Minimum 100ms wait
            else:
                return 0.1  # Short wait if no entries

        except redis.RedisError:
            return 1.0  # Default 1 second wait on Redis error

    def _is_circuit_open(self) -> bool:
        """
        Check if circuit breaker is open
        """
        try:
            circuit_data = self.redis_client.hgetall(self.circuit_key)

            if not circuit_data:
                return False

            state = circuit_data.get('state', 'CLOSED')
            if state != 'OPEN':
                return False

            # Check if circuit should reset
            last_failure_time = float(circuit_data.get('last_failure_time', 0))
            if time.time() - last_failure_time > self.circuit_timeout:
                # Reset circuit to half-open
                self.redis_client.hset(self.circuit_key, 'state', 'HALF_OPEN')
                logger.info("Circuit breaker reset to HALF_OPEN")
                return False

            return True

        except redis.RedisError:
            # If Redis is down, don't block requests
            return False

    def record_api_success(self, client_id: str = None):
        """
        Record successful API call for circuit breaker
        """
        try:
            pipe = self.redis_client.pipeline()

            # Reset failure count on success
            pipe.hset(self.circuit_key, 'failure_count', 0)
            pipe.hset(self.circuit_key, 'state', 'CLOSED')
            pipe.hincrby(self.stats_key, 'api_successes', 1)
            pipe.expire(self.circuit_key, 3600)

            pipe.execute()

        except redis.RedisError as e:
            logger.debug(f"Failed to record API success: {e}")

    def record_api_failure(self, client_id: str = None, error: str = None):
        """
        Record API failure for circuit breaker
        """
        try:
            pipe = self.redis_client.pipeline()

            # Increment failure count
            pipe.hincrby(self.circuit_key, 'failure_count', 1)
            pipe.hset(self.circuit_key, 'last_failure_time', time.time())
            pipe.hincrby(self.stats_key, 'api_failures', 1)

            if error:
                pipe.hset(self.circuit_key, 'last_error', str(error)[:500])

            pipe.expire(self.circuit_key, 3600)

            results = pipe.execute()
            failure_count = int(results[0])

            # Open circuit if threshold exceeded
            if failure_count >= self.circuit_failure_threshold:
                self.redis_client.hset(self.circuit_key, 'state', 'OPEN')
                logger.warning(f"Circuit breaker OPENED after {failure_count} failures")

        except redis.RedisError as e:
            logger.debug(f"Failed to record API failure: {e}")

    def get_status(self) -> Dict[str, Any]:
        """
        Get current rate limiter status
        """
        try:
            current_time = time.time()
            window_start = current_time - self.window_size

            # Get current window stats
            current_count = self.redis_client.zcount(self.calls_key, window_start, current_time)
            max_requests = int(self.requests_per_second * self.window_size)

            # Get overall stats
            stats = self.redis_client.hgetall(self.stats_key) or {}
            circuit_data = self.redis_client.hgetall(self.circuit_key) or {}

            return {
                'current_window_requests': current_count,
                'max_requests_per_window': max_requests,
                'requests_per_second_limit': self.requests_per_second,
                'window_size_seconds': self.window_size,
                'utilization_percent': round((current_count / max_requests) * 100, 2),
                'available_slots': max_requests - current_count,
                'total_requests': int(stats.get('total_requests', 0)),
                'successful_acquisitions': int(stats.get('successful_acquisitions', 0)),
                'rejections': int(stats.get('rejections', 0)),
                'api_successes': int(stats.get('api_successes', 0)),
                'api_failures': int(stats.get('api_failures', 0)),
                'circuit_breaker': {
                    'state': circuit_data.get('state', 'CLOSED'),
                    'failure_count': int(circuit_data.get('failure_count', 0)),
                    'last_failure_time': circuit_data.get('last_failure_time'),
                    'last_error': circuit_data.get('last_error')
                }
            }

        except redis.RedisError as e:
            logger.error(f"Failed to get rate limiter status: {e}")
            return {'error': str(e)}

    def reset_circuit_breaker(self):
        """
        Manually reset circuit breaker (admin function)
        """
        try:
            self.redis_client.delete(self.circuit_key)
            logger.info("Circuit breaker manually reset")
        except redis.RedisError as e:
            logger.error(f"Failed to reset circuit breaker: {e}")

    def clear_rate_limit_data(self):
        """
        Clear all rate limiting data (admin function)
        """
        try:
            pipe = self.redis_client.pipeline()
            pipe.delete(self.calls_key)
            pipe.delete(self.stats_key)
            pipe.delete(self.circuit_key)
            pipe.execute()
            logger.info("Rate limiting data cleared")
        except redis.RedisError as e:
            logger.error(f"Failed to clear rate limiting data: {e}")


# Factory function for easy integration
def create_distributed_rate_limiter(requests_per_second: float = 15.0, redis_url: str = None) -> RedisDistributedRateLimiter:
    """
    Factory function to create Redis distributed rate limiter
    """
    return RedisDistributedRateLimiter(redis_url=redis_url, requests_per_second=requests_per_second)
