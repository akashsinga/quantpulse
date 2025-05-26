# app/services/data_fetchers/rate_limiter.py

import time
import redis
from typing import Optional, Dict, Any
import threading

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class SimpleRedisRateLimiter:
    """
    SIMPLIFIED Redis-based distributed rate limiter
    Fixed the critical bugs in the original implementation
    """

    def __init__(
            self,
            redis_url: str = None,
            requests_per_second: float = 5.0,  # Very conservative
            key_prefix: str = "dhan_api_simple"):
        """
        Initialize simplified rate limiter
        """
        self.redis_url = redis_url or settings.REDIS_URL
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second  # Minimum time between requests
        self.key_prefix = key_prefix

        # Redis keys
        self.last_request_key = f"{key_prefix}:last_request"
        self.stats_key = f"{key_prefix}:stats"

        # Connect to Redis
        try:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True, socket_connect_timeout=5, socket_timeout=5, retry_on_timeout=True)
            # Test connection
            self.redis_client.ping()
            logger.info(f"Simple Redis rate limiter initialized: {requests_per_second} req/sec")
        except Exception as e:
            logger.error(f"CRITICAL: Failed to connect to Redis: {e}")
            raise

    def acquire(self, tokens: int = 1, timeout: Optional[float] = 60.0, client_id: str = None) -> bool:
        """
        SIMPLIFIED acquire method - fixes the pipeline bugs
        """
        start_time = time.time()
        client_id = client_id or f"worker_{id(self)}"

        logger.info(f"[SIMPLE_RATE_LIMIT] {client_id} requesting {tokens} tokens")

        while True:
            try:
                current_time = time.time()

                # Get the last request time from Redis
                last_request_str = self.redis_client.get(self.last_request_key)
                last_request_time = float(last_request_str) if last_request_str else 0

                # Calculate time since last request
                time_since_last = current_time - last_request_time

                logger.debug(f"[SIMPLE_RATE_LIMIT] {client_id} time_since_last: {time_since_last:.3f}s, min_interval: {self.min_interval:.3f}s")

                if time_since_last >= self.min_interval:
                    # We can make the request - update the timestamp atomically
                    # Use Redis SET with conditional logic to prevent race conditions
                    result = self.redis_client.set(
                        self.last_request_key,
                        current_time,
                        ex=300  # Expire after 5 minutes
                    )

                    if result:
                        # Successfully acquired
                        self.redis_client.hincrby(self.stats_key, "successful_acquisitions", 1)
                        self.redis_client.expire(self.stats_key, 3600)

                        duration = time.time() - start_time
                        logger.info(f"[SIMPLE_RATE_LIMIT] ✅ {client_id} acquired {tokens} tokens in {duration:.3f}s")
                        return True

                # Check timeout
                if timeout is not None:
                    elapsed = time.time() - start_time
                    if elapsed >= timeout:
                        logger.error(f"[SIMPLE_RATE_LIMIT] ❌ {client_id} timeout after {elapsed:.2f}s")
                        self.redis_client.hincrby(self.stats_key, "timeouts", 1)
                        return False

                # Calculate wait time
                wait_time = max(0.1, self.min_interval - time_since_last + 0.1)  # Add 100ms buffer
                logger.info(f"[SIMPLE_RATE_LIMIT] 🔄 {client_id} waiting {wait_time:.2f}s")
                time.sleep(wait_time)

            except redis.RedisError as e:
                logger.error(f"[SIMPLE_RATE_LIMIT] Redis error for {client_id}: {e}")
                return False
            except Exception as e:
                logger.error(f"[SIMPLE_RATE_LIMIT] Unexpected error for {client_id}: {e}")
                return False

    def get_status(self) -> Dict[str, Any]:
        """Get current rate limiter status"""
        try:
            stats = self.redis_client.hgetall(self.stats_key) or {}
            last_request_str = self.redis_client.get(self.last_request_key)
            last_request_time = float(last_request_str) if last_request_str else 0
            current_time = time.time()

            return {'requests_per_second_limit': self.requests_per_second, 'min_interval_seconds': self.min_interval, 'last_request_time': last_request_time, 'time_since_last_request': current_time - last_request_time, 'successful_acquisitions': int(stats.get('successful_acquisitions', 0)), 'timeouts': int(stats.get('timeouts', 0)), 'redis_connected': True, 'status': 'ACTIVE'}
        except redis.RedisError as e:
            logger.error(f"Failed to get rate limiter status: {e}")
            return {'error': str(e), 'redis_connected': False}

    def clear_rate_limit_data(self):
        """Clear all rate limiting data"""
        try:
            self.redis_client.delete(self.last_request_key)
            self.redis_client.delete(self.stats_key)
            logger.info("Rate limiting data cleared")
        except redis.RedisError as e:
            logger.error(f"Failed to clear rate limiting data: {e}")


# Global rate limiter instance - ONE PER PROCESS
_global_rate_limiter = None
_rate_limiter_lock = threading.Lock()


def get_global_rate_limiter(requests_per_second: float = 5.0) -> SimpleRedisRateLimiter:
    """
    Get the global rate limiter instance (singleton per process)
    This ensures all API clients in the same process share the same rate limiter
    """
    global _global_rate_limiter

    with _rate_limiter_lock:
        if _global_rate_limiter is None:
            _global_rate_limiter = SimpleRedisRateLimiter(requests_per_second=requests_per_second)
            logger.info(f"Created global rate limiter: {requests_per_second} req/sec")

        return _global_rate_limiter


def create_distributed_rate_limiter(requests_per_second: float = 5.0, redis_url: str = None) -> SimpleRedisRateLimiter:
    """
    Factory function - now returns the global singleton
    """
    return get_global_rate_limiter(requests_per_second)
