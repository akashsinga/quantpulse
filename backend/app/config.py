# backend/app/config.py

from pydantic_settings import BaseSettings
from typing import List, Optional
from functools import lru_cache
from datetime import timezone, timedelta

import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# Find the .env file - first look for it explicitly, then try autodetection
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    # Fall back to auto-detecting .env file
    dotenv_path = find_dotenv()
    if dotenv_path:
        load_dotenv(dotenv_path)
    else:
        print("WARNING: No .env file found!")


class Settings(BaseSettings):
    """Application settings with environment variable loading"""

    # General Settings
    APP_NAME: str = "QuantPulse API"
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    API_V1_PREFIX: str = "/api/v1"

    # CORS Settings
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:8000", "https://quantpulse.app"]

    # Database connection - Enhanced for TimescaleDB optimization
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:password@localhost:5432/quantpulse")
    ASYNC_DATABASE_URL: str = os.getenv("ASYNC_DATABASE_URL", "postgresql+asyncpg://postgres:password@localhost:5432/quantpulse")

    # Authentication
    SECRET_KEY: str = os.getenv("SECRET_KEY", "quant_pulse_2025")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours

    # DHAN API Configuration
    DHAN_ACCESS_TOKEN: str = os.getenv("DHAN_ACCESS_TOKEN", "")
    DHAN_CLIENT_ID: str = os.getenv("DHAN_CLIENT_ID", "")
    DHAN_CHARTS_HISTORICAL_URL: str = os.getenv("DHAN_CHARTS_HISTORICAL_URL", "https://api.dhan.co/v2/charts/historical")
    DHAN_TODAY_EOD_URL: str = os.getenv("DHAN_TODAY_EOD_URL", "https://api.dhan.co/v2/marketfeed/quote")

    # OHLCV Fetcher Configuration - FIXED FOR SEQUENTIAL PROCESSING
    OHLCV_WORKERS: int = 1  # SEQUENTIAL: Only 1 worker
    OHLCV_BATCH_SIZE: int = int(os.getenv("OHLCV_BATCH_SIZE", "50"))  # Smaller batches
    OHLCV_MAX_CONCURRENT: int = 1  # SEQUENTIAL: No concurrency
    OHLCV_REQUEST_DELAY: float = 0.2  # 5 req/sec = 0.2s delay (VERY CONSERVATIVE)
    OHLCV_MAX_RETRIES: int = int(os.getenv("OHLCV_MAX_RETRIES", "3"))
    OHLCV_BULK_INSERT_SIZE: int = int(os.getenv("OHLCV_BULK_INSERT_SIZE", "1000"))
    OHLCV_CACHE_SIZE: int = int(os.getenv("OHLCV_CACHE_SIZE", "5000"))  # Reduced for sequential
    OHLCV_MEMORY_CHECK_INTERVAL: int = int(os.getenv("OHLCV_MEMORY_CHECK_INTERVAL", "100"))
    OHLCV_BATCH_SIZE_TODAY: int = int(os.getenv("OHLCV_BATCH_SIZE_TODAY", "100"))  # Smaller batches
    OHLCV_HISTORICAL_WORKERS: int = 1  # SEQUENTIAL: No parallel workers

    # Weekly Data Aggregation Settings - Optimized for sequential processing
    WEEKLY_AGGREGATION_BATCH_SIZE: int = int(os.getenv("WEEKLY_AGGREGATION_BATCH_SIZE", "100"))  # Smaller batches
    WEEKLY_AGGREGATION_MAX_WORKERS: int = int(os.getenv("WEEKLY_AGGREGATION_MAX_WORKERS", "4"))  # Still parallel for aggregation
    WEEKLY_AGGREGATION_CHUNK_SIZE: int = int(os.getenv("WEEKLY_AGGREGATION_CHUNK_SIZE", "1000"))
    WEEKLY_AGGREGATION_WEEKS_BACK: int = int(os.getenv("WEEKLY_AGGREGATION_WEEKS_BACK", "4"))
    WEEKLY_DATA_RETENTION_DAYS: int = int(os.getenv("WEEKLY_DATA_RETENTION_DAYS", "1825"))  # 5 years

    # TimescaleDB Optimization Settings
    ENABLE_TIMESCALEDB_OPTIMIZATION: bool = os.getenv("ENABLE_TIMESCALEDB_OPTIMIZATION", "True").lower() == "true"
    TIMESCALEDB_CHUNK_TIME_INTERVAL: str = os.getenv("TIMESCALEDB_CHUNK_TIME_INTERVAL", "1 month")
    ENABLE_CONTINUOUS_AGGREGATES: bool = os.getenv("ENABLE_CONTINUOUS_AGGREGATES", "True").lower() == "true"
    CONTINUOUS_AGGREGATE_REFRESH_INTERVAL: str = os.getenv("CONTINUOUS_AGGREGATE_REFRESH_INTERVAL", "1 day")

    # Parallel Processing Settings - DISABLED FOR SEQUENTIAL
    PARALLEL_BATCH_PROCESSING: bool = False  # SEQUENTIAL: Disabled
    MAX_PARALLEL_BATCHES: int = 1  # SEQUENTIAL: Only 1 batch at a time
    SECURITIES_PER_PARALLEL_BATCH: int = int(os.getenv("SECURITIES_PER_PARALLEL_BATCH", "100"))  # Smaller batches

    # Rate limiting - VERY CONSERVATIVE FOR SEQUENTIAL
    RATE_LIMIT_SLEEP: float = 0.2  # 5 req/sec (very conservative)
    API_MAX_RETRIES: int = int(os.getenv("API_MAX_RETRIES", "3"))
    RETRY_BACKOFF_FACTOR: float = float(os.getenv("RETRY_BACKOFF_FACTOR", "2.0"))
    RETRY_INITIAL_WAIT: float = float(os.getenv("RETRY_INITIAL_WAIT", "1.0"))

    # Paths
    MODEL_STORAGE_PATH: str = os.getenv("MODEL_STORAGE_PATH", "./models")
    CACHE_DIR: str = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache")

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Timezone
    INDIA_TZ: timezone = timezone(timedelta(hours=5, minutes=30))

    # Websocket settings
    MAX_CONN_SETTINGS: int = 10
    ATTEMPT_WINDOW: int = 60  # seconds

    # Database pool settings - Reduced for sequential processing
    DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "20"))  # Reduced from 100
    DB_MAX_OVERFLOW: int = int(os.getenv("DB_MAX_OVERFLOW", "20"))  # Reduced from 100
    DB_POOL_TIMEOUT: int = int(os.getenv("DB_POOL_TIMEOUT", "30"))
    DB_POOL_RECYCLE: int = int(os.getenv("DB_POOL_RECYCLE", "1800"))  # 30 minutes
    DB_ECHO: bool = os.getenv("SQL_ECHO", "False").lower() == "true"
    DB_CONNECT_ARGS: dict = {
        "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "10")),
        "server_settings": {
            "application_name": "quantpulse_sequential",
            # PostgreSQL optimization settings for sequential processing
            "shared_preload_libraries": "timescaledb",
            "max_connections": "100",  # Reduced from 200
            "work_mem": "128MB",  # Reduced for sequential processing
            "maintenance_work_mem": "512MB",  # Reduced from 1GB
            "effective_cache_size": "2GB",  # Reduced from 4GB
            "random_page_cost": "1.1",  # For SSD storage
        },
    }

    # Query Optimization Settings
    ENABLE_QUERY_OPTIMIZATION: bool = os.getenv("ENABLE_QUERY_OPTIMIZATION", "True").lower() == "true"
    QUERY_TIMEOUT: int = int(os.getenv("QUERY_TIMEOUT", "300"))  # 5 minutes for complex aggregations
    ENABLE_QUERY_CACHING: bool = os.getenv("ENABLE_QUERY_CACHING", "True").lower() == "true"
    QUERY_CACHE_SIZE: int = int(os.getenv("QUERY_CACHE_SIZE", "500"))  # Reduced from 1000

    # API Settings (Legacy - keeping for backward compatibility)
    DHAN_SCRIP_MASTER_URL: str = os.getenv("DHAN_SCRIP_MASTER_URL", "https://images.dhan.co/api-data/api-scrip-master.csv")
    DHAN_API_HEADERS: str = os.getenv("DHAN_API_HEADERS", '{"Content-Type": "application/json", "Authorization": "Bearer YOUR_TOKEN"}')

    # Default Dates
    FROM_DATE: str = os.getenv("FROM_DATE", "2000-01-01")  # Default start date for historical data
    TO_DATE: Optional[str] = os.getenv("TO_DATE")  # Default end date (None means today)

    # Redis Configuration - Reduced for sequential processing
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    REDIS_MAX_CONNECTIONS: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "20"))  # Reduced from 100
    REDIS_CONNECTION_POOL_SIZE: int = int(os.getenv("REDIS_CONNECTION_POOL_SIZE", "10"))  # Reduced from 50

    # Celery Configuration - Optimized for sequential processing
    CELERY_TASK_SERIALIZER: str = os.getenv("CELERY_TASK_SERIALIZER", "json")
    CELERY_RESULT_SERIALIZER: str = os.getenv("CELERY_RESULT_SERIALIZER", "json")
    CELERY_ACCEPT_CONTENT: List[str] = ["json"]
    CELERY_TIMEZONE: str = "UTC"
    CELERY_ENABLE_UTC: bool = True
    CELERY_TASK_TRACK_STARTED: bool = True
    CELERY_TASK_SEND_SENT_EVENT: bool = True
    CELERY_WORKER_SEND_TASK_EVENTS: bool = True
    CELERY_RESULT_EXPIRES: int = int(os.getenv("CELERY_RESULT_EXPIRES", "7200"))  # 2 hours
    CELERY_WORKER_CONCURRENCY: int = int(os.getenv("CELERY_WORKER_CONCURRENCY", "4"))  # Reduced from 16
    CELERY_WORKER_PREFETCH_MULTIPLIER: int = int(os.getenv("CELERY_WORKER_PREFETCH_MULTIPLIER", "1"))  # Reduced from 4
    CELERY_TASK_SOFT_TIME_LIMIT: int = int(os.getenv("CELERY_TASK_SOFT_TIME_LIMIT", "7200"))  # 2 hours
    CELERY_TASK_TIME_LIMIT: int = int(os.getenv("CELERY_TASK_TIME_LIMIT", "10800"))  # 3 hours (longer for sequential)

    # Monitoring and Health Check Settings
    HEALTH_CHECK_TIMEOUT: int = int(os.getenv("HEALTH_CHECK_TIMEOUT", "5"))
    ENABLE_METRICS: bool = os.getenv("ENABLE_METRICS", "True").lower() == "true"
    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "9090"))

    # Performance Monitoring
    ENABLE_PERFORMANCE_MONITORING: bool = os.getenv("ENABLE_PERFORMANCE_MONITORING", "True").lower() == "true"
    PERFORMANCE_METRICS_INTERVAL: int = int(os.getenv("PERFORMANCE_METRICS_INTERVAL", "60"))  # seconds
    LOG_SLOW_QUERIES: bool = os.getenv("LOG_SLOW_QUERIES", "True").lower() == "true"
    SLOW_QUERY_THRESHOLD: float = float(os.getenv("SLOW_QUERY_THRESHOLD", "5.0"))  # seconds

    # Memory Management - Reduced for sequential processing
    MAX_MEMORY_USAGE_MB: int = int(os.getenv("MAX_MEMORY_USAGE_MB", "2048"))  # Reduced from 4GB to 2GB
    MEMORY_CHECK_INTERVAL: int = int(os.getenv("MEMORY_CHECK_INTERVAL", "100"))  # records
    ENABLE_MEMORY_OPTIMIZATION: bool = os.getenv("ENABLE_MEMORY_OPTIMIZATION", "True").lower() == "true"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


@lru_cache
def get_settings() -> Settings:
    """Create cached settings instance"""
    return Settings()


# Export settings for use in other modules
settings = get_settings()
