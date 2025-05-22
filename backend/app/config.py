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
    print(f"Loading environment from: {env_path}")
    load_dotenv(env_path)
else:
    # Fall back to auto-detecting .env file
    dotenv_path = find_dotenv()
    if dotenv_path:
        print(f"Loading environment from (auto-detected): {dotenv_path}")
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

    # Database connection
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:password@localhost:5432/quantpulse")
    ASYNC_DATABASE_URL: str = os.getenv("ASYNC_DATABASE_URL", "postgresql+asyncpg://postgres:password@localhost:5432/quantpulse")

    # Authentication
    SECRET_KEY: str = os.getenv("SECRET_KEY", "quant_pulse_2025")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours

    # OHLCV Fetcher Configuration - Enhanced with Environment Variables
    OHLCV_WORKERS: int = int(os.getenv("OHLCV_WORKERS", "16"))
    OHLCV_BATCH_SIZE: int = int(os.getenv("OHLCV_BATCH_SIZE", "200"))
    OHLCV_MAX_CONCURRENT: int = int(os.getenv("OHLCV_MAX_CONCURRENT", "50"))
    OHLCV_REQUEST_DELAY: float = float(os.getenv("OHLCV_REQUEST_DELAY", "0.01"))
    OHLCV_MAX_RETRIES: int = int(os.getenv("OHLCV_MAX_RETRIES", "3"))
    OHLCV_BULK_INSERT_SIZE: int = int(os.getenv("OHLCV_BULK_INSERT_SIZE", "1000"))
    OHLCV_CACHE_SIZE: int = int(os.getenv("OHLCV_CACHE_SIZE", "10000"))
    OHLCV_MEMORY_CHECK_INTERVAL: int = int(os.getenv("OHLCV_MEMORY_CHECK_INTERVAL", "100"))

    # Rate limiting - Enhanced
    RATE_LIMIT_SLEEP: float = float(os.getenv("RATE_LIMIT_SLEEP", "0.05"))
    API_MAX_RETRIES: int = int(os.getenv("API_MAX_RETRIES", "3"))
    RETRY_BACKOFF_FACTOR: float = float(os.getenv("RETRY_BACKOFF_FACTOR", "2.0"))
    RETRY_INITIAL_WAIT: float = float(os.getenv("RETRY_INITIAL_WAIT", "1.0"))
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "10"))
    CIRCUIT_BREAKER_TIMEOUT: int = int(os.getenv("CIRCUIT_BREAKER_TIMEOUT", "60"))

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

    # Database pool settings - Enhanced
    DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "20"))
    DB_MAX_OVERFLOW: int = int(os.getenv("DB_MAX_OVERFLOW", "20"))
    DB_POOL_TIMEOUT: int = int(os.getenv("DB_POOL_TIMEOUT", "30"))
    DB_POOL_RECYCLE: int = int(os.getenv("DB_POOL_RECYCLE", "1800"))  # 30 minutes
    DB_ECHO: bool = os.getenv("SQL_ECHO", "False").lower() == "true"
    DB_CONNECT_ARGS: dict = {
        "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "10")),
        "server_settings": {
            "application_name": "quantpulse_ohlcv_fetcher",
        },
    }

    # API Settings
    DHAN_SCRIP_MASTER_URL: str = os.getenv("DHAN_SCRIP_MASTER_URL", "https://images.dhan.co/api-data/api-scrip-master.csv")
    DHAN_CHARTS_HISTORICAL_URL: str = os.getenv("DHAN_CHARTS_HISTORICAL_URL", "https://api.dhan.co/v2/charts/historical")
    DHAN_TODAY_EOD_URL: str = os.getenv("DHAN_TODAY_EOD_URL", "https://api.dhan.co/v2/marketfeed/quote")
    DHAN_API_HEADERS: str = os.getenv("DHAN_API_HEADERS", '{"Content-Type": "application/json", "Authorization": "Bearer YOUR_TOKEN"}')

    # Default Dates
    FROM_DATE: str = os.getenv("FROM_DATE", "2000-01-01")  # Default start date for historical data
    TO_DATE: Optional[str] = os.getenv("TO_DATE")  # Default end date (None means today)

    # Redis Configuration (for future caching)
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    REDIS_MAX_CONNECTIONS: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "50"))

    # Monitoring and Health Check Settings
    HEALTH_CHECK_TIMEOUT: int = int(os.getenv("HEALTH_CHECK_TIMEOUT", "5"))
    ENABLE_METRICS: bool = os.getenv("ENABLE_METRICS", "True").lower() == "true"
    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "9090"))

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
