# app/core/config.py
"""
Centralized configuration management for QuantPulse.
"""

import os
from datetime import timezone, timedelta
from pathlib import Path
from typing import List, Optional
from functools import lru_cache

from pydantic_settings import BaseSettings
from dotenv import load_dotenv, find_dotenv

# Load environment variables
env_path = Path(__file__).resolve().parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    dotenv_path = find_dotenv()
    if dotenv_path:
        load_dotenv(dotenv_path)


class DatabaseSettings(BaseSettings):
    """Database configuration settings."""

    # Connection
    DATABASE_URL: str = "postgresql+psycopg2://postgres:password@localhost:5432/quantpulse"

    # Pool settings
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 20
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_RECYCLE: int = 1800
    DB_ECHO: bool = False

    class Config:
        env_prefix = "DB_"


class APISettings(BaseSettings):
    """API configuration settings."""

    # Application
    APP_NAME: str = "QuantPulse API"
    DEBUG: bool = False
    API_V1_PREFIX: str = "/api/v1"

    # CORS
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:8000", "https://quantpulse.app"]

    class Config:
        env_prefix = "API_"


class AuthSettings(BaseSettings):
    """Authentication configuration settings."""

    SECRET_KEY: str = "quant_pulse_2025"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours

    class Config:
        env_prefix = "AUTH_"


class DhanAPISettings(BaseSettings):
    """Dhan API configuration settings."""

    # Authentication
    DHAN_ACCESS_TOKEN: str = ""
    DHAN_CLIENT_ID: str = ""

    # Endpoints
    DHAN_BASE_URL: str = "https://api.dhan.co/v2"
    DHAN_CHARTS_HISTORICAL_URL: str = "https://api.dhan.co/v2/charts/historical"
    DHAN_TODAY_EOD_URL: str = "https://api.dhan.co/v2/marketfeed/quote"

    # Rate limits
    DHAN_HISTORICAL_RATE_LIMIT: float = 5.0
    DHAN_QUOTE_RATE_LIMIT: float = 1.0
    DHAN_HISTORICAL_DAILY_LIMIT: int = 100_000

    # Timeouts and retries
    DHAN_API_TIMEOUT: float = 30.0
    DHAN_API_MAX_RETRIES: int = 3
    DHAN_API_RETRY_DELAY: float = 2.0
    DHAN_API_MAX_CONNECTIONS: int = 10

    class Config:
        env_prefix = "DHAN_"


class DataPipelineSettings(BaseSettings):
    """Data pipeline configuration settings."""

    # Backfill settings
    HISTORICAL_BACKFILL_BATCH_SIZE: int = 50
    HISTORICAL_BACKFILL_MAX_CONCURRENT: int = 5
    HISTORICAL_BACKFILL_CHUNK_SIZE: int = 1000
    HISTORICAL_DATA_START_DATE: str = "2000-01-01"
    HISTORICAL_DATA_QUALITY_THRESHOLD: float = 0.8

    # Processing settings
    PIPELINE_CHUNK_SIZE: int = 1000
    PIPELINE_BULK_INSERT_SIZE: int = 5000
    API_RATE_LIMIT_PER_SECOND: float = 5.0

    # Gap detection
    GAP_DETECTION_ENABLED: bool = True
    AUTO_FILL_MAX_DAYS: int = 365

    # Data quality
    DATA_QUALITY_ENABLED: bool = True
    DATA_QUALITY_MIN_SCORE: float = 0.8
    OHLCV_VALIDATION_ENABLED: bool = True

    # Memory limits
    HISTORICAL_BACKFILL_MEMORY_LIMIT_MB: int = 4096
    MAX_MEMORY_USAGE_MB: int = 2048

    class Config:
        env_prefix = "PIPELINE_"


class CelerySettings(BaseSettings):
    """Celery configuration settings."""

    REDIS_URL: str = "redis://localhost:6379/0"

    # Serialization
    CELERY_TASK_SERIALIZER: str = "json"
    CELERY_RESULT_SERIALIZER: str = "json"
    CELERY_ACCEPT_CONTENT: List[str] = ["json"]

    # Timezone
    CELERY_TIMEZONE: str = "UTC"
    CELERY_ENABLE_UTC: bool = True

    # Task settings
    CELERY_TASK_TRACK_STARTED: bool = True
    CELERY_RESULT_EXPIRES: int = 7200  # 2 hours

    # Worker settings
    CELERY_WORKER_CONCURRENCY: int = 4
    CELERY_WORKER_PREFETCH_MULTIPLIER: int = 1
    CELERY_TASK_SOFT_TIME_LIMIT: int = 7200  # 2 hours
    CELERY_TASK_TIME_LIMIT: int = 10800  # 3 hours

    class Config:
        env_prefix = "CELERY_"


class LoggingSettings(BaseSettings):
    """Logging configuration settings."""

    LOG_LEVEL: str = "INFO"
    LOG_DIR: str = "logs"
    LOG_ROTATION: str = "500 MB"
    LOG_RETENTION: str = "30 days"
    LOG_COMPRESSION: str = "gz"
    ENABLE_JSON_LOGS: bool = False
    ENABLE_FILE_LOGS: bool = True

    class Config:
        env_prefix = "LOG_"


class Settings(BaseSettings):
    """Main application settings."""

    # Environment
    ENVIRONMENT: str = "development"

    # Timezone
    INDIA_TZ: timezone = timezone(timedelta(hours=5, minutes=30))

    # Component settings
    database: DatabaseSettings = DatabaseSettings()
    api: APISettings = APISettings()
    auth: AuthSettings = AuthSettings()
    dhan: DhanAPISettings = DhanAPISettings()
    pipeline: DataPipelineSettings = DataPipelineSettings()
    celery: CelerySettings = CelerySettings()
    logging: LoggingSettings = LoggingSettings()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Override from environment variables
        self._load_environment_overrides()

    def _load_environment_overrides(self):
        """Load environment-specific overrides."""
        # Override debug mode
        if os.getenv("DEBUG", "").lower() == "true":
            self.api.DEBUG = True
            self.database.DB_ECHO = True

        # Override database URL from environment
        if db_url := os.getenv("DATABASE_URL"):
            self.database.DATABASE_URL = db_url

        # Override Redis URL from environment
        if redis_url := os.getenv("REDIS_URL"):
            self.celery.REDIS_URL = redis_url

    @property
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.ENVIRONMENT.lower() in ("development", "dev")

    @property
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.ENVIRONMENT.lower() in ("production", "prod")

    def get_database_url(self) -> str:
        """Get the database URL."""
        return self.database.DATABASE_URL

    def get_redis_url(self) -> str:
        """Get the Redis URL."""
        return self.celery.REDIS_URL


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Export settings instance
settings = get_settings()
