# backend/app/config.py

from pydantic_settings import BaseSettings
from typing import List
from functools import lru_cache
from datetime import timezone, timedelta

import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# Load environment variables
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    dotenv_path = find_dotenv()
    if dotenv_path:
        load_dotenv(dotenv_path)
    else:
        print("WARNING: No .env file found!")


class Settings(BaseSettings):
    """Application settings - minimal practical version"""

    # ==================== CORE APPLICATION ====================
    APP_NAME: str = "QuantPulse API"
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    API_V1_PREFIX: str = "/api/v1"
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:8000", "https://quantpulse.app"]

    # ==================== DATABASE ====================
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:password@localhost:5432/quantpulse")

    # Basic database settings
    DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "20"))
    DB_MAX_OVERFLOW: int = int(os.getenv("DB_MAX_OVERFLOW", "20"))
    DB_POOL_TIMEOUT: int = int(os.getenv("DB_POOL_TIMEOUT", "30"))
    DB_POOL_RECYCLE: int = int(os.getenv("DB_POOL_RECYCLE", "1800"))
    DB_ECHO: bool = os.getenv("SQL_ECHO", "False").lower() == "true"

    # ==================== AUTHENTICATION ====================
    SECRET_KEY: str = os.getenv("SECRET_KEY", "quant_pulse_2025")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours

    # ==================== DHAN API ====================
    DHAN_ACCESS_TOKEN: str = os.getenv("DHAN_ACCESS_TOKEN", "")
    DHAN_CLIENT_ID: str = os.getenv("DHAN_CLIENT_ID", "")
    DHAN_CHARTS_HISTORICAL_URL: str = os.getenv("DHAN_CHARTS_HISTORICAL_URL", "https://api.dhan.co/v2/charts/historical")
    DHAN_TODAY_EOD_URL: str = os.getenv("DHAN_TODAY_EOD_URL", "https://api.dhan.co/v2/marketfeed/quote")

    # ==================== PIPELINE CORE SETTINGS ====================

    # Processing settings
    PIPELINE_CHUNK_SIZE: int = int(os.getenv("PIPELINE_CHUNK_SIZE", "1000"))  # Records per chunk
    PIPELINE_BULK_INSERT_SIZE: int = int(os.getenv("PIPELINE_BULK_INSERT_SIZE", "5000"))  # Bulk insert size

    # Rate limiting (keep it simple)
    API_RATE_LIMIT_PER_SECOND: float = float(os.getenv("API_RATE_LIMIT_PER_SECOND", "5.0"))  # 5 req/sec

    # Gap detection (essential settings only)
    GAP_DETECTION_ENABLED: bool = os.getenv("GAP_DETECTION_ENABLED", "True").lower() == "true"
    AUTO_FILL_MAX_DAYS: int = int(os.getenv("AUTO_FILL_MAX_DAYS", "365"))  # Auto-fill gaps up to 1 year

    # Data quality (basic)
    DATA_QUALITY_ENABLED: bool = os.getenv("DATA_QUALITY_ENABLED", "True").lower() == "true"
    DATA_QUALITY_MIN_SCORE: float = float(os.getenv("DATA_QUALITY_MIN_SCORE", "0.8"))

    # ==================== REDIS & CELERY ====================
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Celery basics
    CELERY_TASK_SERIALIZER: str = "json"
    CELERY_RESULT_SERIALIZER: str = "json"
    CELERY_ACCEPT_CONTENT: List[str] = ["json"]
    CELERY_TIMEZONE: str = "UTC"
    CELERY_ENABLE_UTC: bool = True
    CELERY_TASK_TRACK_STARTED: bool = True
    CELERY_RESULT_EXPIRES: int = int(os.getenv("CELERY_RESULT_EXPIRES", "7200"))  # 2 hours
    CELERY_WORKER_CONCURRENCY: int = int(os.getenv("CELERY_WORKER_CONCURRENCY", "4"))
    CELERY_WORKER_PREFETCH_MULTIPLIER: int = int(os.getenv("CELERY_WORKER_PREFETCH_MULTIPLIER", "1"))
    CELERY_TASK_SOFT_TIME_LIMIT: int = int(os.getenv("CELERY_TASK_SOFT_TIME_LIMIT", "7200"))  # 2 hours
    CELERY_TASK_TIME_LIMIT: int = int(os.getenv("CELERY_TASK_TIME_LIMIT", "10800"))  # 3 hours

    # ==================== MEMORY & PERFORMANCE ====================
    MAX_MEMORY_USAGE_MB: int = int(os.getenv("MAX_MEMORY_USAGE_MB", "2048"))  # 2GB limit

    # ==================== LOGGING ====================
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # ==================== TIMEZONE ====================
    INDIA_TZ: timezone = timezone(timedelta(hours=5, minutes=30))

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


@lru_cache
def get_settings() -> Settings:
    """Create cached settings instance"""
    return Settings()


# Export settings
settings = get_settings()
