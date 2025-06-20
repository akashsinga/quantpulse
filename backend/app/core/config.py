# backend/app/core/config.py
"""
Centralized configuration management for QuantPulse.
"""

import os
from datetime import timezone, timedelta
from pathlib import Path
from typing import List
from functools import lru_cache

from pydantic_settings import BaseSettings
from dotenv import load_dotenv, find_dotenv

# Load environment file
load_dotenv(find_dotenv())


class APPSettings(BaseSettings):
    """APP Settings"""
    APP_NAME: str = os.getenv("APP_NAME", "QuantPulse API")
    APP_DEBUG: bool = os.getenv("APP_DEBUG", False)

    class Config:
        env_prefix = "APP_"


class DatabaseSettings(BaseSettings):
    """Database configuration settings"""
    DB_URL: str = os.getenv("DB_URL", "postgresql+psycopg2://postgres:password@localhost:5432/quantpulse")
    DB_POOL_SIZE: int = os.getenv("DB_POOL_SIZE", 20)
    DB_MAX_OVERFLOW: int = os.getenv("DB_MAX_OVERFLOW", 20)
    DB_POOL_TIMEOUT: int = os.getenv("DB_POOL_TIMEOUT", 30)
    DB_POOL_RECYCLE: int = os.getenv("DB_POOL_RECYCLE", 1800)
    DB_ECHO: bool = os.getenv("DB_ECHO", False)

    class Config:
        env_prefix = "DB_"


class APISettings(BaseSettings):
    """API Configuration Settings"""
    API_V1_PREFIX: str = "/api/v1"
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:8000", "https://quantpulse.app"]

    class Config:
        env_prefix = "API_"


class AuthSettings(BaseSettings):
    """Authentication and authorization settings"""
    AUTH_SECRET_KEY: str = os.getenv("AUTH_SECRET_KEY", "quant_pulse_2025")
    AUTH_ALGORITHM: str = os.getenv("AUTH_ALGORITHM", "HS256")
    AUTH_ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24

    class Config:
        env_prefix = "AUTH_"


class ExternalAPISettings(BaseSettings):
    """External API Configuration Settings"""
    ACCESS_TOKEN: str = os.getenv("ACCESS_TOKEN", "")
    CLIENT_ID: str = os.getenv("CLIENT_ID", "")

    HISTORICAL_URL: str = os.getenv("HISTORICAL_URL", "https://api.dhan.co/v2/charts/historical")
    EOD_URL: str = os.getenv("EOD_URL", "https://api.dhan.co/v2/marketfeed/quote")


class LoggingSettings(BaseSettings):
    """Logging configuration settings"""
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_DIR: str = os.getenv("LOG_DIR", "logs")
    LOG_ROTATION: str = os.getenv("LOG_ROTATION", "500 MB")
    LOG_RETENTION: str = os.getenv("LOG_RETENTION", "30 days")
    LOG_COMPRESSION: str = os.getenv("LOG_COMPRESSION", "gz")
    ENABLE_JSON_LOGS: bool = os.getenv("ENABLE_JSON_LOGS", False)
    ENABLE_FILE_LOGS: bool = os.getenv("ENABLE_FILE_LOGS", True)


class Settings(BaseSettings):
    """Main Application Settings"""
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    INDIA_TZ: timezone = timezone(timedelta(hours=5, minutes=30))

    app: APPSettings = APPSettings()
    database: DatabaseSettings = DatabaseSettings()
    api: APISettings = APISettings()
    auth: AuthSettings = AuthSettings()
    external: ExternalAPISettings = ExternalAPISettings()
    logging: LoggingSettings = LoggingSettings()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


settings = get_settings()
