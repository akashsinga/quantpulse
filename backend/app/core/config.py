# backend/app/core/config.py
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


class DatabaseSettings(BaseSettings):
    """Database configuration settings"""
    DATABASE_URL: str = "postgresql+psycopg2://postgres:password@localhost:5432/quantpulse"

    # Pool Settings
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 20
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_RECYCLE: int = 1800
    DB_ECHO: bool = False

    class Config:
        env_prefix = "DB_"


class APISettings(BaseSettings):
    """API Configuration settings"""
    APP_NAME: str = "QuantPulse API"
    DEBUG: bool = False
    API_PREFIX: str = "/api/v1"

    # CORS
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:8000", "https://quantpulse.app"]

    class Config:
        env_prefix = "API_"


class AuthSettings(BaseSettings):
    """Authentication configuration settings"""
    SECRET_KEY: str = "quant_pulse_2025"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24

    class Config:
        env_prefix = "AUTH_"


class DhanAPISettings(BaseSettings):
    """Dhan API configuration settings"""
    # Authentication
    DHAN_ACCESS_TOKEN: str = ""
    DHAN_CLIENT_ID: str = ""

    # Endpoints
    DHAN_BASE_URL: str = "https://api.dhan.co/v2"
    DHAN_CHARTS_HISTORICAL_URL: str = f"${DHAN_BASE_URL}/charts/historical"
    DHAN_TODAY_EOD_URL: str = f"${DHAN_BASE_URL}/marketfeed/quote"

    # Timeouts and retries
    DHAN_API_TIMEOUT: float = 30.0
    DHAN_API_MAX_RETRIES: int = 3
    DHAN_API_RETRY_DELAY: float = 2.0
    DHAN_API_MAX_CONNECTIONS: int = 10

    class Config:
        env_prefix = "DHAN_"
