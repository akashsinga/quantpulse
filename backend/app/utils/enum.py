# backend/app/utils/enum.py
"""
Enums used across the application.
"""

from enum import Enum as PythonEnum


class Timeframe(str, PythonEnum):
    """Timeframe constants"""
    HOURLY = "1H"
    DAILY = "1D"
    WEEKLY = "1W"
    MONTHLY = "1M"
