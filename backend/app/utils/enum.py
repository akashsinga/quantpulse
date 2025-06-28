# backend/app/utils/enum.py
"""
Enums used across the application.
"""

from enum import Enum as PythonEnum
from re import L


class Timeframe(str, PythonEnum):
    """Timeframe constants"""
    HOURLY = "1H"
    DAILY = "1D"
    WEEKLY = "1W"
    MONTHLY = "1M"


class SecurityType(str, PythonEnum):
    """Enumeration of security types."""
    STOCK = "STOCK"
    INDEX = "INDEX"
    DERIVATIVE = "DERIVATIVE"
    ETF = "ETF"
    BOND = "BOND"


class SettlementType(str, PythonEnum):
    CASH = "CASH"
    PHYSICAL = "PHYSICAL"


class SecuritySegment(str, PythonEnum):
    """Enumeration of security segments."""
    EQUITY = "EQUITY"
    DERIVATIVE = "DERIVATIVE"
    CURRENCY = "CURRENCY"
    COMMODITY = "COMMODITY"
    INDEX = "INDEX"


class DerivativeType(str, PythonEnum):
    """Type of derivative instrument."""
    FUTURE = "FUTURE"
    OPTION = "OPTION"


class ExpiryMonth(str, PythonEnum):
    """Expiry months for derivatives"""
    JAN = "JAN"
    FEB = "FEB"
    MAR = "MAR"
    APR = "APR"
    MAY = "MAY"
    JUN = "JUN"
    JUL = "JUL"
    AUG = "AUG"
    SEPT = "SEPT"
    OCT = "OCT"
    NOV = "NOV"
    DEC = "DEC"


# Celery Enums
class TaskStatus(str, PythonEnum):
    """Task execution status enumeration"""
    PENDING = "PENDING"
    RECEIVED = "RECEIVED"
    STARTED = "STARTED"
    PROGRESS = "PROGRESS"
    PROCESSING = "PROCESSING"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRY = "RETRY"
    REVOKED = "REVOKED"
    CANCELLED = "CANCELLED"


class TaskType(str, PythonEnum):
    """Task type enumeration"""
    SECURITIES_IMPORT = "SECURITIES_IMPORT"
    DATA_ENRICHMENT = "DATA_ENRICHMENT"
    SYSTEM_MAINTENANCE = "SYSTEM_MAINTENANCE"
    CSV_PROCESSING = "CSV_PROCESSING"
    API_ENRICHMENT = "API_ENRICHMENT"
