# backend/app/tasks/securities_import_helpers.py

import os
import sys

import pandas as pd
from datetime import datetime, date
from typing import Dict, Tuple, Optional
from app.utils.logger import get_logger

logger = get_logger(__name__)


def filter_securities_and_futures(
        df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filter and separate securities and futures from raw Dhan data
    
    Returns:
        Tuple of (securities_df, futures_df)
    """
    logger.info(
        f"Filtering {len(df)} total records for NSE securities and futures")

    # Filter for NSE exchange only
    nse_df = df[df["SEM_EXM_EXCH_ID"] == "NSE"]

    if len(nse_df) == 0:
        logger.warning("No NSE securities found in the data")
        return pd.DataFrame(), pd.DataFrame()

    # Filter main securities (stocks and indices)
    stocks_df = nse_df[(nse_df["SEM_SEGMENT"] == "E")
                       & (nse_df["SEM_EXCH_INSTRUMENT_TYPE"] == "ES")]

    indices_df = nse_df[(nse_df["SEM_SEGMENT"] == "I")
                        & (nse_df["SEM_EXCH_INSTRUMENT_TYPE"] == "INDEX")]

    # Combine stocks and indices
    securities_df = pd.concat([stocks_df, indices_df], ignore_index=True)

    # Filter futures
    futures_df = nse_df[(nse_df["SEM_SEGMENT"] == "D") & (
        nse_df["SEM_INSTRUMENT_NAME"].isin(["FUTSTK", "FUTIDX"]))]

    logger.info(
        f"Filtered {len(stocks_df)} stocks, {len(indices_df)} indices, {len(futures_df)} futures"
    )

    return securities_df, futures_df


def map_security_type(row: Dict) -> str:
    """Map Dhan security segment to our security type"""
    if row["SEM_SEGMENT"] == "E":
        return "STOCK"
    elif row["SEM_SEGMENT"] == "I":
        return "INDEX"
    elif row["SEM_SEGMENT"] == "D":
        return "DERIVATIVE"
    else:
        return "STOCK"  # Default


def extract_underlying_symbol(futures_symbol: str) -> str:
    """
    Extract underlying symbol from futures trading symbol
    
    Examples:
    - "NIFTY-Jun2025-FUT" -> "NIFTY"
    - "RELIANCE-Mar2025-FUT" -> "RELIANCE"
    """
    parts = futures_symbol.split("-")
    return parts[0] if parts else futures_symbol


def parse_expiry_date(expiry_string: str) -> Optional[date]:
    """
    Parse expiry date from various formats in Dhan data
    
    Args:
        expiry_string: Date string from SEM_EXPIRY_DATE field
        
    Returns:
        date object or None if parsing fails
    """
    if not expiry_string or pd.isna(expiry_string):
        return None

    try:
        # Handle "YYYY-MM-DD HH:MM:SS" format
        if " " in expiry_string:
            date_part = expiry_string.split()[0]
        else:
            date_part = expiry_string

        # Parse YYYY-MM-DD format
        return datetime.strptime(date_part, "%Y-%m-%d").date()

    except (ValueError, AttributeError) as e:
        logger.warning(f"Failed to parse expiry date '{expiry_string}': {e}")
        return None


def get_contract_month(expiry_date: date) -> str:
    """Get contract month from expiry date (JAN, FEB, etc.)"""
    return expiry_date.strftime("%b").upper()


def is_index_future(instrument_name: str) -> bool:
    """Check if future is index-based"""
    return str(instrument_name).startswith("FUTIDX")


def safe_int_conversion(value, default: int = 1) -> int:
    """Safely convert value to integer with default"""
    try:
        if pd.isna(value):
            return default
        return int(float(value))
    except (ValueError, TypeError):
        return default


def get_security_name(row: Dict) -> str:
    """
    Get the best available name for a security with priority:
    1. SEM_CUSTOM_SYMBOL (for derivatives)
    2. SM_SYMBOL_NAME 
    3. SEM_TRADING_SYMBOL (fallback)
    """
    # For derivatives, prefer custom symbol
    if row.get("SEM_SEGMENT") == "D" and pd.notna(
            row.get("SEM_CUSTOM_SYMBOL")):
        return row["SEM_CUSTOM_SYMBOL"]

    # For regular securities, prefer symbol name
    if pd.notna(row.get("SM_SYMBOL_NAME")):
        return row["SM_SYMBOL_NAME"]

    # Fallback to trading symbol
    return row["SEM_TRADING_SYMBOL"]


def validate_security_data(row: Dict) -> Tuple[bool, str]:
    """
    Validate required fields for security creation
    
    Returns:
        (is_valid, error_message)
    """
    required_fields = [
        "SEM_SMST_SECURITY_ID", "SEM_TRADING_SYMBOL", "SEM_EXM_EXCH_ID"
    ]

    for field in required_fields:
        if not row.get(field) or pd.isna(row.get(field)):
            return False, f"Missing required field: {field}"

    # Validate external ID is numeric
    try:
        int(row["SEM_SMST_SECURITY_ID"])
    except (ValueError, TypeError):
        return False, f"Invalid external ID: {row['SEM_SMST_SECURITY_ID']}"

    return True, ""


def validate_futures_data(row: Dict) -> Tuple[bool, str]:
    """
    Validate required fields for futures creation
    
    Returns:
        (is_valid, error_message)
    """
    # Basic security validation first
    is_valid, error = validate_security_data(row)
    if not is_valid:
        return is_valid, error

    # Futures-specific validation
    if not row.get("SEM_EXPIRY_DATE") or pd.isna(row.get("SEM_EXPIRY_DATE")):
        return False, "Missing expiry date for future"

    # Validate expiry date can be parsed
    expiry_date = parse_expiry_date(row["SEM_EXPIRY_DATE"])
    if not expiry_date:
        return False, f"Invalid expiry date: {row['SEM_EXPIRY_DATE']}"

    # Check if future has expired (more than 1 day ago)
    if expiry_date < date.today():
        return False, f"Future has expired: {expiry_date}"

    return True, ""
