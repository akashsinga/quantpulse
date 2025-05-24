# app/services/data_fetchers/data_parser.py

from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timezone
from decimal import Decimal
import uuid

from app.utils.logger import get_logger

logger = get_logger(__name__)


class OHLCVDataParser:
    """
    Parser for OHLCV data from Dhan API responses
    Handles both historical and today EOD data formats
    """

    @staticmethod
    def parse_historical_response(security_id: uuid.UUID, response_data: Dict[str, Any], source: str = "dhan_api") -> List[Dict[str, Any]]:
        """
        Parse historical data response into database-ready records
        
        Args:
            security_id: UUID of the security
            response_data: Raw response from Dhan historical API
            source: Data source identifier
            
        Returns:
            List of OHLCV records ready for database insertion
        """
        try:
            # Extract arrays from response
            opens = response_data.get('open', [])
            highs = response_data.get('high', [])
            lows = response_data.get('low', [])
            closes = response_data.get('close', [])
            volumes = response_data.get('volume', [])
            timestamps = response_data.get('timestamp', [])

            # Validate array lengths
            arrays = [opens, highs, lows, closes, volumes, timestamps]
            lengths = [len(arr) for arr in arrays]

            if len(set(lengths)) > 1:
                raise ValueError(f"Inconsistent array lengths: {lengths}")

            if not timestamps:
                logger.warning(f"No data received for security {security_id}")
                return []

            records = []
            processed_dates = set()  # Track dates to handle duplicates

            for i in range(len(timestamps)):
                try:
                    # Convert epoch timestamp to datetime
                    timestamp = int(timestamps[i])
                    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                    date_key = dt.date()

                    # Skip duplicates (same security + same date)
                    if date_key in processed_dates:
                        logger.debug(f"Skipping duplicate data for {security_id} on {date_key}")
                        continue

                    processed_dates.add(date_key)

                    # Validate OHLC relationships
                    open_price = float(opens[i])
                    high_price = float(highs[i])
                    low_price = float(lows[i])
                    close_price = float(closes[i])
                    volume = int(volumes[i]) if volumes[i] else 0

                    # Basic OHLC validation
                    if not OHLCVDataParser._validate_ohlc(open_price, high_price, low_price, close_price):
                        logger.warning(f"Invalid OHLC for {security_id} on {date_key}: O={open_price}, H={high_price}, L={low_price}, C={close_price}")
                        continue

                    record = {
                        'time': dt,
                        'security_id': security_id,
                        'open': Decimal(str(open_price)),
                        'high': Decimal(str(high_price)),
                        'low': Decimal(str(low_price)),
                        'close': Decimal(str(close_price)),
                        'volume': volume,
                        'adjusted_close': None,  # Dhan provides adjusted data by default
                        'source': source
                    }

                    records.append(record)

                except (ValueError, TypeError) as e:
                    logger.warning(f"Error parsing record {i} for security {security_id}: {e}")
                    continue

            logger.info(f"Parsed {len(records)} valid records for security {security_id} (removed {len(timestamps) - len(records)} invalid/duplicate records)")
            return records

        except Exception as e:
            logger.error(f"Error parsing historical data for security {security_id}: {e}")
            return []

    @staticmethod
    def parse_today_eod_response(response_data: Dict[str, Any], security_mapping: Dict[str, uuid.UUID], source: str = "dhan_api") -> List[Dict[str, Any]]:
        """
        Parse today EOD response into database-ready records
        
        Args:
            response_data: Raw response from Dhan today EOD API
            security_mapping: Mapping of external_id to security UUID
            source: Data source identifier
            
        Returns:
            List of OHLCV records ready for database insertion
        """
        try:
            data = response_data.get('data', {})
            records = []
            current_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

            for segment, securities in data.items():
                logger.debug(f"Processing {len(securities)} securities from segment {segment}")

                for external_id, security_data in securities.items():
                    try:
                        # Get security UUID from mapping
                        security_id = security_mapping.get(external_id)
                        if not security_id:
                            logger.warning(f"No UUID mapping found for external_id {external_id}")
                            continue

                        # Extract OHLC data
                        ohlc = security_data.get('ohlc', {})
                        volume = security_data.get('volume', 0)

                        open_price = float(ohlc.get('open', 0))
                        high_price = float(ohlc.get('high', 0))
                        low_price = float(ohlc.get('low', 0))
                        close_price = float(ohlc.get('close', 0))
                        volume = int(volume) if volume else 0

                        # Validate OHLC relationships
                        if not OHLCVDataParser._validate_ohlc(open_price, high_price, low_price, close_price):
                            logger.warning(f"Invalid OHLC for {external_id}: O={open_price}, H={high_price}, L={low_price}, C={close_price}")
                            continue

                        # Skip if all prices are zero (market not open/no trading)
                        if all(price == 0 for price in [open_price, high_price, low_price, close_price]):
                            logger.debug(f"Skipping {external_id}: all prices are zero")
                            continue

                        record = {'time': current_date, 'security_id': security_id, 'open': Decimal(str(open_price)), 'high': Decimal(str(high_price)), 'low': Decimal(str(low_price)), 'close': Decimal(str(close_price)), 'volume': volume, 'adjusted_close': None, 'source': source}

                        records.append(record)

                    except (ValueError, TypeError, KeyError) as e:
                        logger.warning(f"Error parsing today EOD data for {external_id}: {e}")
                        continue

            logger.info(f"Parsed {len(records)} valid today EOD records")
            return records

        except Exception as e:
            logger.error(f"Error parsing today EOD response: {e}")
            return []

    @staticmethod
    def _validate_ohlc(open_price: float, high_price: float, low_price: float, close_price: float) -> bool:
        """
        Validate OHLC price relationships
        
        Args:
            open_price, high_price, low_price, close_price: OHLC prices
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Basic validations
            if any(price < 0 for price in [open_price, high_price, low_price, close_price]):
                return False

            if any(price == 0 for price in [open_price, high_price, low_price, close_price]):
                return False

            # High should be >= Open, Close, Low
            if high_price < max(open_price, close_price, low_price):
                return False

            # Low should be <= Open, Close, High
            if low_price > min(open_price, close_price, high_price):
                return False

            # Additional sanity checks
            # High and Low should not be exactly the same unless it's a very special case
            if high_price == low_price and high_price not in [open_price, close_price]:
                # Allow if open/close are also the same (no trading)
                if not (open_price == close_price == high_price):
                    return False

            return True

        except (ValueError, TypeError):
            return False

    @staticmethod
    def create_security_mapping(securities: List[Any]) -> Dict[str, uuid.UUID]:
        """
        Create mapping from external_id to security UUID
        
        Args:
            securities: List of Security model objects
            
        Returns:
            Dict mapping external_id (str) to security UUID
        """
        return {str(sec.external_id): sec.id for sec in securities}

    @staticmethod
    def group_securities_by_segment(securities: List[Any]) -> Dict[str, List[str]]:
        """
        Group securities by exchange segment for batch API calls
        
        Args:
            securities: List of Security model objects
            
        Returns:
            Dict mapping segment to list of external IDs
        """
        segments = {}

        for security in securities:
            # Map security type to exchange segment
            if security.security_type == "STOCK":
                segment = "NSE_EQ"
            elif security.security_type == "INDEX":
                segment = "IDX_I"
            else:
                continue  # Skip other types

            if segment not in segments:
                segments[segment] = []

            segments[segment].append(str(security.external_id))

        return segments

    @staticmethod
    def get_instrument_type(security_type: str) -> str:
        """
        Map security type to Dhan instrument type
        
        Args:
            security_type: Security type from database
            
        Returns:
            Dhan instrument type
        """
        mapping = {"STOCK": "EQUITY", "INDEX": "INDEX"}
        return mapping.get(security_type, "EQUITY")

    @staticmethod
    def get_exchange_segment(security_type: str) -> str:
        """
        Map security type to Dhan exchange segment
        
        Args:
            security_type: Security type from database
            
        Returns:
            Dhan exchange segment
        """
        mapping = {"STOCK": "NSE_EQ", "INDEX": "IDX_I"}
        return mapping.get(security_type, "NSE_EQ")
