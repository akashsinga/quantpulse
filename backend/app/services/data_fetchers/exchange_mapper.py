# app/services/data_fetchers/exchange_mapper.py

from typing import Dict, Tuple, Any, Optional
from app.db.models.security import Security
from utils.logger import get_logger

logger = get_logger(__name__)


class ExchangeMapper:
    """Maps between internal security representations and Dhan API formats."""

    def __init__(self):
        """Initialize mappings between internal and external exchange codes."""
        # Initialize exchange code mappings
        self.exchange_mapping = self._initialize_exchange_mapping()
        self.segment_mapping = self._initialize_segment_mapping()
        self.instrument_mapping = self._initialize_instrument_mapping()

        logger.info("Initialized exchange and segment mappings")

    def _initialize_exchange_mapping(self) -> Dict[str, str]:
        """Initialize mapping from internal exchange codes to Dhan's format."""
        return {
            # Internal code -> Dhan code
            "NSE": "NSE_EQ",  # NSE Equity
            "BSE": "BSE_EQ",  # BSE Equity
            "NSE_IDX": "NSE_IDX",  # NSE Index
            "BSE_IDX": "BSE_IDX",  # BSE Index
            "NSE_FNO": "NSE_FNO",  # NSE F&O
            "NSE_CDS": "NSE_CDS",  # NSE Currency Derivatives
            "MCX": "MCX_FO",  # MCX Futures & Options
            "NSE_COM": "NSE_COM",  # NSE Commodity
        }

    def _initialize_segment_mapping(self) -> Dict[str, str]:
        """Initialize mapping from internal segment types to Dhan's format."""
        return {
            # Internal segment -> Dhan segment
            "EQUITY": "E",
            "DERIVATIVE": "D",
            "CURRENCY": "C",
            "COMMODITY": "O",
            "INDEX": "I",
        }

    def _initialize_instrument_mapping(self) -> Dict[str, str]:
        """Initialize mapping from internal security types to Dhan instrument types."""
        return {
            # Internal type -> Dhan instrument type
            "STOCK": "EQUITY",
            "INDEX": "INDXX",
            "FUTURE": "FUTIDX",  # Index futures - will be refined by security subtype
            "OPTION": "OPTIDX",  # Index options - will be refined by security subtype
            "DERIVATIVE": "FUTSTK",  # Stock futures - default, will be refined
        }

    def map_exchange_segment(self, security: Security) -> str:
        """Map internal security to Dhan's exchangeSegment parameter.

        Args:
            security: Internal security object

        Returns:
            Dhan's exchangeSegment value
        """
        # Get internal exchange code and security type
        internal_exchange = security.exchange.code
        security_type = security.security_type

        # For indices, use the index-specific exchange code
        if security_type == "INDEX":
            idx_exchange = f"{internal_exchange}_IDX"
            return self.exchange_mapping.get(idx_exchange, "NSE_IDX")  # Default to NSE_IDX

        # For derivatives, use F&O segment
        if security_type in ["DERIVATIVE", "FUTURE", "OPTION"]:
            deriv_exchange = f"{internal_exchange}_FNO"
            return self.exchange_mapping.get(deriv_exchange, "NSE_FNO")  # Default to NSE_FNO

        # For regular securities, use the standard mapping
        return self.exchange_mapping.get(internal_exchange, "NSE_EQ")  # Default to NSE_EQ

    def map_instrument_type(self, security: Security) -> str:
        """Map internal security to Dhan's instrument parameter.

        Args:
            security: Internal security object

        Returns:
            Dhan's instrument value
        """
        # Get internal security type
        security_type = security.security_type

        # Special case for futures - differentiate between index and stock futures
        if security_type == "FUTURE":
            # If this is a future with an index as underlying, use FUTIDX
            if hasattr(security, "futures") and security.futures:
                underlying = security.futures.underlying
                if underlying and underlying.security_type == "INDEX":
                    return "FUTIDX"
                else:
                    return "FUTSTK"  # Stock futures

        # Use the standard mapping with default to EQUITY
        return self.instrument_mapping.get(security_type, "EQUITY")

    def get_dhan_request_params(self, security: Security) -> Dict[str, Any]:
        """Generate Dhan API request parameters for a security.

        Args:
            security: Internal security object

        Returns:
            Dict with parameters for Dhan API request
        """
        # Get external_id for Dhan's securityId
        security_id = str(security.external_id)

        # Map exchange segment and instrument type
        exchange_segment = self.map_exchange_segment(security)
        instrument = self.map_instrument_type(security)

        # Default parameters
        params = {"securityId": security_id, "exchangeSegment": exchange_segment, "instrument": instrument, "expiryCode": 0, "oi": False}  # Default for non-derivatives  # Open interest not needed by default

        # Special handling for futures
        if hasattr(security, "futures") and security.futures:
            # Set open interest flag for futures
            params["oi"] = True

            # Handle expiry code if available
            # In a real implementation, this would map to Dhan's expiry code format
            expiry_date = security.futures.expiration_date
            if expiry_date:
                # Simple example - in practice would need proper mapping
                params["expiryCode"] = 1  # Placeholder

        return params
