# app/services/data_fetchers/exchange_mapper.py

from typing import Dict, Tuple, Any, Optional, List
from sqlalchemy.orm import Session
from app.db.session import get_db, SessionLocal
from app.db.models.security import Security
from app.db.models.exchange import Exchange
from app.db.models.derivatives import Future
from utils.logger import get_logger

logger = get_logger(__name__)


class ExchangeMapper:
    """Maps between internal security representations and Dhan API formats with caching."""

    def __init__(self):
        """Initialize mappings and caches."""
        # Initialize exchange code mappings
        self.exchange_mapping = self._initialize_exchange_mapping()
        self.segment_mapping = self._initialize_segment_mapping()
        self.instrument_mapping = self._initialize_instrument_mapping()

        # Cache exchange information
        self.exchange_info_cache = self._initialize_exchange_info_cache()

        logger.info("Initialized exchange mapper with caches")

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

    def _initialize_exchange_info_cache(self) -> Dict[str, Dict[str, Any]]:
        """Initialize cache of exchange information."""
        cache = {}

        with get_db() as db:
            exchanges = db.query(Exchange).all()
            for exchange in exchanges:
                cache[str(exchange.id)] = {"id": str(exchange.id), "code": exchange.code, "name": exchange.name, "country": exchange.country, "timezone": exchange.timezone}

        logger.info(f"Cached information for {len(cache)} exchanges")
        return cache

    def get_dhan_request_params(self, security: Security, session: Optional[Session] = None) -> Dict[str, Any]:
        """Generate Dhan API request parameters for a security.

        This method can work with either:
        1. A security with loaded relationships
        2. A security with just IDs and the cache
        3. A security with a provided session for relationship loading

        Args:
            security: Internal security object
            session: Optional SQLAlchemy session

        Returns:
            Dict with parameters for Dhan API request
        """
        # Get external_id for Dhan's securityId
        security_id = str(security.external_id)

        # Map exchange segment and instrument type - using session or cache as needed
        exchange_segment = self.map_exchange_segment(security, session)
        instrument = self.map_instrument_type(security, session)

        # Default parameters
        params = {"securityId": security_id, "exchangeSegment": exchange_segment, "instrument": instrument, "expiryCode": 0, "oi": False}  # Default for non-derivatives  # Open interest not needed by default

        # Special handling for futures
        has_futures = False
        expiry_code = 0

        # Check if futures relationship is already loaded without triggering lazy loading
        if hasattr(security, "_sa_instance_state"):
            from sqlalchemy import inspect

            insp = inspect(security)
            if insp.attrs.futures.loaded_value is not None:
                futures = security.futures
                if futures is not None:
                    has_futures = True
                    params["oi"] = True
                    # Try to get expiry information if available
                    try:
                        if futures.expiration_date:
                            # This is a simplified conversion
                            # In a real implementation, would need proper mapping
                            expiry_code = 1
                    except Exception as e:
                        logger.warning(f"Error accessing futures expiration date: {str(e)}")

        # If we couldn't access futures directly but we have a session
        if not has_futures and session and hasattr(security, "id"):
            try:
                # Try to load futures using session
                future = session.query(Future).filter(Future.security_id == security.id).first()
                if future:
                    has_futures = True
                    params["oi"] = True

                    if future.expiration_date:
                        expiry_code = 1  # Simplified conversion
            except Exception as e:
                logger.warning(f"Error loading futures with session: {str(e)}")

        # Set expiry code if we found futures
        if has_futures:
            params["expiryCode"] = expiry_code

        return params

    def map_exchange_segment(self, security: Security, session: Optional[Session] = None) -> str:
        """Map internal security to Dhan's exchangeSegment parameter.

        Works with multiple approaches to get exchange code.

        Args:
            security: Internal security object
            session: Optional SQLAlchemy session

        Returns:
            Dhan's exchangeSegment value
        """
        exchange_code = None
        security_type = security.security_type

        # Approach 1: Check if exchange relationship is already loaded without triggering lazy loading
        if hasattr(security, "_sa_instance_state"):
            from sqlalchemy import inspect

            insp = inspect(security)
            if insp.attrs.exchange.loaded_value is not None:
                try:
                    exchange_code = security.exchange.code
                except Exception as e:
                    logger.warning(f"Error accessing exchange.code: {str(e)}")

        # Approach 2: Try using the exchange_id with cache
        if not exchange_code and hasattr(security, "exchange_id") and security.exchange_id is not None:
            exchange_id = str(security.exchange_id)
            exchange_data = self.exchange_info_cache.get(exchange_id)
            if exchange_data:
                exchange_code = exchange_data.get("code")

        # Approach 3: Try loading with session if provided
        if not exchange_code and session and hasattr(security, "exchange_id") and security.exchange_id is not None:
            try:
                exchange = session.query(Exchange).filter(Exchange.id == security.exchange_id).first()
                if exchange:
                    exchange_code = exchange.code
            except Exception as e:
                logger.warning(f"Error loading exchange with session: {str(e)}")

        # Default to NSE if we couldn't determine exchange
        if not exchange_code:
            logger.warning(f"Couldn't determine exchange for security {security.symbol}, defaulting to NSE")
            exchange_code = "NSE"

        # Mapping logic based on security type
        if security_type == "INDEX":
            idx_exchange = f"{exchange_code}_IDX"
            return self.exchange_mapping.get(idx_exchange, "NSE_IDX")

        if security_type in ["DERIVATIVE", "FUTURE", "OPTION"]:
            deriv_exchange = f"{exchange_code}_FNO"
            return self.exchange_mapping.get(deriv_exchange, "NSE_FNO")

        return self.exchange_mapping.get(exchange_code, "NSE_EQ")

    def map_instrument_type(self, security: Security, session: Optional[Session] = None) -> str:
        """Map internal security to Dhan's instrument parameter.

        Works with multiple approaches to get security type info.

        Args:
            security: Internal security object
            session: Optional SQLAlchemy session

        Returns:
            Dhan's instrument value
        """
        security_type = security.security_type

        # Special case for futures
        if security_type == "FUTURE":
            # Try multiple approaches to determine if it's an index future
            is_index_future = False

            # Approach 1: Check if futures relationship is already loaded without triggering lazy loading
            if hasattr(security, "_sa_instance_state"):
                from sqlalchemy import inspect

                insp = inspect(security)
                if insp.attrs.futures.loaded_value is not None:
                    try:
                        futures = security.futures
                        if futures is not None and hasattr(futures, "underlying") and futures.underlying is not None:
                            underlying = futures.underlying
                            if underlying.security_type == "INDEX":
                                is_index_future = True
                    except Exception as e:
                        logger.warning(f"Error checking futures underlying: {str(e)}")

            # Approach 2: Session-based loading
            if not is_index_future and session and hasattr(security, "id"):
                try:
                    future = session.query(Future).filter(Future.security_id == security.id).first()
                    if future and future.underlying_id:
                        underlying = session.query(Security).filter(Security.id == future.underlying_id).first()
                        if underlying and underlying.security_type == "INDEX":
                            is_index_future = True
                except Exception as e:
                    logger.warning(f"Error determining future type with session: {str(e)}")

            # Return appropriate future type
            if is_index_future:
                return "FUTIDX"
            else:
                return "FUTSTK"

        # Use standard mapping for other types
        return self.instrument_mapping.get(security_type, "EQUITY")
