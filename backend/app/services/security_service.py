# backend/app/services/security_service.py
"""
Service to handle all securities-related database operations.
Uses repositories for database access and handles bulk operations.
"""

from datetime import datetime, date
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session

from app.repositories.securities import SecurityRepository, ExchangeRepository, FutureRepository
from app.models.securities import Security, Exchange
from app.models.derivatives import Future
from app.utils.logger import get_logger

logger = get_logger(__name__)


class SecurityService:
    """Service for securities database operations using repository pattern"""

    def __init__(self, db: Session):
        self.db = db
        self.security_repo = SecurityRepository(db)
        self.exchange_repo = ExchangeRepository(db)
        self.future_repo = FutureRepository(db)

    def ensure_nse_exchange(self) -> Exchange:
        """Ensure NSE exchange exists in database"""
        nse_exchange = self.exchange_repo.get_active_by_code("NSE")

        if not nse_exchange:
            nse_exchange = self.exchange_repo.create_exchange(name="National Stock Exchange of India", code="NSE", country="India", timezone="Asia/Kolkata", currency="INR", trading_hours_start="09:15", trading_hours_end="15:30", is_active=True)
            logger.info("Created NSE exchange")

        return nse_exchange

    def process_securities_batch(self, securities_data: List[Dict[str, Any]], exchange: Exchange) -> Dict[str, int]:
        """
        Process securities using repository pattern with batch optimization
        """
        stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

        if not securities_data:
            return stats

        logger.info(f"Processing {len(securities_data)} securities using repository pattern")

        for security_data in securities_data:
            try:
                # Check if security already exists by external_id
                existing_security = self.security_repo.get_by_external_id(security_data['external_id'])

                if existing_security:
                    # Update existing security
                    update_data = {
                        'symbol': security_data['symbol'],
                        'name': security_data['name'],
                        'security_type': security_data['security_type'],
                        'segment': security_data['segment'],
                        'isin': security_data.get('isin'),
                        'sector': security_data.get('sector'),
                        'industry': security_data.get('industry'),
                        'lot_size': security_data['lot_size'],
                        'tick_size': security_data['tick_size'],
                        'is_active': security_data['is_active'],
                        'is_tradeable': security_data['is_tradeable'],
                        'is_derivatives_eligible': security_data['is_derivatives_eligible'],
                        'has_options': security_data['has_options'],
                        'has_futures': security_data['has_futures'],
                    }
                    self.security_repo.update(existing_security, update_data)
                    stats['updated'] += 1

                else:
                    # Create new security using repository
                    self.security_repo.create_security(symbol=security_data['symbol'],
                                                       name=security_data['name'],
                                                       exchange_id=exchange.id,
                                                       external_id=security_data['external_id'],
                                                       security_type=security_data['security_type'],
                                                       segment=security_data['segment'],
                                                       isin=security_data.get('isin'),
                                                       sector=security_data.get('sector'),
                                                       industry=security_data.get('industry'),
                                                       lot_size=security_data['lot_size'],
                                                       tick_size=security_data['tick_size'],
                                                       is_active=security_data['is_active'],
                                                       is_tradeable=security_data['is_tradeable'],
                                                       is_derivatives_eligible=security_data['is_derivatives_eligible'],
                                                       has_options=security_data['has_options'],
                                                       has_futures=security_data['has_futures'])
                    stats['created'] += 1

            except Exception as e:
                logger.warning(f"Error processing security {security_data.get('symbol', 'unknown')}: {e}")
                stats['errors'] += 1
                continue

        logger.info(f"Securities processing completed: {stats}")
        return stats

    def process_futures_batch(self, futures_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Process futures relationships using repository pattern
        """
        stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

        if not futures_data:
            return stats

        logger.info(f"Processing {len(futures_data)} futures using repository pattern")

        # Build securities cache for efficient lookups
        securities_cache = self._build_securities_cache()

        for future_data in futures_data:
            try:
                # Find derivative security
                derivative_security = self.security_repo.get_by_external_id(future_data['external_id'])
                if not derivative_security:
                    logger.warning(f"Derivative security not found for external_id: {future_data['external_id']}")
                    stats['skipped'] += 1
                    continue

                # Find underlying security
                underlying = self._find_underlying_security(future_data, securities_cache)
                if not underlying:
                    logger.warning(f"Underlying security not found for: {future_data['underlying_symbol']}")
                    stats['skipped'] += 1
                    continue

                # Check if future relationship already exists
                existing_future = self.future_repo.get_by_security_id(derivative_security.id)

                if existing_future:
                    # Update existing future
                    update_data = {
                        'underlying_id': underlying.id,
                        'expiration_date': future_data['expiration_date'],
                        'contract_size': future_data['contract_size'],
                        'settlement_type': future_data['settlement_type'],
                        'contract_month': future_data['contract_month'],
                        'is_active': future_data['is_active'],
                        'is_tradeable': True,
                    }
                    self.future_repo.update(existing_future, update_data)
                    stats['updated'] += 1

                else:
                    # Create new future relationship
                    self.future_repo.create_future(security_id=derivative_security.id, underlying_id=underlying.id, expiration_date=future_data['expiration_date'], contract_month=future_data['contract_month'], settlement_type=future_data['settlement_type'], contract_size=future_data['contract_size'], is_active=future_data['is_active'], is_tradeable=True)
                    stats['created'] += 1

            except Exception as e:
                logger.warning(f"Error processing future: {e}")
                stats['errors'] += 1
                continue

        logger.info(f"Futures processing completed: {stats}")
        return stats

    def mark_expired_futures_inactive(self) -> int:
        """Mark futures that have expired as inactive using repository"""
        today = date.today()

        try:
            # Get expired futures using repository
            expired_futures = self.future_repo.get_expired_futures()

            count = 0
            for future in expired_futures:
                if future.is_active:
                    self.future_repo.update(future, {'is_active': False})
                    count += 1

            logger.info(f"Marked {count} expired futures as inactive")
            return count

        except Exception as e:
            logger.error(f"Error marking expired futures: {e}")
            raise

    def get_import_statistics(self) -> Dict[str, Any]:
        """Get current database statistics using repositories"""
        try:
            # Use repository count methods
            total_securities = self.security_repo.count()
            total_futures = self.future_repo.count()

            # Get active counts using repository filters
            active_securities = len(self.security_repo.get_many_by_field('is_active', True, limit=10000))
            active_futures = len(self.future_repo.get_active_futures(limit=10000))

            # Get counts by type
            stocks = len(self.security_repo.get_securities_by_type('STOCK', limit=10000))
            indices = len(self.security_repo.get_securities_by_type('INDEX', limit=10000))
            derivatives = len(self.security_repo.get_securities_by_type('DERIVATIVE', limit=10000))

            return {'total_securities': total_securities, 'active_securities': active_securities, 'total_futures': total_futures, 'active_futures': active_futures, 'securities_by_type': {'STOCK': stocks, 'INDEX': indices, 'DERIVATIVE': derivatives}}
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}

    def _build_securities_cache(self) -> Dict[str, Security]:
        """Build a cache of securities for faster future processing"""
        logger.info("Building securities cache for futures processing")

        # Get all active stocks and indices
        stocks = self.security_repo.get_securities_by_type('STOCK', active_only=True, limit=10000)
        indices = self.security_repo.get_securities_by_type('INDEX', active_only=True, limit=10000)

        cache = {}
        for sec in stocks + indices:
            cache[sec.symbol] = sec

            # Add variations for indices
            if sec.security_type == "INDEX":
                # Clean symbol variations
                clean_symbol = sec.symbol.replace(" 50", "").replace(" ", "")
                cache[clean_symbol] = sec

                # Common index mappings
                if "NIFTY" in sec.symbol.upper():
                    cache["NIFTY"] = sec
                if "BANK" in sec.symbol.upper() and "NIFTY" in sec.symbol.upper():
                    cache["BANKNIFTY"] = sec
                if "FIN" in sec.symbol.upper() and "NIFTY" in sec.symbol.upper():
                    cache["FINNIFTY"] = sec

        logger.info(f"Built securities cache with {len(cache)} entries")
        return cache

    def _find_underlying_security(self, future_data: Dict[str, Any], securities_cache: Dict[str, Security]) -> Optional[Security]:
        """Find underlying security for a future using multiple strategies"""

        # Strategy 1: Use UNDERLYING_SECURITY_ID if available
        if future_data.get('underlying_security_id'):
            underlying = self.security_repo.get_by_external_id(future_data['underlying_security_id'])
            if underlying:
                return underlying

        # Strategy 2: Use symbol from cache
        underlying_symbol = future_data.get('underlying_symbol')
        if underlying_symbol:
            underlying = securities_cache.get(underlying_symbol)
            if underlying:
                return underlying

            # Strategy 3: Direct database lookup
            underlying = self.security_repo.get_by_symbol(underlying_symbol)
            if underlying:
                return underlying

            # Strategy 4: Try common variations
            variations = []
            if underlying_symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
                variations = [f"{underlying_symbol} 50", underlying_symbol.replace("NIFTY", "NIFTY 50"), underlying_symbol]

            for variation in variations:
                underlying = securities_cache.get(variation)
                if underlying:
                    return underlying

        return None

    def update_derivatives_eligibility(self) -> Dict[str, int]:
        """Update has_futures flag for securities that have futures"""
        stats = {'updated': 0, 'errors': 0}

        try:
            # Get all active futures
            active_futures = self.future_repo.get_active_futures(limit=10000)

            # Get unique underlying security IDs
            underlying_ids = list(set(future.underlying_id for future in active_futures))

            # Update has_futures flag for underlying securities
            for underlying_id in underlying_ids:
                try:
                    security = self.security_repo.get_by_id(underlying_id)
                    if security and not security.has_futures:
                        self.security_repo.update(security, {'has_futures': True})
                        stats['updated'] += 1
                except Exception as e:
                    logger.warning(f"Error updating security {underlying_id}: {e}")
                    stats['errors'] += 1
                    continue

            logger.info(f"Updated derivatives eligibility: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Error updating derivatives eligibility: {e}")
            stats['errors'] += 1
            return stats
