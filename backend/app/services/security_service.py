# backend/app/services/security_service.py
"""
Service to handle all securities-related database operations.
Uses repositories for database access and handles bulk operations with parallel processing.
"""

from datetime import date
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from app.repositories.securities import SecurityRepository, ExchangeRepository, FutureRepository
from app.models.securities import Security, Exchange
from app.utils.logger import get_logger
from app.utils.enum import SecurityType
from app.core.database import init_database
from app.core.config import settings

logger = get_logger(__name__)


class SecurityService:
    """Service for securities database operations using repository pattern with parallel processing"""

    def __init__(self, db: Session):
        self.db = db
        self.security_repo = SecurityRepository(db)
        self.exchange_repo = ExchangeRepository(db)
        self.future_repo = FutureRepository(db)
        self._lock = threading.Lock()

    def get_active_exchange_codes(self) -> List[str]:
        """Get list of active exchange codes from database"""
        exchanges = self.exchange_repo.get_many_by_field('is_active', True)
        exchange_codes = [exchange.code for exchange in exchanges]
        logger.info(f"Found {len(exchange_codes)} active exchanges: {exchange_codes}")
        return exchange_codes

    def process_securities_batch(self, securities_data: List[Dict[str, Any]], max_workers: int = 4) -> Dict[str, int]:
        """
        Process securities using parallel processing with repository pattern
        """
        stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0, 'derivatives_created': 0, 'derivatives_updated': 0}

        if not securities_data:
            return stats

        logger.info(f"Processing {len(securities_data)} securities using parallel processing with {max_workers} workers")

        # Build exchange cache for efficient lookup
        exchange_cache = self._build_exchange_cache()

        # Split securities by exchange for better parallel processing
        securities_by_exchange = self._group_securities_by_exchange(securities_data)

        # FIRST PASS: Process securities in parallel by exchange with separate DB sessions
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks for each exchange
            future_to_exchange = {}
            for exchange_code, exchange_securities in securities_by_exchange.items():
                future = executor.submit(self._process_exchange_securities_new_session, exchange_securities, exchange_cache, exchange_code)
                future_to_exchange[future] = exchange_code

            # Collect results from all exchanges
            derivative_securities_data = []
            for future in as_completed(future_to_exchange):
                exchange_code = future_to_exchange[future]
                try:
                    exchange_stats, exchange_derivatives = future.result()

                    # Merge stats thread-safely
                    with self._lock:
                        for key in ['created', 'updated', 'skipped', 'errors']:
                            stats[key] += exchange_stats[key]

                    derivative_securities_data.extend(exchange_derivatives)
                    logger.info(f"Completed processing {exchange_code}: {exchange_stats}")

                except Exception as e:
                    logger.error(f"Error processing exchange {exchange_code}: {e}")
                    with self._lock:
                        stats['errors'] += len(securities_by_exchange[exchange_code])

        logger.info(f"First pass completed: {stats}. Starting derivative relationships with {len(derivative_securities_data)} derivatives...")

        # SECOND PASS: Process derivatives sequentially (simpler and safer)
        if derivative_securities_data:
            derivative_stats = self._process_derivatives_sequential(derivative_securities_data, exchange_cache)
            stats['derivatives_created'] = derivative_stats['created']
            stats['derivatives_updated'] = derivative_stats['updated']

        logger.info(f"Securities processing completed: {stats}")
        return stats

    def _group_securities_by_exchange(self, securities_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group securities by exchange for parallel processing"""
        securities_by_exchange = {}
        for security_data in securities_data:
            exchange_code = security_data.get('exchange_code', 'UNKNOWN')
            if exchange_code not in securities_by_exchange:
                securities_by_exchange[exchange_code] = []
            securities_by_exchange[exchange_code].append(security_data)

        logger.info(f"Grouped securities by exchange: {[(k, len(v)) for k, v in securities_by_exchange.items()]}")
        return securities_by_exchange

    def _process_exchange_securities_new_session(self, exchange_securities: List[Dict[str, Any]], exchange_cache: Dict[str, Exchange], exchange_code: str) -> Tuple[Dict[str, int], List[Dict[str, Any]]]:
        """Process securities for a single exchange with new DB session"""
        from app.core.database import DatabaseManager

        # Create a new database manager instance for this thread
        thread_db_manager = DatabaseManager(settings.database.DB_URL)

        with thread_db_manager.get_session() as thread_db:
            # Create repositories with new session
            security_repo = SecurityRepository(thread_db)

            stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
            derivative_securities_data = []

            exchange = exchange_cache.get(exchange_code)
            if not exchange:
                logger.warning(f"Exchange {exchange_code} not found in cache")
                stats['skipped'] = len(exchange_securities)
                return stats, derivative_securities_data

            for security_data in exchange_securities:
                try:
                    # Check if security already exists by external_id
                    existing_security = security_repo.get_by_external_id(security_data['external_id'])

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
                            'exchange_id': exchange.id,
                        }
                        updated_security = security_repo.update(existing_security, update_data)
                        stats['updated'] += 1
                        current_security = updated_security

                    else:
                        # Create new security using repository
                        new_security = security_repo.create_security(symbol=security_data['symbol'],
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
                        current_security = new_security

                    # Collect derivative securities with security_id for second pass
                    if self._is_future_security(security_data['security_type']):
                        derivative_data = security_data.copy()
                        derivative_data['security_id'] = current_security.id
                        derivative_securities_data.append(derivative_data)

                except Exception as e:
                    logger.warning(f"Error processing security {security_data.get('symbol', 'unknown')} in {exchange_code}: {e}")
                    stats['errors'] += 1
                    continue

            logger.info(f"Completed {exchange_code}: created={stats['created']}, updated={stats['updated']}, errors={stats['errors']}")

            return stats, derivative_securities_data

    def _process_derivatives_sequential(self, derivative_securities_data: List[Dict[str, Any]], exchange_cache: Dict[str, Exchange]) -> Dict[str, int]:
        """Process derivative relationships sequentially - now with security IDs"""
        stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

        for security_data in derivative_securities_data:
            try:
                # Use the security_id directly instead of looking up by external_id
                security_id = security_data.get('security_id')
                if not security_id:
                    logger.warning(f"No security_id found for derivative: {security_data.get('symbol', 'unknown')}")
                    stats['skipped'] += 1
                    continue

                derivative_security = self.security_repo.get_by_id(security_id)
                if not derivative_security:
                    logger.warning(f"Derivative security not found for ID: {security_id}")
                    stats['skipped'] += 1
                    continue

                # Process derivative relationship
                derivative_stats = self._process_derivative_relationship(derivative_security, security_data, exchange_cache)
                stats['created'] += derivative_stats.get('created', 0)
                stats['updated'] += derivative_stats.get('updated', 0)
                stats['skipped'] += derivative_stats.get('skipped', 0)

            except Exception as e:
                logger.warning(f"Error processing derivative relationship for {security_data.get('symbol', 'unknown')}: {e}")
                stats['errors'] += 1
                continue

        # Commit derivatives
        self.future_repo.commit()
        return stats

    def _process_derivative_relationship(self, derivative_security: Security, security_data: Dict[str, Any], exchange_cache: Dict[str, Exchange]) -> Dict[str, int]:
        """Process derivative relationship for futures"""
        stats = {'created': 0, 'updated': 0, 'skipped': 0}

        try:
            # Find underlying security
            underlying = self._find_underlying_security(security_data, exchange_cache)
            if not underlying:
                logger.warning(f"Underlying security not found for derivative {derivative_security.symbol}")
                stats['skipped'] += 1
                return stats

            # Get contract month, but handle "UNK" case
            contract_month = security_data.get('contract_month', 'UNK')

            # Skip creating Future relationship if we can't determine contract month
            if contract_month == 'UNK':
                logger.debug(f"Skipping Future relationship for {derivative_security.symbol} - unknown contract month")
                stats['skipped'] += 1
                return stats

            # Check if future relationship already exists
            existing_future = self.future_repo.get_by_security_id(derivative_security.id)

            future_data = {
                'underlying_id': underlying.id,
                'expiration_date': security_data.get('expiration_date'),
                'contract_month': contract_month,
                'settlement_type': security_data.get('settlement_type', 'CASH'),
                'contract_size': 1.0,
                'is_active': security_data['is_active'],
                'is_tradeable': security_data['is_tradeable'],
            }

            if existing_future:
                # Update existing future relationship
                self.future_repo.update(existing_future, future_data)
                stats['updated'] += 1
            else:
                # Create new future relationship
                self.future_repo.create_future(security_id=derivative_security.id, underlying_id=underlying.id, expiration_date=security_data.get('expiration_date'), contract_month=contract_month, settlement_type=security_data.get('settlement_type', 'CASH'), contract_size=1.0, is_active=security_data['is_active'], is_tradeable=security_data['is_tradeable'])
                stats['created'] += 1

        except Exception as e:
            logger.warning(f"Error processing derivative relationship for {derivative_security.symbol}: {e}")
            stats['skipped'] += 1

        return stats

    def _find_underlying_security(self, security_data: Dict[str, Any], exchange_cache: Dict[str, Exchange]) -> Optional[Security]:
        """Find underlying security for a derivative"""

        # Strategy 1: Use underlying_security_id if available (most reliable)
        underlying_security_id = security_data.get('underlying_security_id')
        if underlying_security_id:
            try:
                underlying = self.security_repo.get_by_external_id(int(underlying_security_id))
                if underlying:
                    logger.debug(f"Found underlying by security_id: {underlying.symbol}")
                    return underlying
            except (ValueError, TypeError) as e:
                logger.debug(f"Could not convert underlying_security_id to int: {underlying_security_id}, error: {e}")

        # Strategy 2: Use underlying symbol (fallback)
        underlying_symbol = security_data.get('underlying_symbol')
        if underlying_symbol:
            # Try to find by symbol in the same exchange first
            exchange_code = security_data.get('exchange_code')
            if exchange_code:
                exchange = exchange_cache.get(exchange_code)
                if exchange:
                    underlying = self.security_repo.get_by_symbol(underlying_symbol, exchange.id)
                    if underlying:
                        logger.debug(f"Found underlying by symbol in same exchange: {underlying.symbol}")
                        return underlying

            # If not found in same exchange, try any exchange
            underlying = self.security_repo.get_by_symbol(underlying_symbol)
            if underlying:
                logger.debug(f"Found underlying by symbol in any exchange: {underlying.symbol}")
                return underlying

            # Try common variations for indices
            if underlying_symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
                variations = [f"{underlying_symbol} 50", underlying_symbol.replace("NIFTY", "NIFTY 50"), underlying_symbol]

                for variation in variations:
                    underlying = self.security_repo.get_by_symbol(variation)
                    if underlying:
                        logger.debug(f"Found underlying by symbol variation: {underlying.symbol}")
                        return underlying

        logger.debug(f"Could not find underlying for derivative with underlying_symbol: {underlying_symbol}, underlying_security_id: {underlying_security_id}")
        return None

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

    def get_import_statistics(self) -> Dict[str, Any]:
        """Get current database statistics using repositories"""
        try:
            # Use repository count methods
            total_securities = self.security_repo.count()
            total_futures = self.future_repo.count()

            # Get active counts using repository filters
            active_securities = len(self.security_repo.get_many_by_field('is_active', True, limit=10000))
            active_futures = len(self.future_repo.get_active_futures(limit=10000))

            # Get counts by type - using the new SecurityType enum values
            securities_by_type = {}
            for security_type in [SecurityType.EQUITY, SecurityType.INDEX, SecurityType.FUTSTK, SecurityType.FUTIDX, SecurityType.FUTCOM, SecurityType.FUTCUR, SecurityType.OPTSTK, SecurityType.OPTIDX, SecurityType.OPTCOM, SecurityType.OPTCUR]:
                count = len(self.security_repo.get_securities_by_type(security_type.value, limit=10000))
                if count > 0:
                    securities_by_type[security_type.value] = count

            return {'total_securities': total_securities, 'active_securities': active_securities, 'total_futures': total_futures, 'active_futures': active_futures, 'securities_by_type': securities_by_type}
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}

    def _build_exchange_cache(self) -> Dict[str, Exchange]:
        """Build a cache of exchanges for faster lookup"""
        logger.info("Building exchange cache")

        exchanges = self.exchange_repo.get_all()
        cache = {exchange.code: exchange for exchange in exchanges}

        logger.info(f"Built exchange cache with {len(cache)} exchanges: {list(cache.keys())}")
        return cache

    def _is_future_security(self, security_type: str) -> bool:
        """Check if security type is a future"""
        future_types = [SecurityType.FUTSTK.value, SecurityType.FUTIDX.value, SecurityType.FUTCOM.value, SecurityType.FUTCUR.value]
        return security_type in future_types
