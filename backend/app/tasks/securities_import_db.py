# backend/app/tasks/securities_import_db.py

import os
import sys

import uuid
from datetime import datetime, date
from typing import Dict, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.db.models.security import Security
from app.db.models.exchange import Exchange
from app.db.models.derivatives import Future
from .securities_import_helpers import (map_security_type, get_security_name,
                                        safe_int_conversion,
                                        extract_underlying_symbol,
                                        parse_expiry_date, get_contract_month,
                                        is_index_future)
from app.utils.logger import get_logger

logger = get_logger(__name__)


def ensure_nse_exchange(db: Session) -> Exchange:
    """
    Ensure NSE exchange exists in database
    
    Returns:
        Exchange object for NSE
    """
    nse_exchange = db.query(Exchange).filter(Exchange.code == "NSE").first()

    if not nse_exchange:
        # Create NSE exchange with fixed UUID for consistency
        nse_exchange = Exchange(
            id=uuid.UUID("984ffe13-dcfb-4362-8291-5f2bee2645ef"),
            name="National Stock Exchange",
            code="NSE",
            country="India",
            timezone="Asia/Kolkata",
            is_active=True)
        db.add(nse_exchange)
        db.commit()
        logger.info("Created NSE exchange")

    return nse_exchange


def process_single_security(
        db: Session, row: Dict,
        nse_exchange: Exchange) -> Tuple[str, Optional[Security]]:
    """
    Process a single security record
    
    Returns:
        (status, security_object) where status is 'created', 'updated', 'skipped', or 'error'
    """
    try:
        external_id = int(row["SEM_SMST_SECURITY_ID"])
        symbol = row["SEM_TRADING_SYMBOL"]
        name = get_security_name(row)
        security_type = map_security_type(row)

        # Check if security already exists (prefer external_id lookup)
        existing = db.query(Security).filter(
            Security.external_id == external_id).first()

        # If not found by external_id, check by symbol+exchange
        if not existing:
            existing = db.query(Security).filter(
                Security.symbol == symbol,
                Security.exchange_id == nse_exchange.id).first()

        if existing:
            # Update existing security
            existing.symbol = symbol
            existing.name = name
            existing.security_type = security_type
            existing.external_id = external_id  # Ensure external_id is up to date
            existing.is_active = True
            existing.updated_at = datetime.now()

            return 'updated', existing
        else:
            # Double-check to avoid race conditions
            race_check = db.query(Security).filter(
                Security.symbol == symbol,
                Security.exchange_id == nse_exchange.id).first()

            if race_check:
                # Another process created it, update instead
                race_check.name = name
                race_check.security_type = security_type
                race_check.external_id = external_id
                race_check.is_active = True
                race_check.updated_at = datetime.now()
                return 'updated', race_check

            # Create new security
            new_security = Security(id=uuid.uuid4(),
                                    symbol=symbol,
                                    name=name,
                                    exchange_id=nse_exchange.id,
                                    security_type=security_type,
                                    segment="EQUITY",
                                    external_id=external_id,
                                    is_active=True)
            db.add(new_security)

            return 'created', new_security

    except SQLAlchemyError as e:
        db.rollback()
        logger.error(
            f"Database error processing security {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}"
        )
        return 'error', None
    except Exception as e:
        logger.error(
            f"Error processing security {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}"
        )
        return 'error', None


def save_securities_batch(db: Session,
                          securities_df,
                          nse_exchange: Exchange,
                          batch_size: int = 100) -> Dict:
    """
    Save securities to database in batches
    
    Returns:
        Statistics dictionary
    """
    stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

    logger.info(
        f"Saving {len(securities_df)} securities in batches of {batch_size}")

    for index, row in securities_df.iterrows():
        try:
            status, security = process_single_security(db, row, nse_exchange)
            stats[status] += 1

            # Commit every batch_size records
            if (index + 1) % batch_size == 0:
                db.commit()
                logger.info(
                    f"Processed {index + 1}/{len(securities_df)} securities")

        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error in securities batch processing: {e}")
            continue

    # Final commit
    try:
        db.commit()
        logger.info(f"Securities batch processing completed: {stats}")
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Final commit failed for securities: {e}")
        raise

    return stats


def process_single_future(
        db: Session, row: Dict,
        securities_cache: Dict[str, Security]) -> Tuple[str, Optional[Future]]:
    """
    Process a single future record
    
    Args:
        db: Database session
        row: Future data row
        securities_cache: Cache of {symbol: Security} for faster lookups
        
    Returns:
        (status, future_object) where status is 'created', 'updated', 'skipped', or 'error'
    """
    try:
        external_id = int(row["SEM_SMST_SECURITY_ID"])
        symbol = row["SEM_TRADING_SYMBOL"]

        # Find the security record for this future
        security = db.query(Security).filter(
            Security.external_id == external_id).first()
        if not security:
            return 'skipped', None

        # Extract underlying symbol and find underlying security
        underlying_symbol = extract_underlying_symbol(symbol)
        underlying = securities_cache.get(underlying_symbol)

        if not underlying:
            # Try database lookup if not in cache
            underlying = db.query(Security).filter(
                Security.symbol == underlying_symbol,
                Security.security_type.in_(["STOCK", "INDEX"])).first()

        if not underlying:
            return 'skipped', None

        # Parse expiry date
        expiry_date = parse_expiry_date(row["SEM_EXPIRY_DATE"])
        if not expiry_date:
            return 'error', None

        # Get contract specifications
        contract_month = get_contract_month(expiry_date)
        lot_size = safe_int_conversion(row.get("SEM_LOT_UNITS"), 1)
        is_index = is_index_future(row.get("SEM_INSTRUMENT_NAME", ""))

        # Check if future already exists
        existing = db.query(Future).filter(
            Future.security_id == security.id).first()

        if existing:
            # Update existing future
            existing.underlying_id = underlying.id
            existing.expiration_date = expiry_date
            existing.lot_size = lot_size
            existing.contract_month = contract_month
            existing.settlement_type = "CASH" if is_index else "PHYSICAL"
            existing.is_active = True

            return 'updated', existing
        else:
            # Create new future
            new_future = Future(
                security_id=security.id,
                underlying_id=underlying.id,
                expiration_date=expiry_date,
                contract_size=1.0,  # Default contract size
                lot_size=lot_size,
                settlement_type="CASH" if is_index else "PHYSICAL",
                contract_month=contract_month,
                is_active=True)
            db.add(new_future)

            return 'created', new_future

    except Exception as e:
        logger.error(
            f"Error processing future {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}"
        )
        return 'error', None


def build_securities_cache(db: Session) -> Dict[str, Security]:
    """
    Build a cache of securities for faster future processing
    
    Returns:
        Dictionary mapping symbol to Security object
    """
    logger.info("Building securities cache for futures processing")

    securities = db.query(Security).filter(
        Security.security_type.in_(["STOCK", "INDEX"]),
        Security.is_active == True).all()

    cache = {sec.symbol: sec for sec in securities}
    logger.info(f"Built cache with {len(cache)} securities")

    return cache


def save_futures_batch(db: Session, futures_df, batch_size: int = 50) -> Dict:
    """
    Save futures to database in batches
    
    Returns:
        Statistics dictionary
    """
    stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

    logger.info(f"Saving {len(futures_df)} futures in batches of {batch_size}")

    # Build securities cache for faster lookups
    securities_cache = build_securities_cache(db)

    for index, row in futures_df.iterrows():
        try:
            status, future = process_single_future(db, row, securities_cache)
            stats[status] += 1

            # Commit every batch_size records
            if (index + 1) % batch_size == 0:
                db.commit()
                logger.info(f"Processed {index + 1}/{len(futures_df)} futures")

        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error in futures batch processing: {e}")
            continue

    # Final commit
    try:
        db.commit()
        logger.info(f"Futures batch processing completed: {stats}")
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Final commit failed for futures: {e}")
        raise

    return stats


def mark_expired_futures(db: Session) -> int:
    """
    Mark futures that have expired as inactive
    
    Returns:
        Number of futures marked as inactive
    """
    today = date.today()
    marked_count = 0

    try:
        # Get expired but still active futures
        expired_futures = db.query(Future).filter(
            Future.expiration_date < today, Future.is_active == True).all()

        # Mark them as inactive
        for future in expired_futures:
            future.is_active = False
            marked_count += 1

        db.commit()
        logger.info(f"Marked {marked_count} expired futures as inactive")

    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error marking expired futures: {e}")
        raise

    return marked_count
