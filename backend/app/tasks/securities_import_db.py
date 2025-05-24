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
from .securities_import_helpers import (map_security_type, get_security_name, safe_int_conversion, extract_underlying_symbol, parse_expiry_date, get_contract_month, is_index_future)
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
        nse_exchange = Exchange(id=uuid.UUID("984ffe13-dcfb-4362-8291-5f2bee2645ef"), name="National Stock Exchange", code="NSE", country="India", timezone="Asia/Kolkata", is_active=True)
        db.add(nse_exchange)
        db.commit()
        logger.info("Created NSE exchange")

    return nse_exchange


def process_single_security(db: Session, row: Dict, nse_exchange: Exchange) -> Tuple[str, Optional[Security]]:
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
        existing = db.query(Security).filter(Security.external_id == external_id).first()

        # If not found by external_id, check by symbol+exchange
        if not existing:
            existing = db.query(Security).filter(Security.symbol == symbol, Security.exchange_id == nse_exchange.id).first()

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
            race_check = db.query(Security).filter(Security.symbol == symbol, Security.exchange_id == nse_exchange.id).first()

            if race_check:
                # Another process created it, update instead
                race_check.name = name
                race_check.security_type = security_type
                race_check.external_id = external_id
                race_check.is_active = True
                race_check.updated_at = datetime.now()
                return 'updated', race_check

            # Create new security
            new_security = Security(id=uuid.uuid4(), symbol=symbol, name=name, exchange_id=nse_exchange.id, security_type=security_type, segment="EQUITY", external_id=external_id, is_active=True)
            db.add(new_security)

            return 'created', new_security

    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Database error processing security {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}")
        return 'error', None
    except Exception as e:
        logger.error(f"Error processing security {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}")
        return 'error', None


def save_securities_batch(db: Session, securities_df, nse_exchange: Exchange, batch_size: int = 100) -> Dict:
    """
    Save securities to database in batches
    """
    stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

    logger.info(f"Saving {len(securities_df)} securities in batches of {batch_size}")

    # Use enumerate instead of iterrows
    for counter, (index, row) in enumerate(securities_df.iterrows()):
        try:
            status, security = process_single_security(db, row, nse_exchange)
            stats[status] += 1

            # Commit every batch_size records - use counter, not index
            if (counter + 1) % batch_size == 0:
                db.commit()
                logger.info(f"Processed {counter + 1}/{len(securities_df)} securities")

        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error in securities batch processing: {e}")
            continue

    # Final commit
    try:
        db.commit()
        logger.info(f"Securities batch processing completed: {stats}")
        db.expire_all()
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Final commit failed for securities: {e}")
        raise

    return stats


def process_single_future(db: Session, row: Dict, securities_cache: Dict[str, Security]) -> Tuple[str, Optional[Future]]:
    """
    Process a single future record
    """
    try:
        external_id = int(row["SEM_SMST_SECURITY_ID"])
        symbol = row["SEM_TRADING_SYMBOL"]

        # Debug: Log the future we're processing
        logger.debug(f"Processing future: {symbol} (external_id: {external_id})")

        # IMPORTANT: The future itself should already exist as a security with security_type='DERIVATIVE'
        # We created it in the securities processing step
        future_security = db.query(Security).filter(Security.external_id == external_id, Security.security_type == "DERIVATIVE").first()

        if not future_security:
            logger.debug(f"SKIP REASON: No derivative security found for external_id {external_id} (symbol: {symbol})")
            return 'skipped', None

        # Extract underlying symbol and find underlying security
        underlying_symbol = extract_underlying_symbol(symbol)
        logger.debug(f"Looking for underlying symbol: '{underlying_symbol}' for future: {symbol}")

        underlying = securities_cache.get(underlying_symbol)

        if not underlying:
            # Try database lookup if not in cache with different variations
            underlying = db.query(Security).filter(Security.symbol == underlying_symbol, Security.security_type.in_(["STOCK", "INDEX"])).first()

            # Try variations for indices
            if not underlying:
                # Try with "50" suffix for NIFTY
                if underlying_symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
                    variations = [f"{underlying_symbol} 50", f"{underlying_symbol}50", underlying_symbol.replace("NIFTY", "NIFTY 50")]
                    for variation in variations:
                        underlying = securities_cache.get(variation)
                        if underlying:
                            break

                    if not underlying:
                        # Try database lookup for variations
                        for variation in variations:
                            underlying = db.query(Security).filter(Security.symbol == variation, Security.security_type == "INDEX").first()
                            if underlying:
                                break

        if not underlying:
            logger.debug(f"SKIP REASON: No underlying found for symbol: '{underlying_symbol}' (from future: {symbol})")
            # Log available similar symbols for debugging
            similar_symbols = [k for k in securities_cache.keys() if underlying_symbol[:4] in k][:5]
            logger.debug(f"Similar available symbols: {similar_symbols}")
            return 'skipped', None

        # Parse expiry date
        expiry_date = parse_expiry_date(row["SEM_EXPIRY_DATE"])
        if not expiry_date:
            logger.debug(f"SKIP REASON: Invalid expiry date for future: {symbol}")
            return 'error', None

        # Get contract specifications
        contract_month = get_contract_month(expiry_date)
        lot_size = safe_int_conversion(row.get("SEM_LOT_UNITS"), 1)
        is_index = is_index_future(row.get("SEM_INSTRUMENT_NAME", ""))

        # Check if future already exists
        existing = db.query(Future).filter(Future.security_id == future_security.id).first()

        if existing:
            # Update existing future
            existing.underlying_id = underlying.id
            existing.expiration_date = expiry_date
            existing.lot_size = lot_size
            existing.contract_month = contract_month
            existing.settlement_type = "CASH" if is_index else "PHYSICAL"
            existing.is_active = True
            logger.debug(f"Updated future: {symbol}")
            return 'updated', existing
        else:
            # Create new future
            new_future = Future(security_id=future_security.id, underlying_id=underlying.id, expiration_date=expiry_date, contract_size=1.0, lot_size=lot_size, settlement_type="CASH" if is_index else "PHYSICAL", contract_month=contract_month, is_active=True)
            db.add(new_future)
            logger.debug(f"Created future: {symbol}")
            return 'created', new_future

    except Exception as e:
        logger.error(f"Error processing future {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}")
        return 'error', None


def process_all_securities(db: Session, securities_df, futures_df, nse_exchange: Exchange) -> Dict:
    """
    Process all securities (regular + derivative securities from futures) in one pass
    """
    stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

    # Process regular securities first
    logger.info(f"Processing {len(securities_df)} regular securities")
    for index, row in securities_df.iterrows():
        try:
            status, security = process_single_security(db, row, nse_exchange)
            stats[status] += 1

            if (index + 1) % 100 == 0:
                db.commit()
                logger.info(f"Processed {index + 1}/{len(securities_df)} regular securities")
        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error processing regular security: {e}")

    # Now process futures AS securities (create derivative security records)
    logger.info(f"Processing {len(futures_df)} futures as securities")
    for index, row in futures_df.iterrows():
        try:
            status, security = process_single_security(db, row, nse_exchange)
            stats[status] += 1

            if (index + 1) % 100 == 0:
                db.commit()
                logger.info(f"Processed {index + 1}/{len(futures_df)} futures as securities")
        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error processing future as security: {e}")

    # Final commit
    db.commit()
    logger.info(f"All securities processing completed: {stats}")
    return stats


def process_futures_relationships(db: Session, futures_df) -> Dict:
    """
    Process futures relationships (create Future records that link to existing securities)
    """
    stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

    logger.info(f"Creating futures relationships for {len(futures_df)} futures")

    # Build securities cache for faster lookups
    securities_cache = build_securities_cache(db)

    for index, row in futures_df.iterrows():
        try:
            status, future = process_single_future_relationship(db, row, securities_cache)
            stats[status] += 1

            if (index + 1) % 50 == 0:
                db.commit()
                logger.info(f"Processed {index + 1}/{len(futures_df)} futures relationships")

        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error processing future relationship: {e}")

    # Final commit
    db.commit()
    logger.info(f"Futures relationships processing completed: {stats}")
    return stats


def process_single_future_relationship(db: Session, row: Dict, securities_cache: Dict[str, Security]) -> Tuple[str, Optional[Future]]:
    """
    Create a Future relationship record (assuming the derivative security already exists)
    """
    try:
        external_id = int(row["SEM_SMST_SECURITY_ID"])
        symbol = row["SEM_TRADING_SYMBOL"]

        # Find the derivative security (should exist now)
        derivative_security = db.query(Security).filter(Security.external_id == external_id, Security.security_type == "DERIVATIVE").first()

        if not derivative_security:
            return 'skipped', None

        # Extract underlying symbol and find underlying security
        underlying_symbol = extract_underlying_symbol(symbol)
        underlying = securities_cache.get(underlying_symbol)

        if not underlying:
            # Try database lookup with variations
            underlying = db.query(Security).filter(Security.symbol == underlying_symbol, Security.security_type.in_(["STOCK", "INDEX"])).first()

            # Try variations for indices
            if not underlying and underlying_symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
                variations = [f"{underlying_symbol} 50", "FINNIFTY", "NIFTY 50"]
                for variation in variations:
                    underlying = securities_cache.get(variation)
                    if underlying:
                        break
                    underlying = db.query(Security).filter(Security.symbol == variation, Security.security_type == "INDEX").first()
                    if underlying:
                        break

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

        # Check if future relationship already exists
        existing = db.query(Future).filter(Future.security_id == derivative_security.id).first()

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
            # Create new future relationship
            new_future = Future(security_id=derivative_security.id, underlying_id=underlying.id, expiration_date=expiry_date, contract_size=1.0, lot_size=lot_size, settlement_type="CASH" if is_index else "PHYSICAL", contract_month=contract_month, is_active=True)
            db.add(new_future)
            return 'created', new_future

    except Exception as e:
        logger.error(f"Error processing future relationship {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}")
        return 'error', None


def build_securities_cache(db: Session) -> Dict[str, Security]:
    """
    Build a cache of securities for faster future processing
    """
    logger.info("Building securities cache for futures processing")

    securities = db.query(Security).filter(Security.security_type.in_(["STOCK", "INDEX"]), Security.is_active == True).all()

    cache = {}
    for sec in securities:
        cache[sec.symbol] = sec

        # Add variations for indices
        if sec.security_type == "INDEX":
            # Remove " 50" suffix and add base name
            clean_symbol = sec.symbol.replace(" 50", "").replace(" ", "")
            cache[clean_symbol] = sec

            # Add specific variations for common indices
            if "NIFTY" in sec.symbol:
                cache["NIFTY"] = sec
            if "BANK" in sec.symbol and "NIFTY" in sec.symbol:
                cache["BANKNIFTY"] = sec

        # For stocks, also add without common suffixes
        elif sec.security_type == "STOCK":
            # Handle symbols like "BAJAJ-AUTO" -> "BAJAJ"
            if "-" in sec.symbol:
                base_symbol = sec.symbol.split("-")[0]
                if base_symbol not in cache:  # Don't overwrite
                    cache[base_symbol] = sec

    logger.info(f"Built cache with {len(cache)} entries (including variations)")

    # Log some index entries for debugging
    index_entries = {k: v.symbol for k, v in cache.items() if v.security_type == "INDEX"}
    logger.info(f"Index cache entries: {index_entries}")

    return cache


def save_futures_batch(db: Session, futures_df, batch_size: int = 50) -> Dict:
    """
    Save futures to database in batches
    """
    stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

    logger.info(f"Saving {len(futures_df)} futures in batches of {batch_size}")

    # Build securities cache
    import time
    time.sleep(2)
    db.commit()
    db.expire_all()

    securities_cache = build_securities_cache(db)
    logger.info(f"Built securities cache with {len(securities_cache)} securities for futures processing")

    # Log sample future data for debugging
    if len(futures_df) > 0:
        sample_row = futures_df.iloc[0]
        logger.info(f"Sample future data: Symbol={sample_row.get('SEM_TRADING_SYMBOL')}, ExternalID={sample_row.get('SEM_SMST_SECURITY_ID')}")

    # Use enumerate instead of iterrows to get proper counter
    for counter, (index, row) in enumerate(futures_df.iterrows()):
        try:
            status, future = process_single_future(db, row, securities_cache)
            stats[status] += 1

            if status == 'skipped':
                # Log first few skipped items for debugging
                if stats['skipped'] <= 5:
                    underlying_symbol = extract_underlying_symbol(row['SEM_TRADING_SYMBOL'])
                    external_id = row.get('SEM_SMST_SECURITY_ID')
                    logger.debug(f"Future {row['SEM_TRADING_SYMBOL']} skipped - underlying: {underlying_symbol}, external_id: {external_id}")

            # Commit every batch_size records - use counter, not index
            if (counter + 1) % batch_size == 0:
                db.commit()
                logger.info(f"Processed {counter + 1}/{len(futures_df)} futures")

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
        expired_futures = db.query(Future).filter(Future.expiration_date < today, Future.is_active == True).all()

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
