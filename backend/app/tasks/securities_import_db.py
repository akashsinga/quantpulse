# backend/app/tasks/securities_import_db.py

import os
import sys

import uuid
import pandas as pd
from datetime import datetime, date
from typing import Dict, Optional, Tuple, Set
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import and_

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
    Process a single security record with proper error handling
    
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
            # Create new security
            new_security = Security(id=uuid.uuid4(), symbol=symbol, name=name, exchange_id=nse_exchange.id, security_type=security_type, segment="EQUITY", external_id=external_id, is_active=True)
            db.add(new_security)
            return 'created', new_security

    except SQLAlchemyError as e:
        logger.error(f"Database error processing security {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}")
        return 'error', None
    except Exception as e:
        logger.error(f"Error processing security {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}")
        return 'error', None


def process_all_securities(db: Session, securities_df, futures_df, nse_exchange: Exchange) -> Dict:
    """
    Process all securities (regular + derivative securities from futures) with proper error handling
    """
    stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
    batch_size = 100

    # Process regular securities first
    logger.info(f"Processing {len(securities_df)} regular securities")
    for processed_count, (index, row) in enumerate(securities_df.iterrows(), 1):
        try:
            status, security = process_single_security(db, row, nse_exchange)
            stats[status] += 1

            # Commit every batch_size records
            if processed_count % batch_size == 0:
                try:
                    db.commit()
                    logger.info(f"Committed batch: {processed_count}/{len(securities_df)} regular securities")
                except SQLAlchemyError as e:
                    logger.error(f"Batch commit failed for regular securities: {e}")
                    db.rollback()
                    stats['errors'] += batch_size  # Count batch as errors

        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error processing regular security: {e}")
            try:
                db.rollback()
            except:
                pass

    # Process futures AS securities (create derivative security records)
    logger.info(f"Processing {len(futures_df)} futures as securities")
    for processed_count, (index, row) in enumerate(futures_df.iterrows(), 1):
        try:
            status, security = process_single_security(db, row, nse_exchange)
            stats[status] += 1

            # Commit every batch_size records
            if processed_count % batch_size == 0:
                try:
                    db.commit()
                    logger.info(f"Committed batch: {processed_count}/{len(futures_df)} futures as securities")
                except SQLAlchemyError as e:
                    logger.error(f"Batch commit failed for futures as securities: {e}")
                    db.rollback()
                    stats['errors'] += batch_size

        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error processing future as security: {e}")
            try:
                db.rollback()
            except:
                pass

    # Final commit
    try:
        db.commit()
        logger.info(f"All securities processing completed: {stats}")
    except SQLAlchemyError as e:
        logger.error(f"Final commit failed for all securities: {e}")
        db.rollback()
        raise

    return stats


def process_single_future_relationship(db: Session, row: Dict, securities_cache: Dict[str, Security]) -> Tuple[str, Optional[Future]]:
    """
    Create a Future relationship record with proper duplicate checking
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

        # FIXED: Check for existing future using the unique constraint fields
        existing = db.query(Future).filter(and_(Future.underlying_id == underlying.id, Future.contract_month == contract_month, Future.expiration_date == expiry_date)).first()

        if existing:
            # Update existing future - but only if it's the same security or if we should replace it
            if existing.security_id == derivative_security.id:
                # Same security, just update
                existing.lot_size = lot_size
                existing.settlement_type = "CASH" if is_index else "PHYSICAL"
                existing.is_active = True
                return 'updated', existing
            else:
                # Different security but same contract details - this is a data issue
                # Log warning and skip to avoid constraint violation
                logger.warning(f"Duplicate future contract found: {symbol} vs existing security_id: {existing.security_id}")
                return 'skipped', None
        else:
            # Create new future relationship
            new_future = Future(security_id=derivative_security.id, underlying_id=underlying.id, expiration_date=expiry_date, contract_size=1.0, lot_size=lot_size, settlement_type="CASH" if is_index else "PHYSICAL", contract_month=contract_month, is_active=True)
            db.add(new_future)
            return 'created', new_future

    except IntegrityError as e:
        logger.warning(f"Integrity constraint violation for future {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}")
        return 'skipped', None
    except Exception as e:
        logger.error(f"Error processing future relationship {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}")
        return 'error', None


def process_futures_relationships(db: Session, futures_df) -> Dict:
    """
    Process futures relationships with proper error handling and deduplication
    """
    stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

    logger.info(f"Starting futures relationships processing for {len(futures_df)} futures")

    # Build securities cache for faster lookups
    securities_cache = build_securities_cache(db)

    # Deduplicate futures based on unique constraint before processing
    logger.info("Deduplicating futures data...")
    deduplicated_futures = deduplicate_futures_data(futures_df)

    original_count = len(futures_df)
    deduplicated_count = len(deduplicated_futures)
    duplicates_removed = original_count - deduplicated_count

    logger.info(f"Deduplication complete: {deduplicated_count} unique futures to process (removed {duplicates_removed} duplicates)")

    # Process each future with batch commits for better performance
    batch_size = 50

    for processed_count, (index, row) in enumerate(deduplicated_futures.iterrows(), 1):
        try:
            status, future = process_single_future_relationship(db, row, securities_cache)
            stats[status] += 1

            # Commit in batches but handle individual failures
            if processed_count % batch_size == 0:
                try:
                    db.commit()
                    logger.info(f"Progress: {processed_count}/{deduplicated_count} futures processed ({(processed_count/deduplicated_count)*100:.1f}%)")
                except IntegrityError as e:
                    logger.warning(f"Batch constraint violation, rolling back batch of {batch_size} records")
                    db.rollback()
                    stats['errors'] += batch_size
                except SQLAlchemyError as e:
                    logger.error(f"Database error in batch: {e}")
                    db.rollback()
                    stats['errors'] += batch_size

        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error processing future relationship: {e}")
            try:
                db.rollback()
            except:
                pass

    # Final commit for remaining records
    try:
        db.commit()
        logger.info(f"Futures relationships processing completed: {stats}")
    except SQLAlchemyError as e:
        logger.error(f"Final commit failed: {e}")
        db.rollback()
        raise

    return stats


def deduplicate_futures_data(futures_df) -> 'pd.DataFrame':
    """
    Deduplicate futures data based on the unique constraint fields
    """
    logger.info(f"Starting deduplication with {len(futures_df)} futures")

    try:
        # Create dedup columns directly (safer than apply)
        work_df = futures_df.copy()
        logger.info(f"After copy: {len(work_df)} futures")

        # Add columns for deduplication key components
        work_df['underlying_symbol'] = work_df['SEM_TRADING_SYMBOL'].apply(extract_underlying_symbol)
        logger.info(f"After underlying extraction: {len(work_df)} futures")

        work_df['parsed_expiry'] = work_df['SEM_EXPIRY_DATE'].apply(parse_expiry_date)
        logger.info(f"After expiry parsing: {len(work_df)} futures")

        work_df['contract_month'] = work_df['parsed_expiry'].apply(lambda x: get_contract_month(x) if x else None)
        logger.info(f"After contract month: {len(work_df)} futures")

        # Remove rows with invalid data
        valid_df = work_df.dropna(subset=['underlying_symbol', 'parsed_expiry', 'contract_month'])
        logger.info(f"After removing invalid rows: {len(valid_df)} futures")

        # Deduplicate based on the actual unique constraint fields
        deduplicated = valid_df.drop_duplicates(subset=['underlying_symbol', 'contract_month', 'parsed_expiry'], keep='first')
        logger.info(f"After deduplication: {len(deduplicated)} futures")

        # Clean up temporary columns and return original structure
        columns_to_keep = [col for col in futures_df.columns]
        final_df = deduplicated[columns_to_keep].copy()

        logger.info(f"Final deduplicated dataset: {len(final_df)} futures")
        return final_df

    except Exception as e:
        logger.error(f"Error in deduplication: {e}")
        logger.error(f"Returning original dataset of {len(futures_df)} futures")
        return futures_df


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
    return cache


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


# Legacy functions for backward compatibility (but fixed)
def save_securities_batch(db: Session, securities_df, nse_exchange: Exchange, batch_size: int = 100) -> Dict:
    """Legacy function - use process_all_securities instead"""
    import pandas as pd
    return process_all_securities(db, securities_df, pd.DataFrame(), nse_exchange)


def save_futures_batch(db: Session, futures_df, batch_size: int = 50) -> Dict:
    """Legacy function - use process_futures_relationships instead"""
    return process_futures_relationships(db, futures_df)
