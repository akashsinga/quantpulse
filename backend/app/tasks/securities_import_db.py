# backend/app/tasks/securities_import_db.py

import os
import sys

import uuid
import pandas as pd
from datetime import datetime, date
from typing import Dict, Optional, Tuple, Set, List
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import and_, text
from sqlalchemy.dialects.postgresql import insert

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


def process_all_securities(db: Session, securities_df, futures_df, nse_exchange: Exchange) -> Dict:
    """
    Process all securities (regular + derivative securities from futures) using PostgreSQL upserts
    """
    stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

    try:
        # Process regular securities
        logger.info(f"Processing {len(securities_df)} regular securities with bulk upsert")
        regular_stats = _bulk_upsert_securities(db, securities_df, nse_exchange)

        # Add regular securities stats
        for key in stats:
            stats[key] += regular_stats[key]

        # Process futures as securities (derivatives)
        logger.info(f"Processing {len(futures_df)} futures as securities with bulk upsert")
        futures_stats = _bulk_upsert_securities(db, futures_df, nse_exchange)

        # Add futures stats
        for key in stats:
            stats[key] += futures_stats[key]

        logger.info(f"All securities processing completed: {stats}")
        return stats

    except Exception as e:
        logger.error(f"Error in bulk securities processing: {e}")
        db.rollback()
        raise


def _bulk_upsert_securities(db: Session, df: pd.DataFrame, nse_exchange: Exchange) -> Dict:
    """
    Bulk upsert securities using PostgreSQL's ON CONFLICT with proper constraint handling
    FIXED: Handle both external_id and symbol+exchange unique constraints
    """
    stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

    if len(df) == 0:
        return stats

    # Prepare data for bulk insert
    securities_data = []

    for index, row in df.iterrows():
        try:
            external_id = int(row["SEM_SMST_SECURITY_ID"])
            symbol = row["SEM_TRADING_SYMBOL"]
            name = get_security_name(row)
            security_type = map_security_type(row)

            securities_data.append({'id': uuid.uuid4(), 'symbol': symbol, 'name': name, 'exchange_id': nse_exchange.id, 'security_type': security_type, 'segment': "EQUITY", 'external_id': external_id, 'is_active': True, 'created_at': datetime.now(), 'updated_at': datetime.now()})

        except Exception as e:
            logger.warning(f"Error preparing security data for {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}")
            stats['errors'] += 1
            continue

    if not securities_data:
        logger.warning("No valid securities data to process")
        return stats

    # Remove duplicates within this batch by external_id (keep last occurrence)
    logger.info(f"Deduplicating {len(securities_data)} securities within batch")

    external_id_to_data = {}
    duplicates_removed = 0

    for item in securities_data:
        external_id = item['external_id']
        if external_id in external_id_to_data:
            duplicates_removed += 1
            logger.debug(f"Duplicate external_id {external_id} found, keeping latest")
        external_id_to_data[external_id] = item

    # Convert back to list
    deduplicated_data = list(external_id_to_data.values())

    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicates within batch, processing {len(deduplicated_data)} unique securities")
        stats['skipped'] += duplicates_removed

    try:
        # FIXED: Use the correct unique constraint for conflict resolution
        stmt = insert(Security).values(deduplicated_data)

        # Handle conflicts on the symbol+exchange constraint (the actual business rule)
        upsert_stmt = stmt.on_conflict_do_update(
            constraint='uq_symbol_exchange',  # Use the actual constraint that's failing
            set_={
                'name': stmt.excluded.name,
                'security_type': stmt.excluded.security_type,
                'external_id': stmt.excluded.external_id,  # Update external_id in case it changed
                'segment': stmt.excluded.segment,
                'is_active': stmt.excluded.is_active,
                'updated_at': stmt.excluded.updated_at
            })

        # Execute the upsert
        result = db.execute(upsert_stmt)
        db.commit()

        total_affected = result.rowcount
        stats['updated'] = total_affected  # Most likely these are updates since constraint exists

        logger.info(f"Bulk upsert completed: {total_affected} securities processed (constraint: symbol+exchange)")

    except SQLAlchemyError as e:
        logger.error(f"Bulk upsert failed: {e}")
        db.rollback()

        # Try alternative approach: handle external_id conflicts instead
        try:
            logger.info("Retrying with external_id constraint...")

            stmt = insert(Security).values(deduplicated_data)

            # Try with external_id constraint as fallback
            upsert_stmt = stmt.on_conflict_do_update(index_elements=['external_id'], set_={'symbol': stmt.excluded.symbol, 'name': stmt.excluded.name, 'security_type': stmt.excluded.security_type, 'segment': stmt.excluded.segment, 'is_active': stmt.excluded.is_active, 'updated_at': stmt.excluded.updated_at})

            result = db.execute(upsert_stmt)
            db.commit()

            total_affected = result.rowcount
            stats['updated'] = total_affected

            logger.info(f"Fallback upsert completed: {total_affected} securities processed (constraint: external_id)")

        except SQLAlchemyError as e2:
            logger.error(f"Both upsert attempts failed: {e2}")
            stats['errors'] += len(deduplicated_data)
            raise

    return stats


def process_futures_relationships(db: Session, futures_df) -> Dict:
    """
    Process futures relationships using bulk upsert with proper duplicate handling
    FIXED: Better error handling for constraint violations
    """
    stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

    if len(futures_df) == 0:
        return stats

    logger.info(f"Starting futures relationships processing for {len(futures_df)} futures")

    # Build securities cache for faster lookups
    securities_cache = build_securities_cache(db)

    # Prepare futures data with validation
    futures_data = []

    for index, row in futures_df.iterrows():
        try:
            future_data = _prepare_future_data(row, securities_cache, db)
            if future_data:
                futures_data.append(future_data)
            else:
                stats['skipped'] += 1
        except Exception as e:
            logger.warning(f"Error preparing future data for {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}")
            stats['errors'] += 1
            continue

    if not futures_data:
        logger.info("No valid futures data to process")
        return stats

    logger.info(f"Prepared {len(futures_data)} valid futures for bulk processing")

    # Remove duplicates within this batch based on the unique constraint fields
    logger.info(f"Deduplicating {len(futures_data)} futures within batch")

    constraint_key_to_data = {}
    duplicates_removed = 0

    for item in futures_data:
        # Create a key based on the unique constraint fields
        constraint_key = (item['underlying_id'], item['contract_month'], item['expiration_date'], item['settlement_type'])

        if constraint_key in constraint_key_to_data:
            duplicates_removed += 1
            logger.debug(f"Duplicate future contract found: {constraint_key}, keeping latest")

        constraint_key_to_data[constraint_key] = item

    # Convert back to list
    deduplicated_data = list(constraint_key_to_data.values())

    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicate futures within batch, processing {len(deduplicated_data)} unique futures")
        stats['skipped'] += duplicates_removed

    try:
        # Use bulk upsert for futures
        stmt = insert(Future).values(deduplicated_data)

        # On conflict with the unique constraint, update the record
        upsert_stmt = stmt.on_conflict_do_update(
            constraint='uq_future_contract_details',  # underlying_id, contract_month, expiration_date, settlement_type
            set_={
                'lot_size': stmt.excluded.lot_size,
                'contract_size': stmt.excluded.contract_size,
                'is_active': stmt.excluded.is_active,
                'initial_margin': stmt.excluded.initial_margin,
                'maintenance_margin': stmt.excluded.maintenance_margin
            })

        result = db.execute(upsert_stmt)
        db.commit()

        total_affected = result.rowcount
        stats['updated'] = total_affected

        logger.info(f"Bulk futures upsert completed: {total_affected} futures processed")

    except SQLAlchemyError as e:
        logger.error(f"Bulk futures upsert failed: {e}")
        db.rollback()

        # Try processing futures individually as fallback
        try:
            logger.info("Retrying futures with individual processing...")
            individual_stats = _process_futures_individually(db, deduplicated_data)

            for key in stats:
                if key in individual_stats:
                    stats[key] += individual_stats[key]

        except Exception as e2:
            logger.error(f"Individual futures processing also failed: {e2}")
            stats['errors'] += len(deduplicated_data)
            raise

    return stats


def _process_futures_individually(db: Session, futures_data: List[Dict]) -> Dict:
    """
    Process futures individually when bulk processing fails
    """
    stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

    for future_data in futures_data:
        try:
            # Check if future already exists
            existing = db.query(Future).filter(Future.underlying_id == future_data['underlying_id'], Future.contract_month == future_data['contract_month'], Future.expiration_date == future_data['expiration_date'], Future.settlement_type == future_data['settlement_type']).first()

            if existing:
                # Update existing
                for key, value in future_data.items():
                    if key != 'security_id':  # Don't update the primary key
                        setattr(existing, key, value)
                stats['updated'] += 1
            else:
                # Create new
                future = Future(**future_data)
                db.add(future)
                stats['created'] += 1

        except Exception as e:
            logger.warning(f"Error processing individual future: {e}")
            stats['errors'] += 1
            continue

    db.commit()
    return stats


def _prepare_future_data(row: Dict, securities_cache: Dict[str, Security], db: Session) -> Optional[Dict]:
    """
    Prepare a single future record for bulk insert with validation
    """
    try:
        external_id = int(row["SEM_SMST_SECURITY_ID"])
        symbol = row["SEM_TRADING_SYMBOL"]

        # Find the derivative security (should exist now)
        derivative_security = db.query(Security).filter(Security.external_id == external_id, Security.security_type == "DERIVATIVE").first()

        if not derivative_security:
            return None

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
            return None

        # Parse expiry date
        expiry_date = parse_expiry_date(row["SEM_EXPIRY_DATE"])
        if not expiry_date:
            return None

        # Get contract specifications
        contract_month = get_contract_month(expiry_date)
        lot_size = safe_int_conversion(row.get("SEM_LOT_UNITS"), 1)
        is_index = is_index_future(row.get("SEM_INSTRUMENT_NAME", ""))

        return {'security_id': derivative_security.id, 'underlying_id': underlying.id, 'expiration_date': expiry_date, 'contract_size': 1.0, 'lot_size': lot_size, 'settlement_type': "CASH" if is_index else "PHYSICAL", 'contract_month': contract_month, 'is_active': True, 'initial_margin': None, 'maintenance_margin': None, 'previous_contract_id': None}

    except Exception as e:
        logger.warning(f"Error preparing future data for {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}")
        return None


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

    try:
        # Use bulk update for better performance
        update_query = text("""
            UPDATE futures 
            SET is_active = false 
            WHERE expiration_date < :today 
            AND is_active = true
        """)

        result = db.execute(update_query, {'today': today})
        marked_count = result.rowcount
        db.commit()

        logger.info(f"Marked {marked_count} expired futures as inactive")
        return marked_count

    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error marking expired futures: {e}")
        raise


# Simplified legacy functions for backward compatibility
def process_single_security(db: Session, row: Dict, nse_exchange: Exchange) -> Tuple[str, Optional[Security]]:
    """
    Legacy function - now just calls the bulk processor for single items
    """
    df = pd.DataFrame([row])
    result = _bulk_upsert_securities(db, df, nse_exchange)

    if result['errors'] > 0:
        return 'error', None
    elif result['created'] > 0:
        return 'created', None
    else:
        return 'updated', None


def process_single_future_relationship(db: Session, row: Dict, securities_cache: Dict[str, Security]) -> Tuple[str, Optional[Future]]:
    """
    Legacy function - use bulk processing instead
    """
    future_data = _prepare_future_data(row, securities_cache, db)
    if future_data:
        return 'created', None
    else:
        return 'skipped', None


# Remove the complex deduplication function - let PostgreSQL handle duplicates
def deduplicate_futures_data(futures_df) -> 'pd.DataFrame':
    """
    Simplified deduplication - just remove obvious duplicates by external_id
    Let PostgreSQL handle the complex constraint checking
    """
    logger.info(f"Simple deduplication: {len(futures_df)} futures")

    # Only remove exact duplicates by external_id (simple and safe)
    deduplicated = futures_df.drop_duplicates(subset=['SEM_SMST_SECURITY_ID'], keep='first')

    removed_count = len(futures_df) - len(deduplicated)
    if removed_count > 0:
        logger.info(f"Removed {removed_count} exact duplicates by external_id")

    return deduplicated


# Legacy functions kept for compatibility
def save_securities_batch(db: Session, securities_df, nse_exchange: Exchange, batch_size: int = 100) -> Dict:
    """Legacy function - use process_all_securities instead"""
    import pandas as pd
    return process_all_securities(db, securities_df, pd.DataFrame(), nse_exchange)


def save_futures_batch(db: Session, futures_df, batch_size: int = 50) -> Dict:
    """Legacy function - use process_futures_relationships instead"""
    return process_futures_relationships(db, futures_df)
