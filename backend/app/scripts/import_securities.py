#!/usr/bin/env python
"""
Securities Import System for QuantPulse

This script imports securities data from Dhan API into the QuantPulse database.
Modified to import only NSE (National Stock Exchange of India) securities.
Handles incremental updates, includes parallel processing for performance,
and implements comprehensive error handling and logging with progress bars.

Usage:
    As CLI tool: python -m app.scripts.import_securities [--full] [--workers=8]
    From API: POST /api/v1/admin/update_securities

Author: QuantPulse Team
"""

import os
import time
import csv
import io
import uuid
import argparse
import concurrent.futures
import requests
import pandas as pd
from typing import Dict, Tuple, Any
from datetime import datetime, date
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app.db.session import SessionLocal, get_db
from app.db.models.security import Security
from app.db.models.derivatives import Future
from app.db.models.exchange import Exchange
from app.utils.logger import get_logger
from app.config import settings

# Configure logging
logger = get_logger(__name__)

# Constants
CSV_BACKUP_DIR = os.path.join(settings.CACHE_DIR, "securities_data")
BATCH_SIZE = 10  # Process in very small batches to avoid SQL limitations


class SecuritiesImporter:
    """Handles importing securities from Dhan API into QuantPulse database."""

    def __init__(self,
                 workers: int = 8,
                 full_refresh: bool = False,
                 show_progress: bool = True):
        """Initialize the importer.

        Args:
            workers: Number of worker threads for parallel processing
            full_refresh: Whether to perform a full refresh (vs incremental)
            show_progress: Whether to show progress bars (CLI mode)
        """
        self.workers = workers
        self.full_refresh = full_refresh
        self.show_progress = show_progress
        self.exchanges_map = {}
        self.metrics = {
            "securities_total": 0,
            "securities_created": 0,
            "securities_updated": 0,
            "securities_skipped": 0,
            "futures_total": 0,
            "futures_created": 0,
            "futures_updated": 0,
            "futures_skipped": 0,
            "errors": 0,
            "runtime_seconds": 0
        }

        # Load exchange mapping - using a separate session that will be closed after initialization
        with get_db() as db:
            self._load_exchanges(db)

        # Ensure backup directory exists
        os.makedirs(CSV_BACKUP_DIR, exist_ok=True)

        # Check and increase symbol column length if needed
        with get_db() as db:
            self._ensure_symbol_column_length(db)

    def _load_exchanges(self, db: Session) -> None:
        """Load exchanges from database and create a lookup map.

        Args:
            db: Database session
        """
        exchanges = db.query(Exchange).all()
        for exchange in exchanges:
            self.exchanges_map[exchange.code] = exchange.id

        # Create exchanges with fixed IDs for consistency if they don't exist
        exchanges_to_create = []

        # Ensure NSE is always present
        if "NSE" not in self.exchanges_map:
            # Use a fixed UUID for NSE
            nse_id = uuid.UUID("984ffe13-dcfb-4362-8291-5f2bee2645ef")
            nse = Exchange(id=nse_id,
                           name="National Stock Exchange",
                           code="NSE",
                           country="India",
                           timezone="Asia/Kolkata",
                           is_active=True)
            exchanges_to_create.append(nse)
            self.exchanges_map["NSE"] = nse_id

        # We'll add BSE for completeness, but we won't import its securities
        if "BSE" not in self.exchanges_map:
            # Use a different fixed UUID for BSE
            bse_id = uuid.UUID("56d68a9b-c756-4baa-9786-3d1a7b2cfb2c")
            bse = Exchange(id=bse_id,
                           name="Bombay Stock Exchange",
                           code="BSE",
                           country="India",
                           timezone="Asia/Kolkata",
                           is_active=False)  # Set to inactive
            exchanges_to_create.append(bse)
            self.exchanges_map["BSE"] = bse_id

        # Add all exchanges in one batch if needed
        if exchanges_to_create:
            db.add_all(exchanges_to_create)
            db.commit()
            logger.info(
                f"Created {len(exchanges_to_create)} missing exchanges")

    def _ensure_symbol_column_length(self, db: Session) -> None:
        """Check and increase symbol column length if needed.

        Args:
            db: Database session
        """
        try:
            # Check current column length
            result = db.execute(
                text("""
                SELECT character_maximum_length 
                FROM information_schema.columns 
                WHERE table_name = 'securities' AND column_name = 'symbol'
            """)).scalar()

            if result and int(result) < 100:
                # Column is too short, increase it
                logger.info(
                    "Increasing securities.symbol column length to 100 characters"
                )
                db.execute(
                    text("""
                    ALTER TABLE securities 
                    ALTER COLUMN symbol TYPE varchar(100)
                """))
                db.commit()
                logger.info("Column length increased successfully")
            else:
                logger.info(
                    "Securities.symbol column length is already sufficient")

        except Exception as e:
            logger.error(f"Error checking/updating symbol column length: {e}")
            # Continue anyway, we'll catch insert errors later

    def fetch_securities_data(self) -> pd.DataFrame:
        """Download and parse securities data from Dhan API.

        Returns:
            DataFrame with securities data
        """
        start_time = time.time()
        logger.info(
            f"Downloading securities data from {settings.DHAN_SCRIP_MASTER_URL}"
        )

        try:
            # Download data without progress bar
            response = requests.get(settings.DHAN_SCRIP_MASTER_URL, timeout=60)
            response.raise_for_status()
            content = response.content

            # Save backup copy
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = os.path.join(CSV_BACKUP_DIR,
                                       f"dhan_securities_{timestamp}.csv")
            with open(backup_path, "wb") as f:
                f.write(content)
            logger.info(f"Saved backup to {backup_path}")

            # Parse CSV
            logger.info("Parsing CSV data...")
            if self.show_progress:
                logger.info("Parsing CSV data...")

            csv_content = content.decode("utf-8")
            df = pd.read_csv(io.StringIO(csv_content), low_memory=False)

            if self.show_progress:
                logger.info(f" Done! ({len(df)} records)")

            logger.info(
                f"Downloaded and parsed {len(df)} records in {time.time() - start_time:.2f} seconds"
            )
            return df

        except requests.RequestException as e:
            logger.error(f"Error downloading securities data: {e}")

            # Try to use latest backup if available
            backup_files = sorted([
                f for f in os.listdir(CSV_BACKUP_DIR)
                if f.startswith("dhan_securities_")
            ])
            if backup_files:
                latest_backup = os.path.join(CSV_BACKUP_DIR, backup_files[-1])
                logger.info(f"Using latest backup from {latest_backup}")

                if self.show_progress:
                    logger.warning(
                        f"Download failed. Using backup from {latest_backup}")

                return pd.read_csv(latest_backup, low_memory=False)

            raise RuntimeError(
                "Failed to download securities data and no backup available")

    def filter_data(self,
                    df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Filter data to include only relevant NSE securities.

        Filtering criteria:
        1. Exchange only NSE
        2. For Stocks: SEM_SEGMENT == E and SEM_EXCH_INSTRUMENT_TYPE == ES
        3. For Indices: SEM_SEGMENT == I and SEM_EXCH_INSTRUMENT_TYPE == INDEX
        4. For Futures: SEM_SEGMENT == D and SEM_EXCH_INSTRUMENT_TYPE in [FUTSTK, FUTIDX]

        Args:
            df: DataFrame with all securities

        Returns:
            Tuple of (securities_df, futures_df)
        """
        # Filter for NSE exchange only
        nse_df = df[df["SEM_EXM_EXCH_ID"] == "NSE"]

        if len(nse_df) == 0:
            logger.warning(
                "No NSE securities found in the data. Check the exchange codes in the source data."
            )
            # Return empty DataFrames to avoid errors
            return pd.DataFrame(), pd.DataFrame()

        # Filter main securities (stocks and indices) from NSE with specific instrument types
        stocks_df = nse_df[(nse_df["SEM_SEGMENT"] == "E")
                           & (nse_df["SEM_EXCH_INSTRUMENT_TYPE"] == "ES")]
        indices_df = nse_df[(nse_df["SEM_SEGMENT"] == "I")
                            & (nse_df["SEM_EXCH_INSTRUMENT_TYPE"] == "INDEX")]

        # Combine stocks and indices
        securities_df = pd.concat([stocks_df, indices_df])

        # Filter futures from NSE with specific instrument types
        futures_df = nse_df[(nse_df["SEM_SEGMENT"] == "D") & (
            nse_df["SEM_INSTRUMENT_NAME"].isin(["FUTSTK", "FUTIDX"]))]

        logger.info(
            f"Filtered {len(stocks_df)} NSE stocks and {len(indices_df)} NSE indices for a total of {len(securities_df)} securities"
        )
        logger.info(f"Filtered {len(futures_df)} NSE futures")

        if self.show_progress:
            logger.info(
                f"Filtered {len(stocks_df)} NSE stocks and {len(indices_df)} NSE indices"
            )
            logger.info(f"Filtered {len(futures_df)} NSE futures")

        return securities_df, futures_df

    def map_to_security_type(self, row: Dict) -> str:
        """Map Dhan security type to our schema.

        Args:
            row: Security data row

        Returns:
            Security type string
        """
        if row["SEM_SEGMENT"] == "E":
            return "STOCK"

        if row["SEM_SEGMENT"] == "I":
            return "INDEX"

        if row["SEM_SEGMENT"] == "D":
            return "DERIVATIVE"

        return "STOCK"  # Default

    def process_security(self, row: Dict) -> Dict:
        """Process a single security record with proper error handling.

        Args:
            row: Security data row

        Returns:
            Result dictionary
        """
        result = {"status": "unknown", "error": None, "security": None}

        # Skip non-NSE securities
        if row["SEM_EXM_EXCH_ID"] != "NSE":
            result["status"] = "skipped"
            result[
                "error"] = f"Skipping non-NSE exchange: {row['SEM_EXM_EXCH_ID']}"
            return result

        # Create a new session for this operation
        db = SessionLocal()
        try:
            # Get exchange ID for NSE
            exchange_id = self.exchanges_map.get("NSE")
            if not exchange_id:
                result["status"] = "skipped"
                result["error"] = f"Exchange not found: NSE"
                return result

            # Check if security already exists - check both by external_id and by symbol+exchange
            external_id = int(row["SEM_SMST_SECURITY_ID"])
            symbol = row["SEM_TRADING_SYMBOL"]

            # First check by external_id (preferred)
            existing = db.query(Security).filter(
                Security.external_id == external_id).first()

            # If not found by external_id, check by symbol and exchange
            if not existing:
                existing = db.query(Security).filter(
                    Security.symbol == symbol,
                    Security.exchange_id == exchange_id).first()

                # If found by symbol+exchange but has different external_id, this is unusual
                # but we'll update it to match the current row
                if existing and existing.external_id != external_id:
                    logger.warning(
                        f"Security found with same symbol+exchange but different external_id: {symbol} (DB: {existing.external_id}, CSV: {external_id})"
                    )

            security_type = self.map_to_security_type(row)

            # Get name with priority order: SEM_CUSTOM_SYMBOL (for futures), SM_SYMBOL_NAME, SEM_TRADING_SYMBOL
            if security_type == "DERIVATIVE" and pd.notna(
                    row.get("SEM_CUSTOM_SYMBOL")):
                name = row[
                    "SEM_CUSTOM_SYMBOL"]  # Use the custom symbol which should have proper naming
            elif pd.notna(row.get("SM_SYMBOL_NAME")):
                name = row["SM_SYMBOL_NAME"]
            else:
                name = symbol

            if existing:
                # Update existing security
                existing.symbol = symbol
                existing.name = name
                existing.security_type = security_type
                existing.segment = "EQUITY"  # All are EQUITY for now
                existing.is_active = True
                existing.updated_at = datetime.now()
                db.commit()

                result["status"] = "updated"
                result["security"] = existing
            else:
                # Double-check again to avoid race conditions with other threads
                # This helps prevent unique constraint violations
                check_again = db.query(Security).filter(
                    Security.symbol == symbol,
                    Security.exchange_id == exchange_id).first()

                if check_again:
                    # Another thread created this security between our first check and now
                    # Just update it and return
                    check_again.name = name
                    check_again.security_type = security_type
                    check_again.segment = "EQUITY"
                    check_again.is_active = True
                    check_again.external_id = external_id  # Make sure external_id matches
                    check_again.updated_at = datetime.now()
                    db.commit()

                    result["status"] = "updated"
                    result["security"] = check_again
                else:
                    # Create new security
                    new_security = Security(
                        id=uuid.uuid4(),
                        symbol=symbol,
                        name=name,
                        exchange_id=exchange_id,
                        security_type=security_type,
                        segment="EQUITY",
                        external_id=external_id,
                        is_active=True)  # All are EQUITY for now
                    db.add(new_security)

                    try:
                        db.commit()
                        result["status"] = "created"
                        result["security"] = new_security
                    except SQLAlchemyError as e:
                        # If we hit a unique constraint error, try to find the existing record
                        db.rollback()
                        logger.warning(
                            f"Conflict while creating security {symbol}: {e}")

                        # Look up the record again
                        final_check = db.query(Security).filter(
                            Security.symbol == symbol,
                            Security.exchange_id == exchange_id).first()

                        if final_check:
                            # Update and return the existing record
                            final_check.name = name
                            final_check.security_type = security_type
                            final_check.segment = "EQUITY"
                            final_check.is_active = True
                            final_check.external_id = external_id
                            final_check.updated_at = datetime.now()
                            db.commit()

                            result["status"] = "updated"
                            result["security"] = final_check
                        else:
                            # Something else went wrong
                            result["status"] = "error"
                            result["error"] = str(e)
                            logger.error(
                                f"Failed to create or find security {symbol}: {e}"
                            )

        except Exception as e:
            db.rollback()
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(
                f"Error processing security {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}"
            )
        finally:
            db.close()  # Always close the session

        return result

    def process_securities_chunk(self, chunk: pd.DataFrame) -> Dict[str, int]:
        """Process a chunk of securities data in small batches.

        Args:
            chunk: DataFrame with securities to process

        Returns:
            Statistics dictionary
        """
        stats = {"created": 0, "updated": 0, "skipped": 0, "errors": 0}

        # Break into smaller batches for processing
        for i in range(0, len(chunk), BATCH_SIZE):
            batch = chunk.iloc[i:i + BATCH_SIZE]

            for _, row in batch.iterrows():
                result = self.process_security(row)

                if result["status"] == "created":
                    stats["created"] += 1
                elif result["status"] == "updated":
                    stats["updated"] += 1
                elif result["status"] == "skipped":
                    stats["skipped"] += 1
                else:
                    stats["errors"] += 1

        return stats

    def process_securities(self, securities_df: pd.DataFrame) -> None:
        """Process all securities in parallel.

        Args:
            securities_df: DataFrame with securities to process
        """
        start_time = time.time()
        logger.info(
            f"Processing {len(securities_df)} NSE securities with {self.workers} workers"
        )

        # Split data into chunks
        chunk_size = 100
        chunks = [
            securities_df.iloc[i:i + chunk_size]
            for i in range(0, len(securities_df), chunk_size)
        ]
        logger.info(
            f"Split data into {len(chunks)} chunks of {chunk_size} records")

        total_stats = {"created": 0, "updated": 0, "skipped": 0, "errors": 0}

        # Log progress periodically instead of using tqdm
        if self.show_progress:
            logger.info(
                f"Processing {len(securities_df)} NSE securities in {len(chunks)} chunks..."
            )

        # Process chunks in parallel
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.workers) as executor:
            future_to_chunk = {
                executor.submit(self.process_securities_chunk, chunk): i
                for i, chunk in enumerate(chunks)
            }

            completed = 0
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk_idx = future_to_chunk[future]
                completed += 1

                # Log progress every 10% or 10 chunks, whichever is more frequent
                log_interval = max(len(chunks) // 10, 10)
                if completed % log_interval == 0 or completed == len(chunks):
                    if self.show_progress:
                        progress_pct = (completed / len(chunks)) * 100
                        logger.info(
                            f"Progress: {completed}/{len(chunks)} chunks ({progress_pct:.1f}%)..."
                        )

                try:
                    stats = future.result()
                    # Aggregate stats
                    for key in total_stats:
                        total_stats[key] += stats[key]

                    logger.info(
                        f"Completed chunk {chunk_idx+1}/{len(chunks)}: {stats}"
                    )
                except Exception as e:
                    logger.error(f"Error in chunk {chunk_idx}: {e}")
                    total_stats["errors"] += len(chunks[chunk_idx])

        duration = time.time() - start_time
        logger.info(
            f"Finished processing securities in {duration:.2f} seconds: {total_stats}"
        )
        if self.show_progress:
            logger.info(
                f"Securities processing complete: {total_stats['created']} created, {total_stats['updated']} updated, {total_stats['errors']} errors"
            )

        # Update metrics
        self.metrics["securities_total"] = len(securities_df)
        self.metrics["securities_created"] = total_stats["created"]
        self.metrics["securities_updated"] = total_stats["updated"]
        self.metrics["securities_skipped"] = total_stats["skipped"]
        self.metrics["errors"] += total_stats["errors"]

    def extract_underlying_symbol(self, row: Dict) -> str:
        """Extract underlying symbol from futures data.

        Args:
            row: Futures data row

        Returns:
            Underlying symbol
        """
        # Extract from symbol (e.g., "NIFTY-Jun2025-FUT" -> "NIFTY")
        parts = row["SEM_TRADING_SYMBOL"].split("-")
        return parts[0]

    def process_future(self, row: Dict) -> Dict:
        """Process a single future record with proper error handling.

        This method doesn't take a securities_cache to avoid session binding issues.

        Args:
            row: Future data row

        Returns:
            Result dictionary
        """
        result = {"status": "unknown", "error": None, "future": None}

        # Skip non-NSE securities
        if row["SEM_EXM_EXCH_ID"] != "NSE":
            result["status"] = "skipped"
            result[
                "error"] = f"Skipping non-NSE exchange: {row['SEM_EXM_EXCH_ID']}"
            return result

        # Create a new session for this operation
        db = SessionLocal()
        try:
            # First ensure the security record exists
            external_id = int(row["SEM_SMST_SECURITY_ID"])
            symbol = row["SEM_TRADING_SYMBOL"]

            # Check if the security exists by external_id
            security = db.query(Security).filter(
                Security.external_id == external_id).first()

            # If security doesn't exist, create it (with a new session inside process_security)
            if not security:
                # Close current session
                db.close()
                # process_security creates its own session
                security_result = self.process_security(row)

                # Re-open a new session
                db = SessionLocal()

                if security_result["status"] in ["created", "updated"]:
                    # Look up the security using the external_id
                    security = db.query(Security).filter(
                        Security.external_id == external_id).first()

                    if not security:
                        result["status"] = "error"
                        result[
                            "error"] = f"Security was created but cannot be found: {external_id}"
                        return result
                else:
                    result["status"] = "skipped"
                    result[
                        "error"] = f"Failed to create security: {security_result['error']}"
                    return result

            # Extract the underlying symbol
            underlying_symbol = self.extract_underlying_symbol(row)

            # Find underlying security
            underlying = db.query(Security).filter(
                Security.symbol == underlying_symbol,
                Security.security_type.in_(["STOCK", "INDEX"])).first()

            if not underlying:
                result["status"] = "skipped"
                result[
                    "error"] = f"Underlying security not found: {underlying_symbol}"
                return result

            # Parse expiry date
            try:
                expiry_date = datetime.strptime(
                    row["SEM_EXPIRY_DATE"].split()[0], "%Y-%m-%d").date()
            except (ValueError, AttributeError):
                result["status"] = "error"
                result[
                    "error"] = f"Invalid expiry date: {row['SEM_EXPIRY_DATE']}"
                return result

            # Get contract month from expiry date
            contract_month = expiry_date.strftime("%b").upper()

            # Determine if it's a stock or index future
            is_index = str(row["SEM_INSTRUMENT_NAME"]).startswith("FUTIDX")

            # Get lot size
            try:
                lot_size = int(float(row["SEM_LOT_UNITS"]))
            except (ValueError, TypeError):
                logger.error(f"Invalid lot size: {row['SEM_LOT_UNITS']}")
                lot_size = 1  # Default

            # Check if future already exists
            existing = db.query(Future).filter(
                Future.security_id == security.id).first()

            if existing:
                # Update existing future
                existing.underlying_id = underlying.id
                existing.expiration_date = expiry_date
                existing.lot_size = lot_size
                existing.contract_month = contract_month
                existing.is_active = True
                db.commit()

                result["status"] = "updated"
                result["future"] = existing
            else:
                # Create new future
                new_future = Future(
                    security_id=security.id,
                    underlying_id=underlying.id,
                    expiration_date=expiry_date,
                    contract_size=1.0,
                    lot_size=lot_size,
                    settlement_type="CASH" if is_index else "PHYSICAL",
                    contract_month=contract_month,
                    is_active=True)

                db.add(new_future)

                try:
                    db.commit()
                    result["status"] = "created"
                    result["future"] = new_future
                except SQLAlchemyError as e:
                    db.rollback()
                    # Check if the future now exists (created by another thread)
                    check_existing = db.query(Future).filter(
                        Future.security_id == security.id).first()
                    if check_existing:
                        # Update it
                        check_existing.underlying_id = underlying.id
                        check_existing.expiration_date = expiry_date
                        check_existing.lot_size = lot_size
                        check_existing.contract_month = contract_month
                        check_existing.is_active = True
                        db.commit()

                        result["status"] = "updated"
                        result["future"] = check_existing
                    else:
                        result["status"] = "error"
                        result["error"] = str(e)
                        logger.error(
                            f"Failed to create future for {symbol}: {e}")

        except Exception as e:
            db.rollback()
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(
                f"Error processing future {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}"
            )
        finally:
            db.close()  # Always close the session

        return result

    def process_futures_chunk(self, chunk: pd.DataFrame) -> Dict[str, int]:
        """Process a chunk of futures data in small batches.

        Args:
            chunk: DataFrame with futures to process

        Returns:
            Statistics dictionary
        """
        stats = {"created": 0, "updated": 0, "skipped": 0, "errors": 0}

        # Break into smaller batches for processing
        for i in range(0, len(chunk), BATCH_SIZE):
            batch = chunk.iloc[i:i + BATCH_SIZE]

            for _, row in batch.iterrows():
                # Process each future without using a cache
                result = self.process_future(row)

                if result["status"] == "created":
                    stats["created"] += 1
                elif result["status"] == "updated":
                    stats["updated"] += 1
                elif result["status"] == "skipped":
                    stats["skipped"] += 1
                else:
                    stats["errors"] += 1

        return stats

    def process_futures(self, futures_df: pd.DataFrame) -> None:
        """Process all futures in parallel.

        Args:
            futures_df: DataFrame with futures to process
        """
        start_time = time.time()
        logger.info(
            f"Processing {len(futures_df)} NSE futures with {self.workers} workers"
        )

        # Split data into chunks
        chunk_size = 100
        chunks = [
            futures_df.iloc[i:i + chunk_size]
            for i in range(0, len(futures_df), chunk_size)
        ]
        logger.info(
            f"Split data into {len(chunks)} chunks of {chunk_size} records")

        total_stats = {"created": 0, "updated": 0, "skipped": 0, "errors": 0}

        # Log progress periodically instead of using tqdm
        if self.show_progress:
            logger.info(
                f"Processing {len(futures_df)} NSE futures in {len(chunks)} chunks..."
            )

        # Process chunks in parallel
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.workers) as executor:
            future_to_chunk = {
                executor.submit(self.process_futures_chunk, chunk): i
                for i, chunk in enumerate(chunks)
            }

            completed = 0
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk_idx = future_to_chunk[future]
                completed += 1

                # Log progress every 10% or 5 chunks, whichever is more frequent
                log_interval = max(len(chunks) // 10, 5)
                if completed % log_interval == 0 or completed == len(chunks):
                    if self.show_progress:
                        progress_pct = (completed / len(chunks)) * 100
                        logger.info(
                            f"Progress: {completed}/{len(chunks)} chunks ({progress_pct:.1f}%)..."
                        )

                try:
                    stats = future.result()
                    # Aggregate stats
                    for key in total_stats:
                        total_stats[key] += stats[key]

                    logger.info(
                        f"Completed futures chunk {chunk_idx+1}/{len(chunks)}: {stats}"
                    )
                except Exception as e:
                    logger.error(f"Error in futures chunk {chunk_idx}: {e}")
                    total_stats["errors"] += len(chunks[chunk_idx])

        duration = time.time() - start_time
        logger.info(
            f"Finished processing futures in {duration:.2f} seconds: {total_stats}"
        )
        if self.show_progress:
            logger.info(
                f"Futures processing complete: {total_stats['created']} created, {total_stats['updated']} updated, {total_stats['errors']} errors"
            )

        # Update metrics
        self.metrics["futures_total"] = len(futures_df)
        self.metrics["futures_created"] = total_stats["created"]
        self.metrics["futures_updated"] = total_stats["updated"]
        self.metrics["futures_skipped"] = total_stats["skipped"]
        self.metrics["errors"] += total_stats["errors"]

    def mark_expired_futures(self) -> int:
        """Mark futures that have expired as inactive.

        Returns:
            Number of futures marked as inactive
        """
        today = date.today()
        marked_count = 0

        # Using a dedicated session for this operation
        with get_db() as db:
            try:
                # Get futures that have expired but are still marked active
                expired_futures = db.query(Future).filter(
                    Future.expiration_date < today,
                    Future.is_active == True).all()

                if self.show_progress and expired_futures:
                    logger.info(
                        f"Marking {len(expired_futures)} expired futures as inactive..."
                    )

                # Mark them as inactive
                for future in expired_futures:
                    future.is_active = False
                    marked_count += 1

                db.commit()
                logger.info(
                    f"Marked {marked_count} expired futures as inactive")

            except SQLAlchemyError as e:
                db.rollback()
                logger.error(f"Error marking expired futures: {e}")

        return marked_count

    def run(self) -> Dict[str, Any]:
        """Run the full import process.

        Returns:
            Metrics dictionary
        """
        start_time = time.time()
        logger.info(
            f"Starting NSE securities import (full_refresh={self.full_refresh})"
        )

        if self.show_progress:
            logger.info(
                f"\n{'='*80}\n QuantPulse NSE Securities Import\n{'='*80}")
            logger.info(
                f" Mode: {'Full Refresh' if self.full_refresh else 'Incremental Update'}"
            )
            logger.info(f" Workers: {self.workers}")
            logger.info(
                f" Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            logger.info(f" Importing only NSE securities")

        try:
            # Fetch and filter data
            df = self.fetch_securities_data()
            securities_df, futures_df = self.filter_data(df)

            # Process securities
            self.process_securities(securities_df)

            # Process futures
            self.process_futures(futures_df)

            # Mark expired futures
            marked_expired = self.mark_expired_futures()

            # Update final metrics
            self.metrics["runtime_seconds"] = time.time() - start_time

            logger.info(
                f"Import completed in {self.metrics['runtime_seconds']:.2f} seconds"
            )

            if self.show_progress:
                logger.info(f"\n{'='*80}")
                logger.info(
                    f" Import completed in {self.metrics['runtime_seconds']:.2f} seconds"
                )
                logger.info(
                    f" Securities: {self.metrics['securities_created']} created, {self.metrics['securities_updated']} updated"
                )
                logger.info(
                    f" Futures: {self.metrics['futures_created']} created, {self.metrics['futures_updated']} updated"
                )
                logger.info(f" Errors: {self.metrics['errors']}")
                logger.info(f"{'='*80}\n")

            return self.metrics

        except Exception as e:
            logger.error(f"Error in securities import: {e}", exc_info=True)
            self.metrics["errors"] += 1
            self.metrics["runtime_seconds"] = time.time() - start_time

            if self.show_progress:
                logger.error(f"\n{'='*80}")
                logger.error(
                    f" Import FAILED after {self.metrics['runtime_seconds']:.2f} seconds"
                )
                logger.error(f" Error: {str(e)}")
                logger.error(f"{'='*80}\n")

            return self.metrics


# CLI Entry point
def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Import securities from Dhan API")
    parser.add_argument("--full",
                        action="store_true",
                        help="Perform full refresh")
    parser.add_argument("--workers",
                        type=int,
                        default=8,
                        help="Number of worker threads")
    parser.add_argument("--no-progress",
                        action="store_true",
                        help="Hide progress bars")
    args = parser.parse_args()

    # Don't pass a db session to the importer - it will create its own sessions
    importer = SecuritiesImporter(workers=args.workers,
                                  full_refresh=args.full,
                                  show_progress=not args.no_progress)
    metrics = importer.run()

    # Return 1 if there were errors
    return 0 if metrics["errors"] == 0 else 1


# Function for API integration
def import_securities_api(background_tasks=None,
                          workers=8,
                          full_refresh=False):
    """Run securities import from API endpoint.

    Args:
        background_tasks: Optional FastAPI BackgroundTasks object
        workers: Number of worker threads
        full_refresh: Whether to perform full refresh

    Returns:
        Metrics dictionary or task ID if running in background
    """

    def _run_import():
        # Create importer without passing a db session
        importer = SecuritiesImporter(
            workers=workers, full_refresh=full_refresh,
            show_progress=False)  # No progress bars in API mode
        return importer.run()

    # If background_tasks is provided, run in background
    if background_tasks:
        task_id = str(uuid.uuid4())
        background_tasks.add_task(_run_import)
        return {"task_id": task_id, "status": "started"}

    # Otherwise, run synchronously
    return _run_import()


if __name__ == "__main__":
    exit(main())
