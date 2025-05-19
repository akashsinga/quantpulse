#!/usr/bin/env python
"""
Securities Import System for QuantPulse

This script imports securities data from Dhan API into the QuantPulse database.
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
import logging
import pandas as pd
from typing import Dict, List, Tuple, Optional, Set, Any
from datetime import datetime, date
from sqlalchemy import func, or_, and_, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm
from app.db.session import get_db
from app.db.models.security import Security
from app.db.models.derivatives import Future
from app.db.models.exchange import Exchange
from utils.logger import get_logger
from app.config import settings

# Configure logging
logger = get_logger(__name__)

# Constants
CSV_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
CSV_BACKUP_DIR = os.path.join(settings.CACHE_DIR, "securities_data")
BATCH_SIZE = 10  # Process in very small batches to avoid SQL limitations


class SecuritiesImporter:
    """Handles importing securities from Dhan API into QuantPulse database."""

    def __init__(self, db: Session, workers: int = 8, full_refresh: bool = False, show_progress: bool = True):
        """Initialize the importer.

        Args:
            db: Database session
            workers: Number of worker threads for parallel processing
            full_refresh: Whether to perform a full refresh (vs incremental)
            show_progress: Whether to show progress bars (CLI mode)
        """
        self.db = db
        self.workers = workers
        self.full_refresh = full_refresh
        self.show_progress = show_progress
        self.exchanges_map = {}
        self.metrics = {"securities_total": 0, "securities_created": 0, "securities_updated": 0, "securities_skipped": 0, "futures_total": 0, "futures_created": 0, "futures_updated": 0, "futures_skipped": 0, "errors": 0, "runtime_seconds": 0}

        # Load exchange mapping
        self._load_exchanges()

        # Ensure backup directory exists
        os.makedirs(CSV_BACKUP_DIR, exist_ok=True)

        # Check and increase symbol column length if needed
        self._ensure_symbol_column_length()

    def _load_exchanges(self) -> None:
        """Load exchanges from database and create a lookup map."""
        exchanges = self.db.query(Exchange).all()
        for exchange in exchanges:
            self.exchanges_map[exchange.code] = exchange.id

        # If exchanges don't exist yet, create them
        if "NSE" not in self.exchanges_map:
            nse = Exchange(id=uuid.uuid4(), name="National Stock Exchange of India", code="NSE", country="India", timezone="Asia/Kolkata", is_active=True)
            self.db.add(nse)
            self.exchanges_map["NSE"] = nse.id

        if "BSE" not in self.exchanges_map:
            bse = Exchange(id=uuid.uuid4(), name="Bombay Stock Exchange", code="BSE", country="India", timezone="Asia/Kolkata", is_active=True)
            self.db.add(bse)
            self.exchanges_map["BSE"] = bse.id

        if "NSE" not in self.exchanges_map or "BSE" not in self.exchanges_map:
            self.db.commit()

    def _ensure_symbol_column_length(self) -> None:
        """Check and increase symbol column length if needed."""
        try:
            # Check current column length
            result = self.db.execute(
                text(
                    """
                SELECT character_maximum_length 
                FROM information_schema.columns 
                WHERE table_name = 'securities' AND column_name = 'symbol'
            """
                )
            ).scalar()

            if result and int(result) < 50:
                # Column is too short, increase it
                logger.info("Increasing securities.symbol column length to 50 characters")
                self.db.execute(
                    text(
                        """
                    ALTER TABLE securities 
                    ALTER COLUMN symbol TYPE varchar(50)
                """
                    )
                )
                self.db.commit()
                logger.info("Column length increased successfully")
            else:
                logger.info("Securities.symbol column length is already sufficient")

        except Exception as e:
            logger.error(f"Error checking/updating symbol column length: {e}")
            # Continue anyway, we'll catch insert errors later

    def fetch_securities_data(self) -> pd.DataFrame:
        """Download and parse securities data from Dhan API.

        Returns:
            DataFrame with securities data
        """
        start_time = time.time()
        logger.info(f"Downloading securities data from {CSV_URL}")

        try:
            # Create a progress bar for the download
            with tqdm(total=None, unit="B", unit_scale=True, desc="Downloading", disable=not self.show_progress) as t:
                response = requests.get(CSV_URL, timeout=60, stream=True)
                response.raise_for_status()

                # Get the content length if available
                total_size = int(response.headers.get("content-length", 0))
                t.total = total_size

                # Download with progress tracking
                content = bytearray()
                for chunk in response.iter_content(chunk_size=8192):
                    content.extend(chunk)
                    t.update(len(chunk))

            # Save backup copy
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = os.path.join(CSV_BACKUP_DIR, f"dhan_securities_{timestamp}.csv")
            with open(backup_path, "wb") as f:
                f.write(content)
            logger.info(f"Saved backup to {backup_path}")

            # Parse CSV with a progress bar
            logger.info("Parsing CSV data...")
            if self.show_progress:
                print("Parsing CSV data...", end="", flush=True)

            csv_content = content.decode("utf-8")
            df = pd.read_csv(io.StringIO(csv_content))

            if self.show_progress:
                print(f" Done! ({len(df)} records)")

            logger.info(f"Downloaded and parsed {len(df)} records in {time.time() - start_time:.2f} seconds")
            return df

        except requests.RequestException as e:
            logger.error(f"Error downloading securities data: {e}")

            # Try to use latest backup if available
            backup_files = sorted([f for f in os.listdir(CSV_BACKUP_DIR) if f.startswith("dhan_securities_")])
            if backup_files:
                latest_backup = os.path.join(CSV_BACKUP_DIR, backup_files[-1])
                logger.info(f"Using latest backup from {latest_backup}")

                if self.show_progress:
                    print(f"Download failed. Using backup from {latest_backup}")

                return pd.read_csv(latest_backup)

            raise RuntimeError("Failed to download securities data and no backup available")

    def filter_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Filter data to include only relevant securities.

        Args:
            df: DataFrame with all securities

        Returns:
            Tuple of (securities_df, futures_df)
        """
        # Filter main securities (stocks and indices)
        securities_df = df[(df["SEM_SEGMENT"] == "E") | ((df["SEM_SEGMENT"] == "I") & (df["SEM_EXCH_INSTRUMENT_TYPE"] == "INDXX"))]

        # Filter futures
        futures_df = df[(df["SEM_SEGMENT"] == "D") & (df["SEM_INSTRUMENT_NAME"].str.startswith("FUTIDX") | df["SEM_INSTRUMENT_NAME"].str.startswith("FUTSTK"))]

        logger.info(f"Filtered {len(securities_df)} securities and {len(futures_df)} futures")

        if self.show_progress:
            print(f"Filtered {len(securities_df)} securities and {len(futures_df)} futures")

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

        try:
            # Get exchange ID
            exchange_id = self.exchanges_map.get(row["SEM_EXM_EXCH_ID"])
            if not exchange_id:
                result["status"] = "skipped"
                result["error"] = f"Exchange not found: {row['SEM_EXM_EXCH_ID']}"
                return result

            # Check if security already exists
            external_id = int(row["SEM_SMST_SECURITY_ID"])
            existing = self.db.query(Security).filter(Security.external_id == external_id).first()

            security_type = self.map_to_security_type(row)

            # Get name (handle NaN values)
            name = row["SM_SYMBOL_NAME"] if pd.notna(row["SM_SYMBOL_NAME"]) else row["SEM_TRADING_SYMBOL"]

            if existing:
                # Update existing security
                existing.symbol = row["SEM_TRADING_SYMBOL"]
                existing.name = name
                existing.security_type = security_type
                existing.segment = "EQUITY"  # All are EQUITY for now
                existing.is_active = True
                existing.updated_at = datetime.now()
                self.db.commit()

                result["status"] = "updated"
                result["security"] = existing
            else:
                # Create new security
                new_security = Security(id=uuid.uuid4(), symbol=row["SEM_TRADING_SYMBOL"], name=name, exchange_id=exchange_id, security_type=security_type, segment="EQUITY", external_id=external_id, is_active=True)  # All are EQUITY for now
                self.db.add(new_security)
                self.db.commit()

                result["status"] = "created"
                result["security"] = new_security

        except Exception as e:
            self.db.rollback()
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(f"Error processing security {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}")

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
            batch = chunk.iloc[i : i + BATCH_SIZE]

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
        logger.info(f"Processing {len(securities_df)} securities with {self.workers} workers")

        # Split data into chunks
        chunk_size = 100  # Larger chunks for distribution to workers
        chunks = [securities_df.iloc[i : i + chunk_size] for i in range(0, len(securities_df), chunk_size)]
        logger.info(f"Split data into {len(chunks)} chunks of {chunk_size} records")

        total_stats = {"created": 0, "updated": 0, "skipped": 0, "errors": 0}

        # Process chunks in parallel with progress bar
        with tqdm(total=len(chunks), desc="Processing securities", unit="chunk", disable=not self.show_progress) as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
                future_to_chunk = {executor.submit(self.process_securities_chunk, chunk): i for i, chunk in enumerate(chunks)}

                for future in concurrent.futures.as_completed(future_to_chunk):
                    chunk_idx = future_to_chunk[future]
                    try:
                        stats = future.result()
                        # Aggregate stats
                        for key in total_stats:
                            total_stats[key] += stats[key]

                        # Update progress bar
                        pbar.update(1)
                        pbar.set_postfix({"created": total_stats["created"], "updated": total_stats["updated"], "errors": total_stats["errors"]})

                        logger.info(f"Completed chunk {chunk_idx+1}/{len(chunks)}: {stats}")
                    except Exception as e:
                        logger.error(f"Error in chunk {chunk_idx}: {e}")
                        total_stats["errors"] += len(chunks[chunk_idx])
                        pbar.update(1)

        duration = time.time() - start_time
        logger.info(f"Finished processing securities in {duration:.2f} seconds: {total_stats}")

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

    def process_future(self, row: Dict, securities_cache: Dict = None) -> Dict:
        """Process a single future record with proper error handling.

        Args:
            row: Future data row
            securities_cache: Optional cache of previously processed securities

        Returns:
            Result dictionary
        """
        result = {"status": "unknown", "error": None, "future": None}

        try:
            # First ensure the security record exists
            external_id = int(row["SEM_SMST_SECURITY_ID"])

            # Check cache first if provided
            security = None
            if securities_cache and external_id in securities_cache:
                security = securities_cache[external_id]
            else:
                # Check database
                security = self.db.query(Security).filter(Security.external_id == external_id).first()

            # If security doesn't exist, create it
            if not security:
                security_result = self.process_security(row)
                if security_result["status"] in ["created", "updated"]:
                    security = security_result["security"]
                else:
                    result["status"] = "skipped"
                    result["error"] = f"Failed to create security: {security_result['error']}"
                    return result

            # Find underlying security
            underlying_symbol = self.extract_underlying_symbol(row)
            underlying = self.db.query(Security).filter(Security.symbol == underlying_symbol, Security.security_type.in_(["STOCK", "INDEX"])).first()

            if not underlying:
                result["status"] = "skipped"
                result["error"] = f"Underlying security not found: {underlying_symbol}"
                return result

            # Parse expiry date
            try:
                expiry_date = datetime.strptime(row["SEM_EXPIRY_DATE"].split()[0], "%Y-%m-%d").date()
            except (ValueError, AttributeError):
                result["status"] = "error"
                result["error"] = f"Invalid expiry date: {row['SEM_EXPIRY_DATE']}"
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
            existing = self.db.query(Future).filter(Future.security_id == security.id).first()

            if existing:
                # Update existing future
                existing.underlying_id = underlying.id
                existing.expiration_date = expiry_date
                existing.lot_size = lot_size
                existing.contract_month = contract_month
                existing.is_active = True
                self.db.commit()

                result["status"] = "updated"
                result["future"] = existing
            else:
                # Create new future
                new_future = Future(security_id=security.id, underlying_id=underlying.id, expiration_date=expiry_date, contract_size=1.0, lot_size=lot_size, settlement_type="CASH" if is_index else "PHYSICAL", contract_month=contract_month, is_active=True)  # Standard for Indian markets
                self.db.add(new_future)
                self.db.commit()

                result["status"] = "created"
                result["future"] = new_future

        except Exception as e:
            self.db.rollback()
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(f"Error processing future {row.get('SEM_TRADING_SYMBOL', 'unknown')}: {e}")

        return result

    def process_futures_chunk(self, chunk: pd.DataFrame) -> Dict[str, int]:
        """Process a chunk of futures data in small batches.

        Args:
            chunk: DataFrame with futures to process

        Returns:
            Statistics dictionary
        """
        stats = {"created": 0, "updated": 0, "skipped": 0, "errors": 0}

        # Create a securities cache for this chunk
        securities_cache = {}

        # Break into smaller batches for processing
        for i in range(0, len(chunk), BATCH_SIZE):
            batch = chunk.iloc[i : i + BATCH_SIZE]

            for _, row in batch.iterrows():
                result = self.process_future(row, securities_cache)

                if result["status"] == "created":
                    stats["created"] += 1
                elif result["status"] == "updated":
                    stats["updated"] += 1
                elif result["status"] == "skipped":
                    stats["skipped"] += 1
                else:
                    stats["errors"] += 1

                # Update cache if future was created/updated and has a security
                if result["status"] in ["created", "updated"] and result["future"]:
                    external_id = int(row["SEM_SMST_SECURITY_ID"])
                    securities_cache[external_id] = result["future"].security

        return stats

    def process_futures(self, futures_df: pd.DataFrame) -> None:
        """Process all futures in parallel.

        Args:
            futures_df: DataFrame with futures to process
        """
        start_time = time.time()
        logger.info(f"Processing {len(futures_df)} futures with {self.workers} workers")

        # Split data into chunks
        chunk_size = 100  # Larger chunks for distribution to workers
        chunks = [futures_df.iloc[i : i + chunk_size] for i in range(0, len(futures_df), chunk_size)]
        logger.info(f"Split data into {len(chunks)} chunks of {chunk_size} records")

        total_stats = {"created": 0, "updated": 0, "skipped": 0, "errors": 0}

        # Process chunks in parallel with progress bar
        with tqdm(total=len(chunks), desc="Processing futures", unit="chunk", disable=not self.show_progress) as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
                future_to_chunk = {executor.submit(self.process_futures_chunk, chunk): i for i, chunk in enumerate(chunks)}

                for future in concurrent.futures.as_completed(future_to_chunk):
                    chunk_idx = future_to_chunk[future]
                    try:
                        stats = future.result()
                        # Aggregate stats
                        for key in total_stats:
                            total_stats[key] += stats[key]

                        # Update progress bar
                        pbar.update(1)
                        pbar.set_postfix({"created": total_stats["created"], "updated": total_stats["updated"], "errors": total_stats["errors"]})

                        logger.info(f"Completed futures chunk {chunk_idx+1}/{len(chunks)}: {stats}")
                    except Exception as e:
                        logger.error(f"Error in futures chunk {chunk_idx}: {e}")
                        total_stats["errors"] += len(chunks[chunk_idx])
                        pbar.update(1)

        duration = time.time() - start_time
        logger.info(f"Finished processing futures in {duration:.2f} seconds: {total_stats}")

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

        try:
            # Get futures that have expired but are still marked active
            expired_futures = self.db.query(Future).filter(Future.expiration_date < today, Future.is_active == True).all()

            if self.show_progress and expired_futures:
                print(f"Marking {len(expired_futures)} expired futures as inactive...")

            # Mark them as inactive
            for future in expired_futures:
                future.is_active = False

            self.db.commit()
            logger.info(f"Marked {len(expired_futures)} expired futures as inactive")
            return len(expired_futures)

        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Error marking expired futures: {e}")
            return 0

    def run(self) -> Dict[str, Any]:
        """Run the full import process.

        Returns:
            Metrics dictionary
        """
        start_time = time.time()
        logger.info(f"Starting securities import (full_refresh={self.full_refresh})")

        if self.show_progress:
            print(f"\n{'='*80}\n QuantPulse Securities Import\n{'='*80}")
            print(f" Mode: {'Full Refresh' if self.full_refresh else 'Incremental Update'}")
            print(f" Workers: {self.workers}")
            print(f" Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

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

            logger.info(f"Import completed in {self.metrics['runtime_seconds']:.2f} seconds")

            if self.show_progress:
                print(f"\n{'='*80}")
                print(f" Import completed in {self.metrics['runtime_seconds']:.2f} seconds")
                print(f" Securities: {self.metrics['securities_created']} created, {self.metrics['securities_updated']} updated")
                print(f" Futures: {self.metrics['futures_created']} created, {self.metrics['futures_updated']} updated")
                print(f" Errors: {self.metrics['errors']}")
                print(f"{'='*80}\n")

            return self.metrics

        except Exception as e:
            logger.error(f"Error in securities import: {e}", exc_info=True)
            self.metrics["errors"] += 1
            self.metrics["runtime_seconds"] = time.time() - start_time

            if self.show_progress:
                print(f"\n{'='*80}")
                print(f" Import FAILED after {self.metrics['runtime_seconds']:.2f} seconds")
                print(f" Error: {str(e)}")
                print(f"{'='*80}\n")

            return self.metrics


# CLI Entry point
def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Import securities from Dhan API")
    parser.add_argument("--full", action="store_true", help="Perform full refresh")
    parser.add_argument("--workers", type=int, default=8, help="Number of worker threads")
    parser.add_argument("--no-progress", action="store_true", help="Hide progress bars")
    args = parser.parse_args()

    with get_db() as db:
        importer = SecuritiesImporter(db, workers=args.workers, full_refresh=args.full, show_progress=not args.no_progress)
        metrics = importer.run()

    # Return 1 if there were errors
    return 0 if metrics["errors"] == 0 else 1


# Function for API integration
def import_securities_api(background_tasks=None, workers=8, full_refresh=False):
    """Run securities import from API endpoint.

    Args:
        background_tasks: Optional FastAPI BackgroundTasks object
        workers: Number of worker threads
        full_refresh: Whether to perform full refresh

    Returns:
        Metrics dictionary or task ID if running in background
    """

    def _run_import():
        with get_db() as db:
            importer = SecuritiesImporter(db, workers=workers, full_refresh=full_refresh, show_progress=False)  # No progress bars in API mode
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
