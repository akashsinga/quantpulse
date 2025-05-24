# backend/app/tasks/securities_import.py

import os
import sys

# Add the backend directory to Python path (same as main.py)
sys.path.insert(
    0,
    os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))))

import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from typing import Dict

from app.core.celery_app import celery_app
from app.db.session import SessionLocal
from .securities_import_helpers import (filter_securities_and_futures,
                                        validate_security_data,
                                        validate_futures_data)
from .securities_import_db import (ensure_nse_exchange, save_securities_batch,
                                   save_futures_batch, mark_expired_futures)
from utils.logger import get_logger

logger = get_logger(__name__)


@celery_app.task(bind=True, name="import_securities")
def import_securities_task(self) -> Dict:
    """
    Celery task to import securities and futures from Dhan API
    Complete implementation with securities and futures processing
    """
    task_id = self.request.id
    start_time = datetime.now()
    logger.info(f"Starting securities import task {task_id}")

    # Initialize progress
    self.update_state(state='PROGRESS',
                      meta={
                          'status': 'Starting download...',
                          'progress': 0
                      })

    try:
        # Step 1: Download data from Dhan API
        logger.info("Downloading securities data from Dhan API")
        self.update_state(state='PROGRESS',
                          meta={
                              'status': 'Downloading data from Dhan API...',
                              'progress': 5
                          })

        response = requests.get(
            "https://images.dhan.co/api-data/api-scrip-master.csv",
            timeout=120  # Increased timeout for large file
        )
        response.raise_for_status()

        # Step 2: Parse CSV data
        self.update_state(state='PROGRESS',
                          meta={
                              'status': 'Parsing CSV data...',
                              'progress': 15
                          })

        df = pd.read_csv(StringIO(response.text), low_memory=False)
        total_records = len(df)
        logger.info(f"Downloaded {total_records} total records")

        # Step 3: Filter and separate securities and futures
        self.update_state(state='PROGRESS',
                          meta={
                              'status':
                              'Filtering NSE securities and futures...',
                              'progress': 25
                          })

        securities_df, futures_df = filter_securities_and_futures(df)

        if len(securities_df) == 0:
            raise Exception("No NSE securities found in the data")

        logger.info(
            f"Filtered {len(securities_df)} securities and {len(futures_df)} futures"
        )

        # Step 4: Validate data quality
        self.update_state(state='PROGRESS',
                          meta={
                              'status': 'Validating data quality...',
                              'progress': 30
                          })

        securities_df = validate_and_clean_securities(securities_df)
        futures_df = validate_and_clean_futures(futures_df)

        logger.info(
            f"After validation: {len(securities_df)} securities, {len(futures_df)} futures"
        )

        # Step 5: Database operations
        db = SessionLocal()
        try:
            # Ensure NSE exchange exists
            self.update_state(state='PROGRESS',
                              meta={
                                  'status': 'Setting up database...',
                                  'progress': 35
                              })

            nse_exchange = ensure_nse_exchange(db)

            # Step 6: Save securities first (futures depend on them)
            self.update_state(
                state='PROGRESS',
                meta={
                    'status':
                    f'Saving {len(securities_df)} securities to database...',
                    'progress': 40
                })

            securities_stats = save_securities_batch(db, securities_df,
                                                     nse_exchange)
            logger.info(f"Securities saved: {securities_stats}")

            # Update progress after securities
            self.update_state(
                state='PROGRESS',
                meta={
                    'status':
                    f'Saving {len(futures_df)} futures to database...',
                    'progress': 70,
                    'securities_stats': securities_stats
                })

            # Step 7: Save futures (after securities exist)
            futures_stats = save_futures_batch(db, futures_df)
            logger.info(f"Futures saved: {futures_stats}")

            # Step 8: Mark expired futures as inactive
            self.update_state(state='PROGRESS',
                              meta={
                                  'status':
                                  'Marking expired futures as inactive...',
                                  'progress': 90
                              })

            expired_count = mark_expired_futures(db)

        finally:
            db.close()

        # Step 9: Final results
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        final_result = {
            'status': 'SUCCESS',
            'task_id': task_id,
            'duration_seconds': round(duration, 2),
            'started_at': start_time.isoformat(),
            'completed_at': end_time.isoformat(),

            # Data statistics
            'total_downloaded': total_records,
            'total_filtered_securities': len(securities_df),
            'total_filtered_futures': len(futures_df),

            # Securities results
            'securities_created': securities_stats['created'],
            'securities_updated': securities_stats['updated'],
            'securities_skipped': securities_stats['skipped'],
            'securities_errors': securities_stats['errors'],

            # Futures results
            'futures_created': futures_stats['created'],
            'futures_updated': futures_stats['updated'],
            'futures_skipped': futures_stats['skipped'],
            'futures_errors': futures_stats['errors'],

            # Maintenance
            'expired_futures_marked': expired_count,

            # Summary
            'total_errors':
            securities_stats['errors'] + futures_stats['errors']
        }

        logger.info(
            f"Securities import completed successfully: {final_result}")
        return final_result

    except requests.RequestException as e:
        error_msg = f"Failed to download securities data: {str(e)}"
        logger.error(error_msg)

        self.update_state(state='FAILURE',
                          meta={
                              'error': error_msg,
                              'task_id': task_id
                          })
        raise Exception(error_msg)

    except Exception as e:
        error_msg = f"Securities import failed: {str(e)}"
        logger.error(error_msg, exc_info=True)

        self.update_state(state='FAILURE',
                          meta={
                              'error': error_msg,
                              'task_id': task_id
                          })
        raise Exception(error_msg)


def validate_and_clean_securities(securities_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and clean securities data, removing invalid records
    """
    logger.info(f"Validating {len(securities_df)} securities records")

    valid_records = []
    invalid_count = 0

    for index, row in securities_df.iterrows():
        is_valid, error = validate_security_data(row)
        if is_valid:
            valid_records.append(row)
        else:
            invalid_count += 1
            if invalid_count <= 10:  # Log first 10 errors
                logger.warning(
                    f"Invalid security record: {error} - {row.get('SEM_TRADING_SYMBOL', 'unknown')}"
                )

    if invalid_count > 0:
        logger.warning(f"Removed {invalid_count} invalid securities records")

    return pd.DataFrame(valid_records)


def validate_and_clean_futures(futures_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and clean futures data, removing invalid records
    """
    logger.info(f"Validating {len(futures_df)} futures records")

    valid_records = []
    invalid_count = 0

    for index, row in futures_df.iterrows():
        is_valid, error = validate_futures_data(row)
        if is_valid:
            valid_records.append(row)
        else:
            invalid_count += 1
            if invalid_count <= 10:  # Log first 10 errors
                logger.warning(
                    f"Invalid future record: {error} - {row.get('SEM_TRADING_SYMBOL', 'unknown')}"
                )

    if invalid_count > 0:
        logger.warning(
            f"Removed {invalid_count} invalid/expired futures records")

    return pd.DataFrame(valid_records)
