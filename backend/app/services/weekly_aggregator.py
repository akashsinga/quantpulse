# app/services/weekly_aggregator.py

from typing import List, Dict, Any, Optional, Callable
from datetime import datetime, date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, text, func, select
from sqlalchemy.dialects.postgresql import insert
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid
import math

from app.db.session import get_db
from app.db.models.security import Security
from app.db.models.ohlcv_daily import OHLCVDaily
from app.db.models.ohlcv_weekly import OHLCVWeekly
from app.utils.logger import get_logger

logger = get_logger(__name__)


class WeeklyDataAggregator:
    """
    Optimized service to generate weekly OHLCV data from daily data
    Uses TimescaleDB's time_bucket for efficient aggregation and batch processing
    """

    def __init__(self, batch_size: int = 100, max_workers: int = 4):
        self.batch_size = batch_size
        self.max_workers = max_workers
        logger.info(f"Initialized Weekly Data Aggregator (batch_size={batch_size}, workers={max_workers})")

    def generate_weekly_data(self, security_ids: Optional[List[uuid.UUID]] = None, weeks_back: int = 4, progress_callback: Optional[Callable[[int], None]] = None) -> Dict[str, Any]:
        """
        Generate weekly OHLCV data from daily data using optimized batch processing
        
        Args:
            security_ids: List of security UUIDs to process (None = all)
            weeks_back: Number of weeks back to regenerate
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dict with aggregation results
        """
        start_time = datetime.now()

        try:
            with get_db() as db:
                # Get securities to process
                securities = self._get_securities_to_process(db, security_ids)

                if not securities:
                    return {'status': 'SUCCESS', 'message': 'No securities to process', 'processed': 0}

                logger.info(f"Generating weekly data for {len(securities)} securities")

                # Calculate date range
                end_date = date.today()
                start_date = end_date - timedelta(weeks=weeks_back * 7)

                # Check if TimescaleDB is available for optimized processing
                use_timescaledb = self._check_timescaledb_available(db)

                if use_timescaledb:
                    logger.info("Using TimescaleDB optimized aggregation")
                    result = self._generate_weekly_with_timescaledb(db, securities, start_date, end_date, progress_callback)
                else:
                    logger.info("Using standard batch processing")
                    result = self._generate_weekly_with_batching(db, securities, start_date, end_date, progress_callback)

                duration = (datetime.now() - start_time).total_seconds()
                result['duration_seconds'] = round(duration, 2)
                result['date_range'] = f"{start_date} to {end_date}"

                logger.info(f"Weekly data generation completed: {result}")
                return result

        except Exception as e:
            logger.error(f"Error in weekly data generation: {e}")
            raise

    def _get_securities_to_process(self, db: Session, security_ids: Optional[List[uuid.UUID]]) -> List[Security]:
        """Get securities to process with optimized query"""
        query = db.query(Security).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX'])))

        if security_ids:
            query = query.filter(Security.id.in_(security_ids))

        return query.all()

    def _check_timescaledb_available(self, db: Session) -> bool:
        """Check if TimescaleDB extension is available"""
        try:
            result = db.execute(text("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb');")).scalar()
            return bool(result)
        except:
            return False

    def _generate_weekly_with_timescaledb(self, db: Session, securities: List[Security], start_date: date, end_date: date, progress_callback: Optional[Callable[[int], None]]) -> Dict[str, Any]:
        """
        Generate weekly data using TimescaleDB's time_bucket function for optimal performance
        """
        security_ids = [sec.id for sec in securities]
        total_records = 0

        try:
            # Use TimescaleDB's time_bucket for efficient aggregation
            # This query aggregates all securities and weeks in a single operation
            aggregation_query = text("""
                WITH weekly_aggregates AS (
                    SELECT 
                        time_bucket('1 week', time, 'Monday') as week_start,
                        security_id,
                        (array_agg(open ORDER BY time))[1] as week_open,
                        MAX(high) as week_high,
                        MIN(low) as week_low,
                        (array_agg(close ORDER BY time DESC))[1] as week_close,
                        SUM(volume) as week_volume
                    FROM ohlcv_daily
                    WHERE security_id = ANY(:security_ids)
                        AND time >= :start_date
                        AND time <= :end_date
                    GROUP BY time_bucket('1 week', time, 'Monday'), security_id
                    HAVING COUNT(*) > 0
                )
                SELECT 
                    week_start,
                    security_id,
                    week_open,
                    week_high,
                    week_low,
                    week_close,
                    week_volume
                FROM weekly_aggregates
                ORDER BY security_id, week_start
            """)

            # Execute the aggregation query
            result = db.execute(aggregation_query, {'security_ids': security_ids, 'start_date': datetime.combine(start_date, datetime.min.time()), 'end_date': datetime.combine(end_date, datetime.max.time())})

            # Prepare records for bulk insert
            weekly_records = []
            for row in result:
                weekly_records.append({'time': row.week_start, 'security_id': row.security_id, 'open': row.week_open, 'high': row.week_high, 'low': row.week_low, 'close': row.week_close, 'volume': row.week_volume, 'adjusted_close': None})

            # Bulk insert/update in chunks
            if weekly_records:
                total_records = self._bulk_upsert_weekly_data_chunked(db, weekly_records, progress_callback)

            return {'status': 'SUCCESS', 'processed_securities': len(securities), 'total_weekly_records': total_records, 'method': 'timescaledb_optimized'}

        except Exception as e:
            logger.error(f"TimescaleDB aggregation failed: {e}")
            raise

    def _generate_weekly_with_batching(self, db: Session, securities: List[Security], start_date: date, end_date: date, progress_callback: Optional[Callable[[int], None]]) -> Dict[str, Any]:
        """
        Generate weekly data using batch processing with parallel execution
        """
        total_records = 0
        processed_securities = 0

        # Process securities in batches
        security_batches = [securities[i:i + self.batch_size] for i in range(0, len(securities), self.batch_size)]

        logger.info(f"Processing {len(securities)} securities in {len(security_batches)} batches")

        for batch_idx, batch in enumerate(security_batches):
            try:
                # Process batch with parallel workers
                batch_records = self._process_securities_batch_parallel(batch, start_date, end_date)

                if batch_records:
                    # Bulk insert batch records
                    inserted = self._bulk_upsert_weekly_data_chunked(db, batch_records)
                    total_records += inserted

                processed_securities += len(batch)

                # Update progress
                if progress_callback:
                    progress = int((processed_securities / len(securities)) * 100)
                    progress_callback(progress)

                logger.info(f"Completed batch {batch_idx + 1}/{len(security_batches)}: {len(batch_records)} records")

            except Exception as e:
                logger.error(f"Error processing batch {batch_idx + 1}: {e}")
                continue

        return {'status': 'SUCCESS', 'processed_securities': processed_securities, 'total_weekly_records': total_records, 'method': 'batch_parallel'}

    def _process_securities_batch_parallel(self, securities: List[Security], start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """
        Process a batch of securities in parallel using ThreadPoolExecutor
        """
        all_records = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit tasks for each security
            future_to_security = {executor.submit(self._generate_weekly_for_security_optimized, sec.id, start_date, end_date): sec for sec in securities}

            # Collect results as they complete
            for future in as_completed(future_to_security):
                security = future_to_security[future]
                try:
                    records = future.result()
                    if records:
                        all_records.extend(records)
                except Exception as e:
                    logger.error(f"Error processing security {security.symbol}: {e}")

        return all_records

    def _generate_weekly_for_security_optimized(self, security_id: uuid.UUID, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """
        Generate weekly records for a specific security using optimized SQL
        """
        try:
            with get_db() as db:
                # Use a single query with window functions for efficiency
                query = text("""
                    WITH daily_with_week AS (
                        SELECT 
                            time,
                            open, high, low, close, volume,
                            date_trunc('week', time) as week_start
                        FROM ohlcv_daily
                        WHERE security_id = :security_id
                            AND time >= :start_date
                            AND time <= :end_date
                        ORDER BY time
                    ),
                    weekly_agg AS (
                        SELECT 
                            week_start,
                            (array_agg(open ORDER BY time))[1] as week_open,
                            MAX(high) as week_high,
                            MIN(low) as week_low,
                            (array_agg(close ORDER BY time DESC))[1] as week_close,
                            SUM(volume) as week_volume
                        FROM daily_with_week
                        GROUP BY week_start
                        HAVING COUNT(*) > 0
                    )
                    SELECT * FROM weekly_agg ORDER BY week_start
                """)

                result = db.execute(query, {'security_id': security_id, 'start_date': datetime.combine(start_date, datetime.min.time()), 'end_date': datetime.combine(end_date, datetime.max.time())})

                records = []
                for row in result:
                    records.append({'time': row.week_start, 'security_id': security_id, 'open': row.week_open, 'high': row.week_high, 'low': row.week_low, 'close': row.week_close, 'volume': row.week_volume, 'adjusted_close': None})

                return records

        except Exception as e:
            logger.error(f"Error generating weekly data for security {security_id}: {e}")
            return []

    def _bulk_upsert_weekly_data_chunked(self, db: Session, records: List[Dict[str, Any]], progress_callback: Optional[Callable[[int], None]] = None) -> int:
        """
        Bulk insert/update weekly OHLCV records in chunks for better performance
        """
        if not records:
            return 0

        chunk_size = 1000  # Optimize chunk size for PostgreSQL
        total_processed = 0
        total_chunks = math.ceil(len(records) / chunk_size)

        try:
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]

                # Use PostgreSQL's ON CONFLICT for upsert
                stmt = insert(OHLCVWeekly).values(chunk)
                stmt = stmt.on_conflict_do_update(index_elements=['time', 'security_id'], set_={'open': stmt.excluded.open, 'high': stmt.excluded.high, 'low': stmt.excluded.low, 'close': stmt.excluded.close, 'volume': stmt.excluded.volume, 'adjusted_close': stmt.excluded.adjusted_close})

                db.execute(stmt)
                db.commit()  # Commit each chunk to avoid long transactions

                total_processed += len(chunk)

                # Update progress if callback provided
                if progress_callback and total_chunks > 1:
                    progress = int((i / len(records)) * 100)
                    progress_callback(progress)

                logger.debug(f"Processed chunk {(i // chunk_size) + 1}/{total_chunks}: {len(chunk)} records")

            return total_processed

        except Exception as e:
            db.rollback()
            logger.error(f"Error in chunked bulk upsert: {e}")
            raise

    def generate_weekly_for_date_range(self, security_id: uuid.UUID, start_date: date, end_date: date) -> Dict[str, Any]:
        """
        Generate weekly data for a specific security and date range
        """
        try:
            records = self._generate_weekly_for_security_optimized(security_id, start_date, end_date)

            if records:
                with get_db() as db:
                    inserted = self._bulk_upsert_weekly_data_chunked(db, records)
                    return {'status': 'SUCCESS', 'records_generated': inserted, 'date_range': f"{start_date} to {end_date}"}
            else:
                return {'status': 'SUCCESS', 'records_generated': 0, 'message': 'No daily data available for weekly aggregation'}

        except Exception as e:
            logger.error(f"Error generating weekly data for security {security_id}: {e}")
            return {'status': 'FAILED', 'error': str(e)}

    def get_weekly_data_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about weekly data coverage using optimized queries
        """
        try:
            with get_db() as db:
                # Use a single query with multiple aggregations
                stats_query = text("""
                    SELECT 
                        (SELECT COUNT(DISTINCT security_id) FROM ohlcv_weekly) as securities_with_weekly,
                        (SELECT COUNT(DISTINCT security_id) FROM ohlcv_daily) as securities_with_daily,
                        (SELECT COUNT(*) FROM ohlcv_weekly) as total_weekly_records,
                        (SELECT MIN(time) FROM ohlcv_weekly) as earliest_weekly,
                        (SELECT MAX(time) FROM ohlcv_weekly) as latest_weekly
                """)

                result = db.execute(stats_query).first()

                return {
                    'securities_with_weekly_data': result.securities_with_weekly or 0,
                    'securities_with_daily_data': result.securities_with_daily or 0,
                    'total_weekly_records': result.total_weekly_records or 0,
                    'weekly_data_coverage_percent': round((result.securities_with_weekly / result.securities_with_daily) * 100, 2) if result.securities_with_daily > 0 else 0,
                    'earliest_weekly_date': result.earliest_weekly.date() if result.earliest_weekly else None,
                    'latest_weekly_date': result.latest_weekly.date() if result.latest_weekly else None
                }

        except Exception as e:
            logger.error(f"Error getting weekly data statistics: {e}")
            return {'error': str(e)}

    def cleanup_old_weekly_data(self, days_to_keep: int = 365 * 5) -> Dict[str, Any]:
        """
        Clean up old weekly data beyond retention period using efficient deletion
        """
        try:
            cutoff_date = date.today() - timedelta(days=days_to_keep)

            with get_db() as db:
                # Use efficient DELETE with date range
                delete_query = text("""
                    DELETE FROM ohlcv_weekly 
                    WHERE time < :cutoff_date
                """)

                result = db.execute(delete_query, {'cutoff_date': datetime.combine(cutoff_date, datetime.min.time())})
                deleted_count = result.rowcount
                db.commit()

                if deleted_count == 0:
                    return {'status': 'SUCCESS', 'message': 'No old weekly data to cleanup', 'deleted_records': 0}

                logger.info(f"Cleaned up {deleted_count} old weekly records (older than {cutoff_date})")

                return {'status': 'SUCCESS', 'deleted_records': deleted_count, 'cutoff_date': cutoff_date.isoformat()}

        except Exception as e:
            logger.error(f"Error cleaning up old weekly data: {e}")
            return {'status': 'FAILED', 'error': str(e)}

    def create_continuous_aggregate(self) -> Dict[str, Any]:
        """
        Create TimescaleDB continuous aggregate for weekly data (if TimescaleDB is available)
        """
        try:
            with get_db() as db:
                # Check if TimescaleDB is available
                if not self._check_timescaledb_available(db):
                    return {'status': 'SKIPPED', 'message': 'TimescaleDB not available'}

                # Create continuous aggregate for automatic weekly rollups
                create_aggregate_query = text("""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS weekly_ohlcv_continuous
                    WITH (timescaledb.continuous) AS
                    SELECT 
                        time_bucket('1 week', time, 'Monday') AS week_start,
                        security_id,
                        (array_agg(open ORDER BY time))[1] as open,
                        MAX(high) as high,
                        MIN(low) as low,
                        (array_agg(close ORDER BY time DESC))[1] as close,
                        SUM(volume) as volume
                    FROM ohlcv_daily
                    GROUP BY week_start, security_id;
                """)

                db.execute(create_aggregate_query)
                db.commit()

                # Add refresh policy
                refresh_policy_query = text("""
                    SELECT add_continuous_aggregate_policy('weekly_ohlcv_continuous',
                        start_offset => INTERVAL '1 month',
                        end_offset => INTERVAL '1 day',
                        schedule_interval => INTERVAL '1 day');
                """)

                try:
                    db.execute(refresh_policy_query)
                    db.commit()
                except:
                    # Policy might already exist
                    pass

                return {'status': 'SUCCESS', 'message': 'Continuous aggregate created successfully'}

        except Exception as e:
            logger.error(f"Error creating continuous aggregate: {e}")
            return {'status': 'FAILED', 'error': str(e)}
