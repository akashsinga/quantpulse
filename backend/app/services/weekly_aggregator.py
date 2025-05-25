# app/services/weekly_aggregator.py

from typing import List, Dict, Any, Optional, Callable
from datetime import datetime, date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, text, func
from sqlalchemy.dialects.postgresql import insert
import uuid

from app.db.session import get_db
from app.db.models.security import Security
from app.db.models.ohlcv_daily import OHLCVDaily
from app.db.models.ohlcv_weekly import OHLCVWeekly
from app.utils.logger import get_logger

logger = get_logger(__name__)


class WeeklyDataAggregator:
    """
    Service to generate weekly OHLCV data from daily data
    Uses TimescaleDB's continuous aggregates for efficiency
    """

    def __init__(self):
        logger.info("Initialized Weekly Data Aggregator")

    def generate_weekly_data(self, security_ids: Optional[List[uuid.UUID]] = None, weeks_back: int = 4, progress_callback: Optional[Callable[[int], None]] = None) -> Dict[str, Any]:
        """
        Generate weekly OHLCV data from daily data
        
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
                if security_ids:
                    securities = db.query(Security).filter(and_(Security.id.in_(security_ids), Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']))).all()
                else:
                    securities = db.query(Security).filter(and_(Security.is_active == True, Security.security_type.in_(['STOCK', 'INDEX']))).all()

                if not securities:
                    return {'status': 'SUCCESS', 'message': 'No securities to process', 'processed': 0}

                logger.info(f"Generating weekly data for {len(securities)} securities")

                # Calculate date range
                end_date = date.today()
                start_date = end_date - timedelta(weeks=weeks_back)

                # Get weekly periods to process
                weekly_periods = self._get_weekly_periods(start_date, end_date)

                logger.info(f"Processing {len(weekly_periods)} weekly periods from {start_date} to {end_date}")

                processed_count = 0
                total_records = 0

                # Process each security
                for i, security in enumerate(securities):
                    try:
                        if progress_callback:
                            progress = int((i / len(securities)) * 100)
                            progress_callback(progress)

                        # Generate weekly data for this security
                        records = self._generate_weekly_for_security(db, security.id, weekly_periods)

                        if records:
                            # Bulk insert/update weekly records
                            inserted = self._bulk_upsert_weekly_data(db, records)
                            total_records += inserted
                            logger.debug(f"Generated {inserted} weekly records for {security.symbol}")

                        processed_count += 1

                    except Exception as e:
                        logger.error(f"Error generating weekly data for {security.symbol}: {e}")
                        continue

                if progress_callback:
                    progress_callback(100)

                duration = (datetime.now() - start_time).total_seconds()

                result = {'status': 'SUCCESS', 'processed_securities': processed_count, 'total_weekly_records': total_records, 'weeks_processed': len(weekly_periods), 'duration_seconds': round(duration, 2), 'date_range': f"{start_date} to {end_date}"}

                logger.info(f"Weekly data generation completed: {result}")
                return result

        except Exception as e:
            logger.error(f"Error in weekly data generation: {e}")
            raise

    def _get_weekly_periods(self, start_date: date, end_date: date) -> List[Dict[str, date]]:
        """
        Get list of weekly periods to process
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            List of weekly period dicts with 'start' and 'end' dates
        """
        periods = []
        current = start_date

        # Align to Monday (start of week)
        days_since_monday = current.weekday()
        current = current - timedelta(days=days_since_monday)

        while current <= end_date:
            week_end = current + timedelta(days=6)  # Sunday
            periods.append({
                'start': current,
                'end': min(week_end, end_date),
                'week_start': current  # For the weekly record timestamp
            })
            current += timedelta(days=7)

        return periods

    def _generate_weekly_for_security(self, db: Session, security_id: uuid.UUID, weekly_periods: List[Dict[str, date]]) -> List[Dict[str, Any]]:
        """
        Generate weekly records for a specific security
        
        Args:
            db: Database session
            security_id: Security UUID
            weekly_periods: List of weekly periods
            
        Returns:
            List of weekly OHLCV records
        """
        weekly_records = []

        for period in weekly_periods:
            try:
                # Query daily data for this week
                daily_data = db.query(OHLCVDaily).filter(and_(OHLCVDaily.security_id == security_id, OHLCVDaily.time >= datetime.combine(period['start'], datetime.min.time()), OHLCVDaily.time <= datetime.combine(period['end'], datetime.max.time()))).order_by(OHLCVDaily.time).all()

                if not daily_data:
                    continue  # No data for this week

                # Aggregate OHLCV data
                open_price = daily_data[0].open  # First day's open
                close_price = daily_data[-1].close  # Last day's close
                high_price = max(record.high for record in daily_data)
                low_price = min(record.low for record in daily_data)
                total_volume = sum(record.volume for record in daily_data)

                # Create weekly record
                weekly_record = {
                    'time': datetime.combine(period['week_start'], datetime.min.time()),
                    'security_id': security_id,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': total_volume,
                    'adjusted_close': None  # Not calculated for weekly
                }

                weekly_records.append(weekly_record)

            except Exception as e:
                logger.warning(f"Error aggregating week {period['start']} for security {security_id}: {e}")
                continue

        return weekly_records

    def _bulk_upsert_weekly_data(self, db: Session, records: List[Dict[str, Any]]) -> int:
        """
        Bulk insert/update weekly OHLCV records
        
        Args:
            db: Database session
            records: List of weekly OHLCV records
            
        Returns:
            Number of records processed
        """
        if not records:
            return 0

        try:
            # Use PostgreSQL's ON CONFLICT for upsert
            stmt = insert(OHLCVWeekly).values(records)

            # Handle conflicts on primary key (time, security_id)
            stmt = stmt.on_conflict_do_update(index_elements=['time', 'security_id'], set_={'open': stmt.excluded.open, 'high': stmt.excluded.high, 'low': stmt.excluded.low, 'close': stmt.excluded.close, 'volume': stmt.excluded.volume, 'adjusted_close': stmt.excluded.adjusted_close})

            db.execute(stmt)
            db.commit()

            return len(records)

        except Exception as e:
            db.rollback()
            logger.error(f"Error in weekly data bulk upsert: {e}")
            raise

    def generate_weekly_for_date_range(self, security_id: uuid.UUID, start_date: date, end_date: date) -> Dict[str, Any]:
        """
        Generate weekly data for a specific security and date range
        
        Args:
            security_id: Security UUID
            start_date: Start date
            end_date: End date
            
        Returns:
            Dict with generation results
        """
        try:
            with get_db() as db:
                weekly_periods = self._get_weekly_periods(start_date, end_date)
                records = self._generate_weekly_for_security(db, security_id, weekly_periods)

                if records:
                    inserted = self._bulk_upsert_weekly_data(db, records)
                    return {'status': 'SUCCESS', 'records_generated': inserted, 'date_range': f"{start_date} to {end_date}"}
                else:
                    return {'status': 'SUCCESS', 'records_generated': 0, 'message': 'No daily data available for weekly aggregation'}

        except Exception as e:
            logger.error(f"Error generating weekly data for security {security_id}: {e}")
            return {'status': 'FAILED', 'error': str(e)}

    def get_weekly_data_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about weekly data coverage
        
        Returns:
            Dict with weekly data statistics
        """
        try:
            with get_db() as db:
                # Count securities with weekly data
                securities_with_weekly = db.query(func.count(func.distinct(OHLCVWeekly.security_id))).scalar()

                # Count total weekly records
                total_weekly_records = db.query(func.count(OHLCVWeekly.time)).scalar()

                # Get date range of weekly data
                date_range = db.query(func.min(OHLCVWeekly.time), func.max(OHLCVWeekly.time)).first()

                # Count securities with daily data (for comparison)
                securities_with_daily = db.query(func.count(func.distinct(OHLCVDaily.security_id))).scalar()

                return {'securities_with_weekly_data': securities_with_weekly or 0, 'securities_with_daily_data': securities_with_daily or 0, 'total_weekly_records': total_weekly_records or 0, 'weekly_data_coverage_percent': round((securities_with_weekly / securities_with_daily) * 100, 2) if securities_with_daily > 0 else 0, 'earliest_weekly_date': date_range[0].date() if date_range[0] else None, 'latest_weekly_date': date_range[1].date() if date_range[1] else None}

        except Exception as e:
            logger.error(f"Error getting weekly data statistics: {e}")
            return {'error': str(e)}

    def cleanup_old_weekly_data(self, days_to_keep: int = 365 * 5) -> Dict[str, Any]:
        """
        Clean up old weekly data beyond retention period
        
        Args:
            days_to_keep: Number of days to retain (default: 5 years)
            
        Returns:
            Dict with cleanup results
        """
        try:
            cutoff_date = date.today() - timedelta(days=days_to_keep)

            with get_db() as db:
                # Count records to be deleted
                count_query = db.query(func.count(OHLCVWeekly.time)).filter(OHLCVWeekly.time < datetime.combine(cutoff_date, datetime.min.time()))
                records_to_delete = count_query.scalar()

                if records_to_delete == 0:
                    return {'status': 'SUCCESS', 'message': 'No old weekly data to cleanup', 'deleted_records': 0}

                # Delete old records
                delete_query = db.query(OHLCVWeekly).filter(OHLCVWeekly.time < datetime.combine(cutoff_date, datetime.min.time()))
                delete_query.delete()
                db.commit()

                logger.info(f"Cleaned up {records_to_delete} old weekly records (older than {cutoff_date})")

                return {'status': 'SUCCESS', 'deleted_records': records_to_delete, 'cutoff_date': cutoff_date.isoformat()}

        except Exception as e:
            logger.error(f"Error cleaning up old weekly data: {e}")
            return {'status': 'FAILED', 'error': str(e)}
