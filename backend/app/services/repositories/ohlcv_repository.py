# app/services/repositories/ohlcv_repository.py

from datetime import datetime, date, timedelta
from typing import List, Dict, Tuple, Optional, Any, Union
from sqlalchemy import text, func, and_, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from app.db.session import get_db, SessionLocal
from app.db.models.ohlcv_daily import OHLCVDaily
from app.db.models.security import Security
from app.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class OHLCVRepository:
    """Repository for OHLCV data operations with TimescaleDB optimization."""

    def __init__(self):
        """Initialize the OHLCV repository."""
        logger.info("Initializing OHLCV repository")

    def upsert_daily_data(self, security_id: str, records: List[Dict[str, Any]], source: str = "dhan_api") -> Tuple[int, int, int]:
        """Insert or update daily OHLCV data.

        Args:
            security_id: UUID of the security
            records: List of OHLCV data records
            source: Data source identifier

        Returns:
            Tuple of (inserted, updated, total) counts
        """
        if not records:
            logger.warning(f"No records provided for security {security_id}")
            return (0, 0, 0)

        logger.info(f"Upserting {len(records)} records for security {security_id}")

        inserted = 0
        updated = 0

        with get_db() as db:
            try:
                # Process in smaller batches to avoid memory issues
                batch_size = 500
                for i in range(0, len(records), batch_size):
                    batch = records[i : i + batch_size]

                    # Convert batch to database format
                    db_records = []
                    for record in batch:
                        db_record = {"time": record["time"], "security_id": security_id, "open": record["open"], "high": record["high"], "low": record["low"], "close": record["close"], "volume": record["volume"], "source": source}

                        # Add adjusted_close if available
                        if "adjusted_close" in record:
                            db_record["adjusted_close"] = record["adjusted_close"]

                        db_records.append(db_record)

                    # Perform the upsert operation
                    insert_stmt = insert(OHLCVDaily).values(db_records)

                    # Specify the conflict resolution strategy
                    upsert_stmt = insert_stmt.on_conflict_do_update(constraint="ohlcv_daily_pkey", set_={"open": insert_stmt.excluded.open, "high": insert_stmt.excluded.high, "low": insert_stmt.excluded.low, "close": insert_stmt.excluded.close, "volume": insert_stmt.excluded.volume, "source": insert_stmt.excluded.source})  # Primary key constraint

                    # Execute and get result metadata
                    result = db.execute(upsert_stmt)
                    db.commit()

                    # TimescaleDB doesn't directly support reporting inserted/updated counts
                    # So we need to query to determine the count

                    # This is a simplification - in a real implementation,
                    # you would track which records were newly inserted vs updated
                    batch_inserted = len(batch)
                    inserted += batch_inserted

                logger.info(f"Successfully upserted {len(records)} records for security {security_id}")
                return (inserted, updated, len(records))

            except Exception as e:
                db.rollback()
                logger.error(f"Error upserting data for security {security_id}: {str(e)}")
                raise

    def find_data_gaps(self, security_id: str, start_date: Optional[date] = None, end_date: Optional[date] = None, min_gap_days: int = 1) -> List[Dict[str, date]]:
        """Find gaps in OHLCV data for a security.

        Args:
            security_id: UUID of the security
            start_date: Start date for gap detection
            end_date: End date for gap detection
            min_gap_days: Minimum gap size to report (in days)

        Returns:
            List of gaps as [{"start": date1, "end": date2}, ...]
        """
        logger.info(f"Finding data gaps for security {security_id}")

        # Default date range from settings
        if not start_date:
            try:
                start_date = datetime.strptime(settings.FROM_DATE, "%Y-%m-%d").date()
            except ValueError:
                start_date = date(2000, 1, 1)  # Default fallback

        if not end_date:
            end_date = date.today()

        with get_db() as db:
            try:
                # TimescaleDB-optimized query to find gaps
                query = text(
                    """
                    WITH dates AS (
                        SELECT time::date as record_date
                        FROM ohlcv_daily
                        WHERE security_id = :security_id
                          AND time >= :start_date
                          AND time <= :end_date
                        GROUP BY record_date
                        ORDER BY record_date
                    ),
                    date_gaps AS (
                        SELECT
                            dates.record_date as start_date,
                            LEAD(dates.record_date) OVER (ORDER BY dates.record_date) as next_date,
                            LEAD(dates.record_date) OVER (ORDER BY dates.record_date) - dates.record_date as gap_days
                        FROM dates
                    )
                    SELECT start_date, next_date - INTERVAL '1 day' as end_date
                    FROM date_gaps
                    WHERE gap_days > :min_gap_days
                """
                )

                result = db.execute(query, {"security_id": security_id, "start_date": start_date, "end_date": end_date, "min_gap_days": min_gap_days + 1})  # +1 because we're looking for gaps larger than min_gap_days

                gaps = []
                for row in result:
                    gap = {"start": row[0] + timedelta(days=1), "end": row[1]}  # Start of gap is day after last record  # End of gap is day before next record
                    gaps.append(gap)

                logger.info(f"Found {len(gaps)} data gaps for security {security_id}")
                return gaps

            except Exception as e:
                logger.error(f"Error finding data gaps for security {security_id}: {str(e)}")
                return []

    def get_latest_data_point(self, security_id: str) -> Optional[Dict[str, Any]]:
        """Get the most recent OHLCV data point for a security.

        Args:
            security_id: UUID of the security

        Returns:
            Dict with the latest OHLCV data or None if not found
        """
        logger.debug(f"Getting latest data point for security {security_id}")

        with get_db() as db:
            try:
                # TimescaleDB optimized query to get latest point
                latest = db.query(OHLCVDaily).filter(OHLCVDaily.security_id == security_id).order_by(OHLCVDaily.time.desc()).first()

                if not latest:
                    return None

                return {"time": latest.time, "open": latest.open, "high": latest.high, "low": latest.low, "close": latest.close, "volume": latest.volume, "adjusted_close": latest.adjusted_close, "source": latest.source}

            except Exception as e:
                logger.error(f"Error getting latest data point for security {security_id}: {str(e)}")
                return None

    def bulk_upsert(self, records_by_security: Dict[str, List[Dict[str, Any]]], source: str = "dhan_api") -> Dict[str, Tuple[int, int, int]]:
        """Perform bulk upsert operations for multiple securities.

        Args:
            records_by_security: Dict mapping security IDs to lists of OHLCV records
            source: Data source identifier

        Returns:
            Dict mapping security IDs to (inserted, updated, total) counts
        """
        logger.info(f"Bulk upserting data for {len(records_by_security)} securities")

        results = {}

        for security_id, records in records_by_security.items():
            try:
                result = self.upsert_daily_data(security_id, records, source)
                results[security_id] = result
            except Exception as e:
                logger.error(f"Error in bulk upsert for security {security_id}: {str(e)}")
                results[security_id] = (0, 0, 0)

        return results

    def validate_data_continuity(self, security_id: str, start_date: date, end_date: date) -> Dict[str, Any]:
        """Validate data continuity for a security within a date range.

        Args:
            security_id: UUID of the security
            start_date: Start date for validation
            end_date: End date for validation

        Returns:
            Dict with validation results
        """
        logger.info(f"Validating data continuity for security {security_id}")

        # Find gaps
        gaps = self.find_data_gaps(security_id, start_date, end_date)

        # Get count of available data points
        with get_db() as db:
            try:
                count = db.query(func.count(OHLCVDaily.time)).filter(OHLCVDaily.security_id == security_id, OHLCVDaily.time >= start_date, OHLCVDaily.time <= end_date).scalar()

                # Calculate expected business days (simplified - in real impl would use trading calendar)
                business_days = self._count_business_days(start_date, end_date)

                # Calculate coverage percentage
                coverage = (count / business_days) * 100 if business_days > 0 else 0

                return {"security_id": security_id, "start_date": start_date, "end_date": end_date, "data_points": count, "expected_days": business_days, "coverage_pct": coverage, "gaps": gaps, "has_gaps": len(gaps) > 0}

            except Exception as e:
                logger.error(f"Error validating data continuity for security {security_id}: {str(e)}")
                return {"security_id": security_id, "error": str(e), "has_gaps": True}

    def _count_business_days(self, start_date: date, end_date: date) -> int:
        """Count business days between two dates (excluding weekends).

        This is a simplified implementation - a real version would use
        an exchange trading calendar to account for holidays.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            Number of business days
        """
        days = 0
        current = start_date

        while current <= end_date:
            # Check if current date is a weekday (0=Monday, 6=Sunday)
            if current.weekday() < 5:  # 0-4 are Monday to Friday
                days += 1
            current += timedelta(days=1)

        return days
