# app/services/repositories/ohlcv_repository.py

from datetime import datetime, date, timedelta
from typing import List, Dict, Tuple, Optional, Any, Union
from sqlalchemy import text, func, and_, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import Session
from contextlib import contextmanager
import time
import uuid

from app.db.session import get_db, SessionLocal
from app.db.models.ohlcv_daily import OHLCVDaily
from app.db.models.security import Security
from app.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


@contextmanager
def get_repository_session():
    """Context manager for repository database sessions."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Repository session error: {str(e)}")
        raise
    finally:
        session.close()


class OHLCVRepository:
    """Enhanced repository for OHLCV data operations with optimized bulk processing."""

    def __init__(self):
        """Initialize the enhanced OHLCV repository."""
        self.bulk_insert_size = settings.OHLCV_BULK_INSERT_SIZE
        self.performance_stats = {"total_operations": 0, "successful_operations": 0, "failed_operations": 0, "total_records_processed": 0, "total_records_inserted": 0, "total_records_updated": 0, "avg_operation_time_ms": 0.0}
        logger.info(f"Initialized enhanced OHLCV repository with bulk size: {self.bulk_insert_size}")

    def bulk_upsert_optimized(self, records_by_security: Dict[str, List[Dict[str, Any]]], source: str = "dhan_api") -> Dict[str, Tuple[int, int, int]]:
        """
        Perform optimized bulk upsert operations for multiple securities.

        Args:
            records_by_security: Dict mapping security IDs to lists of OHLCV records
            source: Data source identifier

        Returns:
            Dict mapping security IDs to (inserted, updated, total) counts
        """
        start_time = time.time()
        operation_id = str(uuid.uuid4())[:8]

        logger.info(f"Starting bulk upsert operation {operation_id} for {len(records_by_security)} securities")

        results = {}
        total_records = sum(len(records) for records in records_by_security.values())

        try:
            with get_repository_session() as session:
                # Prepare all records for bulk operation
                all_records = []
                security_record_counts = {}

                for security_id, records in records_by_security.items():
                    if not records:
                        results[security_id] = (0, 0, 0)
                        continue

                    # Deduplicate records by time for this security
                    unique_records = self._deduplicate_records(records)
                    security_record_counts[security_id] = len(unique_records)

                    # Convert to database format
                    for record in unique_records:
                        try:
                            db_record = {"time": self._ensure_datetime(record["time"]), "security_id": uuid.UUID(security_id), "open": float(record["open"]), "high": float(record["high"]), "low": float(record["low"]), "close": float(record["close"]), "volume": int(record["volume"]), "source": source}

                            # Add adjusted_close if available
                            if "adjusted_close" in record and record["adjusted_close"] is not None:
                                db_record["adjusted_close"] = float(record["adjusted_close"])

                            all_records.append(db_record)

                        except (ValueError, TypeError, KeyError) as e:
                            logger.warning(f"Skipping invalid record for security {security_id}: {e}")
                            continue

                if not all_records:
                    logger.warning(f"No valid records to process in operation {operation_id}")
                    return results

                # Process in batches to avoid memory issues
                batch_size = min(self.bulk_insert_size, len(all_records))
                processed_records = 0

                for i in range(0, len(all_records), batch_size):
                    batch = all_records[i : i + batch_size]

                    try:
                        # Perform bulk upsert using PostgreSQL's ON CONFLICT
                        insert_stmt = insert(OHLCVDaily).values(batch)
                        upsert_stmt = insert_stmt.on_conflict_do_update(constraint="ohlcv_daily_pkey", set_={"open": insert_stmt.excluded.open, "high": insert_stmt.excluded.high, "low": insert_stmt.excluded.low, "close": insert_stmt.excluded.close, "volume": insert_stmt.excluded.volume, "adjusted_close": insert_stmt.excluded.adjusted_close, "source": insert_stmt.excluded.source})  # Primary key constraint

                        result = session.execute(upsert_stmt)
                        processed_records += len(batch)

                        # Log progress for large operations
                        if len(all_records) > 1000 and processed_records % 1000 == 0:
                            progress = (processed_records / len(all_records)) * 100
                            logger.info(f"Operation {operation_id}: {progress:.1f}% complete ({processed_records}/{len(all_records)})")

                    except IntegrityError as e:
                        logger.error(f"Integrity error in batch {i//batch_size + 1}: {str(e)}")
                        session.rollback()
                        # Try individual inserts for this batch
                        self._fallback_individual_inserts(session, batch, operation_id)
                    except Exception as e:
                        logger.error(f"Error in batch {i//batch_size + 1}: {str(e)}")
                        session.rollback()
                        raise

                # Commit the transaction
                session.commit()

                # Calculate results (approximate since we can't easily distinguish inserts vs updates in bulk)
                for security_id, record_count in security_record_counts.items():
                    # For bulk operations, we approximate all as "inserted" since we can't easily track updates
                    results[security_id] = (record_count, 0, record_count)

                # Update performance stats
                operation_time = (time.time() - start_time) * 1000  # Convert to ms
                self._update_performance_stats(True, total_records, operation_time)

                logger.info(f"Completed bulk upsert operation {operation_id}: {processed_records} records in {operation_time:.2f}ms")

        except Exception as e:
            logger.error(f"Bulk upsert operation {operation_id} failed: {str(e)}")
            self._update_performance_stats(False, total_records, (time.time() - start_time) * 1000)

            # Return error results
            for security_id in records_by_security.keys():
                results[security_id] = (0, 0, 0)

        return results

    def _deduplicate_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Deduplicate records by time, keeping the latest record for each timestamp."""
        if not records:
            return []

        unique_records = {}
        for record in records:
            time_key = record["time"]
            if isinstance(time_key, datetime):
                time_key = time_key.isoformat()
            elif isinstance(time_key, str):
                time_key = time_key
            else:
                time_key = str(time_key)

            # Keep the last record for each timestamp (assuming records are chronologically ordered)
            unique_records[time_key] = record

        return list(unique_records.values())

    def _ensure_datetime(self, time_value: Any) -> datetime:
        """Ensure time value is a datetime object."""
        if isinstance(time_value, datetime):
            return time_value
        elif isinstance(time_value, str):
            # Try to parse ISO format string
            try:
                return datetime.fromisoformat(time_value.replace("Z", "+00:00"))
            except ValueError:
                # Try other common formats
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
                    try:
                        return datetime.strptime(time_value, fmt)
                    except ValueError:
                        continue
                raise ValueError(f"Unable to parse datetime: {time_value}")
        elif isinstance(time_value, (int, float)):
            # Assume Unix timestamp
            return datetime.fromtimestamp(time_value, tz=settings.INDIA_TZ)
        else:
            raise ValueError(f"Invalid time value type: {type(time_value)}")

    def _fallback_individual_inserts(self, session: Session, batch: List[Dict[str, Any]], operation_id: str):
        """Fallback to individual inserts when bulk insert fails."""
        logger.warning(f"Operation {operation_id}: Falling back to individual inserts for {len(batch)} records")

        success_count = 0
        for record in batch:
            try:
                insert_stmt = insert(OHLCVDaily).values(**record)
                upsert_stmt = insert_stmt.on_conflict_do_update(constraint="ohlcv_daily_pkey", set_={"open": insert_stmt.excluded.open, "high": insert_stmt.excluded.high, "low": insert_stmt.excluded.low, "close": insert_stmt.excluded.close, "volume": insert_stmt.excluded.volume, "adjusted_close": insert_stmt.excluded.adjusted_close, "source": insert_stmt.excluded.source})

                session.execute(upsert_stmt)
                success_count += 1

            except Exception as e:
                logger.debug(f"Individual insert failed for record: {e}")
                continue

        logger.info(f"Operation {operation_id}: Individual fallback completed {success_count}/{len(batch)} records")

    def upsert_daily_data(self, security_id: str, records: List[Dict[str, Any]], source: str = "dhan_api") -> Tuple[int, int, int]:
        """
        Insert or update daily OHLCV data for a single security.

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

        # Use the bulk upsert method for single security
        result = self.bulk_upsert_optimized({security_id: records}, source)
        return result.get(security_id, (0, 0, 0))

    def find_data_gaps(self, security_id: str, start_date: Optional[date] = None, end_date: Optional[date] = None, min_gap_days: int = 1) -> List[Dict[str, date]]:
        """
        Find gaps in OHLCV data for a security using optimized TimescaleDB queries.

        Args:
            security_id: UUID of the security
            start_date: Start date for gap detection
            end_date: End date for gap detection
            min_gap_days: Minimum gap size to report (in days)

        Returns:
            List of gaps as [{"start": date1, "end": date2}, ...]
        """
        logger.info(f"Finding data gaps for security {security_id}")

        # Default date range
        if not start_date:
            try:
                start_date = datetime.strptime(settings.FROM_DATE, "%Y-%m-%d").date()
            except ValueError:
                start_date = date(2000, 1, 1)

        if not end_date:
            end_date = date.today()

        try:
            with get_repository_session() as session:
                # Optimized TimescaleDB query to find gaps
                query = text(
                    """
                    WITH date_series AS (
                        SELECT time::date as record_date
                        FROM ohlcv_daily
                        WHERE security_id = :security_id
                        AND time >= :start_date
                        AND time <= :end_date
                        GROUP BY record_date
                        ORDER BY record_date
                    ),
                    gap_analysis AS (
                        SELECT
                            record_date,
                            LEAD(record_date) OVER (ORDER BY record_date) as next_date,
                            LEAD(record_date) OVER (ORDER BY record_date) - record_date as gap_days
                        FROM date_series
                    )
                    SELECT 
                        record_date + INTERVAL '1 day' as gap_start,
                        next_date - INTERVAL '1 day' as gap_end,
                        gap_days
                    FROM gap_analysis
                    WHERE gap_days > :min_gap_days
                    ORDER BY record_date
                """
                )

                result = session.execute(query, {"security_id": security_id, "start_date": start_date, "end_date": end_date, "min_gap_days": min_gap_days + 1})

                gaps = []
                for row in result:
                    gap = {"start": row.gap_start.date(), "end": row.gap_end.date(), "days": row.gap_days}
                    gaps.append(gap)

                logger.info(f"Found {len(gaps)} data gaps for security {security_id}")
                return gaps

        except Exception as e:
            logger.error(f"Error finding data gaps for security {security_id}: {str(e)}")
            return []

    def get_latest_data_point(self, security_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the most recent OHLCV data point for a security.

        Args:
            security_id: UUID of the security

        Returns:
            Dict with the latest OHLCV data or None if not found
        """
        logger.debug(f"Getting latest data point for security {security_id}")

        try:
            with get_repository_session() as session:
                # Optimized query using TimescaleDB time-weighted functions
                latest = session.query(OHLCVDaily).filter(OHLCVDaily.security_id == security_id).order_by(OHLCVDaily.time.desc()).first()

                if not latest:
                    return None

                return {"time": latest.time, "open": float(latest.open), "high": float(latest.high), "low": float(latest.low), "close": float(latest.close), "volume": int(latest.volume), "adjusted_close": float(latest.adjusted_close) if latest.adjusted_close else None, "source": latest.source}

        except Exception as e:
            logger.error(f"Error getting latest data point for security {security_id}: {str(e)}")
            return None

    def get_data_summary(self, security_id: str, start_date: Optional[date] = None, end_date: Optional[date] = None) -> Dict[str, Any]:
        """
        Get data summary statistics for a security.

        Args:
            security_id: UUID of the security
            start_date: Start date for summary
            end_date: End date for summary

        Returns:
            Dict with summary statistics
        """
        try:
            with get_repository_session() as session:
                query = session.query(func.count(OHLCVDaily.time).label("record_count"), func.min(OHLCVDaily.time).label("first_date"), func.max(OHLCVDaily.time).label("last_date"), func.avg(OHLCVDaily.volume).label("avg_volume"), func.min(OHLCVDaily.low).label("min_price"), func.max(OHLCVDaily.high).label("max_price")).filter(OHLCVDaily.security_id == security_id)

                if start_date:
                    query = query.filter(OHLCVDaily.time >= start_date)
                if end_date:
                    query = query.filter(OHLCVDaily.time <= end_date)

                result = query.first()

                if not result or result.record_count == 0:
                    return {"record_count": 0, "status": "no_data"}

                return {"record_count": result.record_count, "first_date": result.first_date.date() if result.first_date else None, "last_date": result.last_date.date() if result.last_date else None, "avg_volume": float(result.avg_volume) if result.avg_volume else 0, "min_price": float(result.min_price) if result.min_price else 0, "max_price": float(result.max_price) if result.max_price else 0, "status": "ok"}

        except Exception as e:
            logger.error(f"Error getting data summary for security {security_id}: {str(e)}")
            return {"status": "error", "error": str(e)}

    def validate_data_continuity(self, security_id: str, start_date: date, end_date: date) -> Dict[str, Any]:
        """
        Validate data continuity for a security within a date range.

        Args:
            security_id: UUID of the security
            start_date: Start date for validation
            end_date: End date for validation

        Returns:
            Dict with validation results
        """
        logger.info(f"Validating data continuity for security {security_id}")

        try:
            # Get data summary
            summary = self.get_data_summary(security_id, start_date, end_date)

            if summary.get("status") != "ok":
                return summary

            # Find gaps
            gaps = self.find_data_gaps(security_id, start_date, end_date)

            # Calculate expected business days (simplified)
            business_days = self._count_business_days(start_date, end_date)

            # Calculate coverage percentage
            coverage = (summary["record_count"] / business_days) * 100 if business_days > 0 else 0

            return {"security_id": security_id, "start_date": start_date, "end_date": end_date, "data_points": summary["record_count"], "expected_days": business_days, "coverage_pct": round(coverage, 2), "gaps": gaps, "gap_count": len(gaps), "has_gaps": len(gaps) > 0, "first_date": summary["first_date"], "last_date": summary["last_date"], "status": "complete" if len(gaps) == 0 else "gaps_found"}

        except Exception as e:
            logger.error(f"Error validating data continuity for security {security_id}: {str(e)}")
            return {"security_id": security_id, "error": str(e), "status": "error"}

    def _count_business_days(self, start_date: date, end_date: date) -> int:
        """
        Count business days between two dates (excluding weekends).

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

    def _update_performance_stats(self, success: bool, record_count: int, operation_time_ms: float):
        """Update repository performance statistics."""
        self.performance_stats["total_operations"] += 1
        self.performance_stats["total_records_processed"] += record_count

        if success:
            self.performance_stats["successful_operations"] += 1
            self.performance_stats["total_records_inserted"] += record_count  # Approximate
        else:
            self.performance_stats["failed_operations"] += 1

        # Update average operation time
        total_ops = self.performance_stats["total_operations"]
        current_avg = self.performance_stats["avg_operation_time_ms"]
        self.performance_stats["avg_operation_time_ms"] = (current_avg * (total_ops - 1) + operation_time_ms) / total_ops

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get repository performance statistics."""
        stats = self.performance_stats.copy()

        # Calculate additional metrics
        if stats["total_operations"] > 0:
            stats["success_rate_pct"] = (stats["successful_operations"] / stats["total_operations"]) * 100
            stats["avg_records_per_operation"] = stats["total_records_processed"] / stats["total_operations"]
        else:
            stats["success_rate_pct"] = 0
            stats["avg_records_per_operation"] = 0

        # Round floating point values
        for key, value in stats.items():
            if isinstance(value, float):
                stats[key] = round(value, 2)

        return stats

    def reset_performance_stats(self):
        """Reset repository performance statistics."""
        self.performance_stats = {"total_operations": 0, "successful_operations": 0, "failed_operations": 0, "total_records_processed": 0, "total_records_inserted": 0, "total_records_updated": 0, "avg_operation_time_ms": 0.0}
        logger.info("Repository performance statistics reset")

    def cleanup_old_data(self, retention_days: int = 2555) -> Dict[str, Any]:
        """
        Clean up old data beyond retention period.

        Args:
            retention_days: Number of days to retain (default ~7 years)

        Returns:
            Dict with cleanup results
        """
        cutoff_date = datetime.now() - timedelta(days=retention_days)

        try:
            with get_repository_session() as session:
                # Count records to be deleted
                count_query = session.query(func.count(OHLCVDaily.time)).filter(OHLCVDaily.time < cutoff_date)
                records_to_delete = count_query.scalar()

                if records_to_delete == 0:
                    return {"status": "no_cleanup_needed", "records_deleted": 0}

                # Delete old records
                delete_query = session.query(OHLCVDaily).filter(OHLCVDaily.time < cutoff_date)
                deleted_count = delete_query.delete(synchronize_session=False)

                session.commit()

                logger.info(f"Cleaned up {deleted_count} old records (older than {cutoff_date.date()})")

                return {"status": "success", "records_deleted": deleted_count, "cutoff_date": cutoff_date.date(), "retention_days": retention_days}

        except Exception as e:
            logger.error(f"Error during data cleanup: {str(e)}")
            return {"status": "error", "error": str(e)}
