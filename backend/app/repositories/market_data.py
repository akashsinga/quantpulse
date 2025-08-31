# backend/app/repositories/market_data.py
"""
Data access layer for market data operations.
Handles all database interactions for OHLCV data and technical indicators.
"""

from typing import Optional, List, Dict, Any
from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc
from datetime import date, datetime, timedelta

from app.repositories.base import BaseRepository
from app.models.market_data import OHLCVData, TechnicalIndicator, MarketDataImportLog
from app.utils.logger import get_logger
from app.utils.enum import Timeframe

logger = get_logger(__name__)


class OHLCVRepository(BaseRepository[OHLCVData]):
    """Repository for OHLCV data operations"""

    def __init__(self, db: Session):
        super().__init__(db, OHLCVData)

    def get_by_security_and_date(self, security_id: UUID, date: date, timeframe: str = Timeframe.DAILY.value) -> Optional[OHLCVData]:
        """Get OHLCV data by security, date and timeframe"""
        return self.db.query(OHLCVData).filter(OHLCVData.security_id == security_id, OHLCVData.date == date, OHLCVData.timeframe == timeframe, OHLCVData.is_deleted == False).first()

    def get_by_security_date_range(self, security_id: UUID, date_from: date, date_to: date, timeframe: str = Timeframe.DAILY.value, skip: int = 0, limit: int = 1000) -> List[OHLCVData]:
        """Get OHLCV data for a security within date range"""
        return self.db.query(OHLCVData).filter(OHLCVData.security_id == security_id, OHLCVData.date >= date_from, OHLCVData.date <= date_to, OHLCVData.timeframe == timeframe, OHLCVData.is_deleted == False).order_by(OHLCVData.date.asc()).offset(skip).limit(limit).all()

    def get_latest_data_date(self, security_id: UUID, timeframe: str = Timeframe.DAILY.value) -> Optional[date]:
        """Get the latest date for which OHLCV data exists for a security"""
        result = self.db.query(func.max(OHLCVData.date)).filter(OHLCVData.security_id == security_id, OHLCVData.timeframe == timeframe, OHLCVData.is_deleted == False).scalar()
        return result

    def get_earliest_data_date(self, security_id: UUID, timeframe: str = Timeframe.DAILY.value) -> Optional[date]:
        """Get the earliest date for which OHLCV data exists for a security"""
        result = self.db.query(func.min(OHLCVData.date)).filter(OHLCVData.security_id == security_id, OHLCVData.timeframe == timeframe, OHLCVData.is_deleted == False).scalar()
        return result

    def create_or_update_ohlcv(self, security_id: UUID, date: date, ohlcv_data: Dict[str, Any], timeframe: str = Timeframe.DAILY.value) -> OHLCVData:
        """Create or update OHLCV data for a security and date"""
        existing = self.get_by_security_and_date(security_id, date, timeframe)

        ohlcv_data_full = {'security_id': security_id, 'date': date, 'timeframe': timeframe, **ohlcv_data}

        if existing:
            # Update existing record
            return self.update(existing, ohlcv_data_full)
        else:
            # Create new record
            new_ohlcv = OHLCVData(**ohlcv_data_full)
            return self.create(new_ohlcv)

    def bulk_create_or_update_ohlcv(self, ohlcv_records: List[Dict[str, Any]]) -> Dict[str, int]:
        """Bulk create or update OHLCV records"""
        stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}

        for record in ohlcv_records:
            try:
                security_id = record['security_id']
                date_val = record['date']
                timeframe = record.get('timeframe', Timeframe.DAILY.value)

                existing = self.get_by_security_and_date(security_id, date_val, timeframe)

                if existing:
                    # Update existing
                    self.update(existing, record)
                    stats['updated'] += 1
                else:
                    # Create new
                    new_ohlcv = OHLCVData(**record)
                    self.create(new_ohlcv)
                    stats['created'] += 1

            except Exception as e:
                logger.warning(f"Error processing OHLCV record {record.get('security_id', 'unknown')}/{record.get('date', 'unknown')}: {e}")
                stats['errors'] += 1
                continue

        return stats

    def get_securities_missing_data(self, date_from: date, date_to: date, security_ids: Optional[List[UUID]] = None, timeframe: str = Timeframe.DAILY.value) -> List[UUID]:
        """Get list of security IDs that are missing OHLCV data for the date range"""
        from app.models.securities import Security

        # Build base query for active securities
        securities_query = self.db.query(Security.id).filter(Security.is_active == True, Security.is_deleted == False)

        if security_ids:
            securities_query = securities_query.filter(Security.id.in_(security_ids))

        all_security_ids = [row[0] for row in securities_query.all()]

        # Get security IDs that have data in the date range
        securities_with_data = self.db.query(OHLCVData.security_id.distinct()).filter(OHLCVData.date >= date_from, OHLCVData.date <= date_to, OHLCVData.timeframe == timeframe, OHLCVData.is_deleted == False).all()

        securities_with_data_ids = [row[0] for row in securities_with_data]

        # Return securities that don't have data
        missing_security_ids = [sid for sid in all_security_ids if sid not in securities_with_data_ids]

        return missing_security_ids

    def get_data_coverage_stats(self, security_id: UUID, timeframe: str = Timeframe.DAILY.value) -> Dict[str, Any]:
        """Get data coverage statistics for a security"""
        earliest_date = self.get_earliest_data_date(security_id, timeframe)
        latest_date = self.get_latest_data_date(security_id, timeframe)

        if not earliest_date or not latest_date:
            return {'earliest_date': None, 'latest_date': None, 'total_records': 0, 'date_range_days': 0, 'coverage_percentage': 0.0}

        total_records = self.db.query(OHLCVData).filter(OHLCVData.security_id == security_id, OHLCVData.timeframe == timeframe, OHLCVData.is_deleted == False).count()

        date_range_days = (latest_date - earliest_date).days + 1
        coverage_percentage = (total_records / date_range_days * 100) if date_range_days > 0 else 0.0

        return {'security_id': str(security_id), 'earliest_date': earliest_date.isoformat(), 'latest_date': latest_date.isoformat(), 'total_records': total_records, 'date_range_days': date_range_days, 'coverage_percentage': round(coverage_percentage, 2)}

    def delete_data_by_date_range(self, security_id: UUID, date_from: date, date_to: date, timeframe: str = Timeframe.DAILY.value, hard_delete: bool = False) -> int:
        """Delete OHLCV data within date range"""
        query = self.db.query(OHLCVData).filter(OHLCVData.security_id == security_id, OHLCVData.date >= date_from, OHLCVData.date <= date_to, OHLCVData.timeframe == timeframe, OHLCVData.is_deleted == False)

        records_to_delete = query.all()
        count = len(records_to_delete)

        for record in records_to_delete:
            self.delete(record, soft_delete=not hard_delete)

        return count

    def get_high_volume_days(self, security_id: UUID, top_n: int = 10, days_back: int = 30, timeframe: str = Timeframe.DAILY.value) -> List[OHLCVData]:
        """Get top N highest volume trading days for a security"""
        date_from = date.today() - timedelta(days=days_back)

        return self.db.query(OHLCVData).filter(OHLCVData.security_id == security_id, OHLCVData.date >= date_from, OHLCVData.timeframe == timeframe, OHLCVData.is_deleted == False).order_by(desc(OHLCVData.volume)).limit(top_n).all()

    def get_price_extremes(self, security_id: UUID, days_back: int = 30, timeframe: str = Timeframe.DAILY.value) -> Dict[str, Any]:
        """Get price extremes (highs and lows) for a security"""
        date_from = date.today() - timedelta(days=days_back)

        # Get highest high
        highest = self.db.query(OHLCVData).filter(OHLCVData.security_id == security_id, OHLCVData.date >= date_from, OHLCVData.timeframe == timeframe, OHLCVData.is_deleted == False).order_by(desc(OHLCVData.high_price)).first()

        # Get lowest low
        lowest = self.db.query(OHLCVData).filter(OHLCVData.security_id == security_id, OHLCVData.date >= date_from, OHLCVData.timeframe == timeframe, OHLCVData.is_deleted == False).order_by(asc(OHLCVData.low_price)).first()

        return {'highest': {'price': float(highest.high_price) if highest else None, 'date': highest.date.isoformat() if highest else None}, 'lowest': {'price': float(lowest.low_price) if lowest else None, 'date': lowest.date.isoformat() if lowest else None}, 'period_days': days_back}


class TechnicalIndicatorRepository(BaseRepository[TechnicalIndicator]):
    """Repository for technical indicators operations"""

    def __init__(self, db: Session):
        super().__init__(db, TechnicalIndicator)

    def get_by_ohlcv_and_name(self, ohlcv_data_id: UUID, indicator_name: str) -> Optional[TechnicalIndicator]:
        """Get technical indicator by OHLCV data ID and indicator name"""
        return self.db.query(TechnicalIndicator).filter(TechnicalIndicator.ohlcv_data_id == ohlcv_data_id, TechnicalIndicator.indicator_name == indicator_name, TechnicalIndicator.is_deleted == False).first()

    def create_or_update_indicator(self, ohlcv_data_id: UUID, indicator_name: str, indicator_value: float, calculation_params: str = None) -> TechnicalIndicator:
        """Create or update a technical indicator"""
        existing = self.get_by_ohlcv_and_name(ohlcv_data_id, indicator_name)

        indicator_data = {'ohlcv_data_id': ohlcv_data_id, 'indicator_name': indicator_name, 'indicator_value': indicator_value, 'calculation_params': calculation_params, 'calculation_timestamp': datetime.now()}

        if existing:
            return self.update(existing, indicator_data)
        else:
            new_indicator = TechnicalIndicator(**indicator_data)
            return self.create(new_indicator)

    def bulk_create_indicators(self, indicators_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Bulk create technical indicators"""
        stats = {'created': 0, 'updated': 0, 'errors': 0}

        for indicator_data in indicators_data:
            try:
                self.create_or_update_indicator(**indicator_data)
                stats['created'] += 1
            except Exception as e:
                logger.warning(f"Error creating indicator {indicator_data.get('indicator_name', 'unknown')}: {e}")
                stats['errors'] += 1

        return stats

    def get_indicators_for_security_date_range(self, security_id: UUID, date_from: date, date_to: date, indicator_names: List[str] = None, timeframe: str = Timeframe.DAILY.value) -> List[TechnicalIndicator]:
        """Get technical indicators for a security within date range"""
        query = self.db.query(TechnicalIndicator).join(OHLCVData).filter(OHLCVData.security_id == security_id, OHLCVData.date >= date_from, OHLCVData.date <= date_to, OHLCVData.timeframe == timeframe, TechnicalIndicator.is_deleted == False, OHLCVData.is_deleted == False)

        if indicator_names:
            query = query.filter(TechnicalIndicator.indicator_name.in_(indicator_names))

        return query.order_by(OHLCVData.date.asc()).all()


class MarketDataImportLogRepository(BaseRepository[MarketDataImportLog]):
    """Repository for market data import log operations"""

    def __init__(self, db: Session):
        super().__init__(db, MarketDataImportLog)

    def create_import_log(self, import_data: Dict[str, Any]) -> MarketDataImportLog:
        """Create a new import log entry"""
        log_entry = MarketDataImportLog(**import_data)
        return self.create(log_entry)

    def get_latest_import_for_security(self, security_id: UUID, import_type: str = None) -> Optional[MarketDataImportLog]:
        """Get the latest import log for a security"""
        query = self.db.query(MarketDataImportLog).filter(MarketDataImportLog.security_id == security_id, MarketDataImportLog.is_deleted == False)

        if import_type:
            query = query.filter(MarketDataImportLog.import_type == import_type)

        return query.order_by(desc(MarketDataImportLog.created_at)).first()

    def get_import_stats_by_date(self, import_date: date) -> Dict[str, Any]:
        """Get import statistics for a specific date"""
        logs = self.db.query(MarketDataImportLog).filter(MarketDataImportLog.import_date == import_date, MarketDataImportLog.is_deleted == False).all()

        if not logs:
            return {'import_date': import_date.isoformat(), 'total_imports': 0, 'successful_imports': 0, 'failed_imports': 0, 'total_records_processed': 0, 'total_records_created': 0, 'total_records_updated': 0}

        total_imports = len(logs)
        successful_imports = len([log for log in logs if log.status == 'SUCCESS'])
        failed_imports = len([log for log in logs if log.status == 'FAILURE'])

        total_records_processed = sum(log.total_records_processed for log in logs)
        total_records_created = sum(log.records_created for log in logs)
        total_records_updated = sum(log.records_updated for log in logs)

        return {'import_date': import_date.isoformat(), 'total_imports': total_imports, 'successful_imports': successful_imports, 'failed_imports': failed_imports, 'success_rate': round((successful_imports / total_imports * 100), 2) if total_imports > 0 else 0, 'total_records_processed': total_records_processed, 'total_records_created': total_records_created, 'total_records_updated': total_records_updated}

    def get_recent_imports(self, days_back: int = 7, limit: int = 100) -> List[MarketDataImportLog]:
        """Get recent import logs"""
        date_from = date.today() - timedelta(days=days_back)

        return self.db.query(MarketDataImportLog).filter(MarketDataImportLog.import_date >= date_from, MarketDataImportLog.is_deleted == False).order_by(desc(MarketDataImportLog.created_at)).limit(limit).all()
