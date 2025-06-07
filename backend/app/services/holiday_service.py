# backend/app/services/holiday_service.py

from datetime import date, timedelta
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc

from app.db.models.market_holiday import MarketHoliday
from app.db.models.exchange import Exchange
from app.utils.logger import get_logger

logger = get_logger(__name__)


class HolidayService:
    """Service for market holiday operations"""

    def __init__(self, db: Session):
        self.db = db

    def is_trading_day(self, check_date: date, exchange_code: str = "NSE"):
        """Check if a given date is a trading day"""
        if check_date.weekday() >= 5:
            return False

        holiday = self.db.query(MarketHoliday).join(Exchange).filter(and_(MarketHoliday.holiday_date == check_date, Exchange.code == exchange_code)).first()

        return holiday is None

    def is_market_holiday(self, check_date: date, exchange_code: str = "NSE"):
        """Check if a given date is a trading holiday"""
        holiday = self.db.query(MarketHoliday).join(Exchange).filter(and_(MarketHoliday.holiday_date == check_date, Exchange.code == exchange_code)).first()
        return holiday is not None

    def get_next_trading_day(self, from_date: date, exchange_code: str = "NSE"):
        """Get next trading day after given date"""
        next_date = from_date + timedelta(days=1)

        while not self.is_trading_day(next_date, exchange_code):
            next_date += timedelta(days=1)
            if (next_date - from_date).days > 30:
                logger.warning(f"Could not find trading day within 30 days of {from_date}")
                break

        return next_date

    def get_previous_trading_day(self, from_date: date, exchange_code: str = "NSE"):
        """Get previous trading day before given date"""
        prev_date = from_date - timedelta(days=1)
        while not self.is_trading_day(prev_date, exchange_code):
            prev_date -= timedelta(days=1)
            if (from_date - prev_date).days > 30:
                logger.warning(f"Could not find trading day within 30 days before {from_date}")
                break

        return prev_date

    def get_trading_days_between(self, start_date: date, end_date: date, exchange_code: str = "NSE"):
        """Get all trading days between two dates (inclusive)"""
        trading_days = []
        current = start_date

        while current <= end_date:
            if self.is_trading_day(current, exchange_code):
                trading_days.append(current)
            current += timedelta(days=1)

        return trading_days

    def get_holidays_in_range(self, start_date: date, end_date: date, exchange_code: str = "NSE"):
        """Get all holidays in date range"""
        return self.db.query(MarketHoliday).join(Exchange).filter(and_(MarketHoliday.holiday_date >= start_date, MarketHoliday.holiday_date <= end_date, Exchange.code == exchange_code)).order_by(MarketHoliday.holiday_date).all()

    def count_trading_days(self, start_date: date, end_date: date, exchange_code: str = "NSE"):
        """Count trading days between two dates"""
        return len(self.get_trading_days_between((start_date, end_date, exchange_code)))

    def add_holiday(self, holiday_date: date, holiday_name: str, exchange_code: str = "NSE", holiday_type: str = "full", description: Optional[str] = None):
        """Add a new market holiday"""
        exchange = self.db.query(Exchange).filter(Exchange.code == exchange_code).first()

        if not exchange:
            raise ValueError(f"Exchange {exchange_code} not found")

        # Check if holiday already exists
        existing = self.db.query(MarketHoliday).filter(and_(MarketHoliday.holiday_date == holiday_date, MarketHoliday.exchange == exchange_code)).first()

        if existing:
            logger.info("Holiday already exists")
            return

        holiday = MarketHoliday(holiday_date=holiday_date, exchange_id=exchange.id, holiday_name=holiday_name, holiday_type=holiday_type, description=description)

        self.db.add(holiday)
        self.db.commit()
        self.db.refresh(holiday)

        logger.info(f"Added holiday: {holiday_name} on {holiday_date} for {exchange_code}")

        return holiday
