# backend/app/api/v1/holidays.py

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date, timedelta
from uuid import UUID

from app.db.session import get_db_session
from app.db.models.user import User
from app.api.deps import get_current_user, get_current_superadmin
from app.services.holiday_service import HolidayService
from app.schemas.holiday import (HolidayCreate, HolidayUpdate, HolidayResponse, TradingDayCheck, TradingDayResponse, TradingDaysRequest, TradingDaysResponse)
from app.utils.logger import get_logger

router = APIRouter()
logger = get_logger(__name__)


@router.get("/check-trading-day", response_model=TradingDayResponse)
async def check_trading_day(check_date: date = Query(..., description="Date to check"), exchange_code: str = Query("NSE", description="Exchange code"), db: Session = Depends(get_db_session), current_user: User = Depends(get_current_user)):
    """Check if a specific date is a trading day"""
    holiday_service = HolidayService(db)

    is_trading_day = holiday_service.is_trading_day(check_date, exchange_code)
    is_weekend = check_date.weekday() >= 5
    is_holiday = holiday_service.is_market_holiday(check_date, exchange_code)

    holiday_name = None
    if is_holiday:
        holidays = holiday_service.get_holidays_in_range(check_date, check_date, exchange_code)
        if holidays:
            holiday_name = holidays[0].holiday_name

    next_trading_day = holiday_service.get_next_trading_day(check_date, exchange_code)
    previous_trading_day = holiday_service.get_previous_trading_day(check_date, exchange_code)

    return TradingDayResponse(date=check_date, is_trading_day=is_trading_day, is_weekend=is_weekend, is_holiday=is_holiday, holiday_name=holiday_name, next_trading_day=next_trading_day, previous_trading_day=previous_trading_day)


@router.get("/trading-days", response_model=TradingDaysResponse)
async def get_trading_days(start_date: date = Query(..., description="Start date"), end_date: date = Query(..., description="End date"), exchange_code: str = Query("NSE", description="Exchange code"), db: Session = Depends(get_db_session), current_user: User = Depends(get_current_user)):
    """Get trading days and holidays in a date range"""
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="Start date must be before end date")

    # Limit range to prevent abuse
    if (end_date - start_date).days > 1095:  # 3 years
        raise HTTPException(status_code=400, detail="Date range cannot exceed 3 years")

    holiday_service = HolidayService(db)

    trading_days = holiday_service.get_trading_days_between(start_date, end_date, exchange_code)
    holidays = holiday_service.get_holidays_in_range(start_date, end_date, exchange_code)

    total_days = (end_date - start_date).days + 1
    weekend_days = sum(1 for d in range(total_days) if (start_date + timedelta(days=d)).weekday() >= 5)

    # Convert holidays to response format
    holiday_responses = []
    for holiday in holidays:
        holiday_responses.append(HolidayResponse(id=holiday.id, holiday_date=holiday.holiday_date, holiday_name=holiday.holiday_name, holiday_type=holiday.holiday_type, description=holiday.description, is_recurring=holiday.is_recurring, exchange_id=holiday.exchange_id, exchange_code=holiday.exchange.code, created_at=holiday.created_at.date(), updated_at=holiday.updated_at.date()))

    return TradingDaysResponse(start_date=start_date, end_date=end_date, exchange_code=exchange_code, total_days=total_days, trading_days=len(trading_days), weekend_days=weekend_days, holidays=len(holidays), trading_day_list=trading_days, holiday_list=holiday_responses)


@router.get("/", response_model=List[HolidayResponse])
async def get_holidays(start_date: Optional[date] = Query(None, description="Start date filter"), end_date: Optional[date] = Query(None, description="End date filter"), exchange_code: str = Query("NSE", description="Exchange code"), db: Session = Depends(get_db_session), current_user: User = Depends(get_current_user)):
    """Get all holidays with optional date filtering"""
    holiday_service = HolidayService(db)

    # Default to current year if no dates provided
    if not start_date and not end_date:
        from datetime import datetime
        current_year = datetime.now().year
        start_date = date(current_year, 1, 1)
        end_date = date(current_year, 12, 31)
    elif start_date and not end_date:
        end_date = date(start_date.year, 12, 31)
    elif not start_date and end_date:
        start_date = date(end_date.year, 1, 1)

    holidays = holiday_service.get_holidays_in_range(start_date, end_date, exchange_code)

    return [HolidayResponse(id=holiday.id, holiday_date=holiday.holiday_date, holiday_name=holiday.holiday_name, holiday_type=holiday.holiday_type, description=holiday.description, is_recurring=holiday.is_recurring, exchange_id=holiday.exchange_id, exchange_code=holiday.exchange.code, created_at=holiday.created_at.date(), updated_at=holiday.updated_at.date()) for holiday in holidays]


@router.post("/", response_model=HolidayResponse)
async def create_holiday(holiday_data: HolidayCreate, db: Session = Depends(get_db_session), current_user: User = Depends(get_current_superadmin)):
    """Create a new market holiday (Admin only)"""
    try:
        holiday_service = HolidayService(db)
        holiday = holiday_service.add_holiday(holiday_date=holiday_data.holiday_date, holiday_name=holiday_data.holiday_name, exchange_code=holiday_data.exchange_code, holiday_type=holiday_data.holiday_type, description=holiday_data.description)

        return HolidayResponse(id=holiday.id, holiday_date=holiday.holiday_date, holiday_name=holiday.holiday_name, holiday_type=holiday.holiday_type, description=holiday.description, is_recurring=holiday.is_recurring, exchange_id=holiday.exchange_id, exchange_code=holiday.exchange.code, created_at=holiday.created_at.date(), updated_at=holiday.updated_at.date())

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating holiday: {e}")
        raise HTTPException(status_code=500, detail="Failed to create holiday")


@router.delete("/{holiday_id}")
async def delete_holiday(holiday_id: UUID, db: Session = Depends(get_db_session), current_user: User = Depends(get_current_superadmin)):
    """Delete a market holiday (Admin only)"""
    from app.db.models.market_holiday import MarketHoliday

    holiday = db.query(MarketHoliday).filter(MarketHoliday.id == holiday_id).first()
    if not holiday:
        raise HTTPException(status_code=404, detail="Holiday not found")

    db.delete(holiday)
    db.commit()

    logger.info(f"Deleted holiday: {holiday.holiday_name} on {holiday.holiday_date}")
    return {"message": "Holiday deleted successfully"}
