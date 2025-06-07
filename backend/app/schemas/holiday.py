# backend/app/schemas/holiday.py

from pydantic import BaseModel, Field
from typing import Optional
from datetime import date as datetype
from uuid import UUID


class HolidayBase(BaseModel):
    holiday_date: datetype
    holiday_name: str = Field(..., min_length=1, max_length=255)
    holiday_type: str = Field(default="full", pattern="^(full|partial|settlement)$")
    description: Optional[str] = None
    is_recurring: bool = False


class HolidayCreate(HolidayBase):
    exchange_code: str = Field(default="NSE", min_length=1, max_length=20)


class HolidayUpdate(HolidayBase):
    holiday_name: Optional[str] = Field(None, min_length=1, max_length=255)
    holiday_type: Optional[str] = Field(None, pattern="^(full|partial|settlement)$")
    description: Optional[str] = None
    is_recurring: Optional[bool] = None


class HolidayResponse(HolidayBase):
    id: UUID
    exchange_id: UUID
    exchange_code: str
    created_at: datetype
    updated_at: datetype

    class Config:
        from_attributes = True


class TradingDayCheck(BaseModel):
    check_date: datetype
    exchange_code: str = "NSE"


class TradingDayResponse(BaseModel):
    date: datetype
    is_trading_day: bool
    is_weekend: bool
    is_holiday: bool
    holiday_name: Optional[str] = None
    next_trading_day: datetype
    previous_trading_day: datetype


class TradingDaysRequest(BaseModel):
    start_date: datetype
    end_date: datetype
    exchange_code: str = "NSE"


class TradingDaysResponse(BaseModel):
    start_date: datetype
    end_date: datetype
    exchange_code: str
    total_days: int
    trading_days: int
    weekend_days: int
    holidays: int
    trading_day_list: list[datetype]
    holiday_list: list[HolidayResponse]
