# backend/app/schemas/ohlcv.py
"""
Pydantic schemas for OHLCV (Market Data) API requests and responses.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, date as datetype
from decimal import Decimal
from pydantic import BaseModel, Field, validator
from uuid import UUID

from app.schemas.base import BaseResponseSchema
from app.utils.enum import Timeframe


# Base Schemas
class OHLCVBase(BaseModel):
    """Base OHLCV schema with common fields"""
    date: datetype = Field(..., description="Trading date")
    timeframe: str = Field(Timeframe.DAILY.value, description="Data timeframe")
    open_price: Decimal = Field(..., gt=0, description="Opening price")
    high_price: Decimal = Field(..., gt=0, description="Highest price")
    low_price: Decimal = Field(..., gt=0, description="Lowest price")
    close_price: Decimal = Field(..., gt=0, description="Closing price")
    volume: int = Field(0, ge=0, description="Trading volume")


# Request Schemas
class OHLCVImportRequest(BaseModel):
    """Schema for OHLCV data import requests"""
    security_id: Optional[UUID] = Field(None, description="Specific security ID (None for all)")
    date_from: Optional[datetype] = Field(None, description="Start date for import")
    date_to: Optional[datetype] = Field(None, description="End date for import")
    timeframe: str = Field(Timeframe.DAILY.value, description="Data timeframe")
    import_type: str = Field("INCREMENTAL", description="Import type (FULL, INCREMENTAL, BACKFILL)")
    force_update: bool = Field(False, description="Force update existing data")

    @validator('import_type')
    def validate_import_type(cls, v):
        if v not in ['FULL', 'INCREMENTAL', 'BACKFILL']:
            raise ValueError('Import type must be FULL, INCREMENTAL, or BACKFILL')
        return v

    @validator('timeframe')
    def validate_timeframe(cls, v):
        if v not in [t.value for t in Timeframe]:
            raise ValueError(f'Invalid timeframe: {v}')
        return v

    @validator('date_to')
    def validate_date_range(cls, v, values):
        if 'date_from' in values and values['date_from'] and v:
            if values['date_from'] > v:
                raise ValueError('date_from cannot be greater than date_to')
        return v

    class Config:
        json_schema_extra = {"example": {"security_id": "550e8400-e29b-41d4-a716-446655440000", "date_from": "2024-01-01", "date_to": "2024-01-31", "timeframe": "1D", "import_type": "INCREMENTAL", "force_update": False}}


class OHLCVBulkRequest(BaseModel):
    """Schema for bulk OHLCV data requests"""
    security_ids: List[UUID] = Field(..., min_items=1, max_items=50, description="List of security IDs")
    date_from: Optional[datetype] = Field(None, description="Start date")
    date_to: Optional[datetype] = Field(None, description="End date")
    timeframe: str = Field(Timeframe.DAILY.value, description="Data timeframe")
    limit_per_security: int = Field(100, le=1000, description="Max records per security")


# Response Schemas
class OHLCVResponse(BaseResponseSchema):
    """Schema for OHLCV data responses"""
    id: UUID
    security_id: UUID
    date: datetype
    timeframe: str
    open_price: Decimal = Field(..., alias="open")
    high_price: Decimal = Field(..., alias="high")
    low_price: Decimal = Field(..., alias="low")
    close_price: Decimal = Field(..., alias="close")
    volume: int
    value: Optional[Decimal] = None
    trades: Optional[int] = None
    deliverable_qty: Optional[int] = None
    deliverable_percent: Optional[Decimal] = None
    adjusted_close: Optional[Decimal] = None
    adjustment_factor: Optional[Decimal] = Field(1.0, description="Corporate action adjustment factor")
    is_adjusted: bool = Field(False, description="Whether prices are adjusted for corporate actions")
    data_source: str

    # Calculated fields
    typical_price: Decimal = Field(..., description="Typical price (HLC/3)")
    price_change: Decimal = Field(..., description="Price change (Close - Open)")
    price_change_percent: Decimal = Field(..., description="Price change percentage")
    trading_range: Decimal = Field(..., description="Trading range (High - Low)")
    is_green: bool = Field(..., description="Whether candle is green (close > open)")
    is_red: bool = Field(..., description="Whether candle is red (close < open)")
    is_doji: bool = Field(..., description="Whether candle is doji (close ≈ open)")

    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "security_id": "550e8400-e29b-41d4-a716-446655440001",
                "date": "2024-01-15",
                "timeframe": "1D",
                "open": 2650.0,
                "high": 2670.0,
                "low": 2640.0,
                "close": 2660.0,
                "volume": 1500000,
                "value": 3990000000.0,
                "trades": 125000,
                "typical_price": 2656.67,
                "price_change": 10.0,
                "price_change_percent": 0.38,
                "trading_range": 30.0,
                "is_green": True,
                "is_red": False,
                "is_doji": False,
                "data_source": "DHAN",
                "created_at": "2024-01-16T10:00:00Z",
                "updated_at": "2024-01-16T10:00:00Z"
            }
        }


class OHLCVStatsResponse(BaseModel):
    """Schema for OHLCV statistical analysis"""
    total_records: int
    date_range: Dict[str, Any]
    price_statistics: Dict[str, Dict[str, float]]
    returns: Dict[str, float]
    trading_days: Dict[str, Any]
    error: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "total_records": 90,
                "date_range": {
                    "start": "2024-01-01",
                    "end": "2024-03-31",
                    "days": 90
                },
                "price_statistics": {
                    "close": {
                        "mean": 2650.0,
                        "std": 150.5,
                        "min": 2400.0,
                        "max": 2900.0,
                        "median": 2645.0
                    },
                    "volume": {
                        "mean": 1500000,
                        "std": 500000,
                        "min": 800000,
                        "max": 3000000
                    }
                },
                "returns": {
                    "mean_daily_return": 0.12,
                    "volatility_daily": 0.025,
                    "volatility_annual": 0.40,
                    "max_gain": 0.08,
                    "max_loss": -0.06
                },
                "trading_days": {
                    "up_days": 48,
                    "down_days": 40,
                    "unchanged_days": 2,
                    "up_percentage": 53.33
                }
            }
        }


class OHLCVImportStatusResponse(BaseResponseSchema):
    """Schema for OHLCV import status responses"""
    task_id: UUID
    celery_task_id: Optional[str]
    status: str
    message: str
    progress_percentage: int = 0
    created_at: datetime
    completed_at: Optional[datetime] = None
    result_data: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True
        json_schema_extra = {"example": {"task_id": "550e8400-e29b-41d4-a716-446655440000", "celery_task_id": "12345678-1234-1234-1234-123456789012", "status": "PROGRESS", "message": "Processing OHLCV import for 50 securities...", "progress_percentage": 65, "created_at": "2024-01-16T10:00:00Z", "result_data": {"securities_processed": 32, "records_created": 3200, "records_updated": 150}}}


class DataCoverageResponse(BaseModel):
    """Schema for data coverage analysis responses"""
    total_securities: int
    securities_with_data: int
    average_coverage_percentage: float
    earliest_data_date: Optional[str]
    latest_data_date: Optional[str]
    securities_coverage: List[Dict[str, Any]]

    class Config:
        json_schema_extra = {"example": {"total_securities": 100, "securities_with_data": 85, "average_coverage_percentage": 87.5, "earliest_data_date": "2023-01-01", "latest_data_date": "2024-01-15", "securities_coverage": [{"security_id": "550e8400-e29b-41d4-a716-446655440000", "symbol": "RELIANCE", "name": "Reliance Industries Limited", "earliest_date": "2023-01-01", "latest_date": "2024-01-15", "total_records": 365, "coverage_percentage": 95.2}]}}


class OHLCVBulkResponse(BaseModel):
    """Schema for bulk OHLCV data responses"""
    data: Dict[str, List[OHLCVResponse]] = Field(..., description="Security ID mapped to OHLCV data")
    summary: Dict[str, Any] = Field(..., description="Summary of bulk operation")

    class Config:
        json_schema_extra = {
            "example": {
                "data": {
                    "550e8400-e29b-41d4-a716-446655440000": [
                        # List of OHLCVResponse objects
                    ]
                },
                "summary": {
                    "requested_securities": 5,
                    "successful_securities": 4,
                    "failed_securities": 1,
                    "failed_security_ids": ["550e8400-e29b-41d4-a716-446655440001"],
                    "date_range": {
                        "from": "2024-01-01",
                        "to": "2024-01-31"
                    }
                }
            }
        }


# Technical Indicator Schemas
class TechnicalIndicatorBase(BaseModel):
    """Base schema for technical indicators"""
    indicator_name: str = Field(..., description="Indicator name (e.g., SMA_20, RSI_14)")
    indicator_value: Decimal = Field(..., description="Calculated indicator value")
    calculation_params: Optional[str] = Field(None, description="Parameters used for calculation")


class TechnicalIndicatorResponse(BaseResponseSchema):
    """Schema for technical indicator responses"""
    id: UUID
    ohlcv_data_id: UUID
    indicator_name: str
    indicator_value: Decimal
    calculation_params: Optional[str]
    calculation_timestamp: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class TechnicalIndicatorRequest(BaseModel):
    """Schema for technical indicator calculation requests"""
    indicators: List[str] = Field(..., description="List of indicators to calculate")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Custom parameters for indicators")
    recalculate: bool = Field(False, description="Force recalculation of existing indicators")

    @validator('indicators')
    def validate_indicators(cls, v):
        valid_indicators = ['SMA', 'EMA', 'RSI', 'MACD', 'BB', 'STOCH']
        for indicator in v:
            base_indicator = indicator.split('_')[0]
            if base_indicator not in valid_indicators:
                raise ValueError(f'Unsupported indicator: {indicator}')
        return v

    class Config:
        json_schema_extra = {"example": {"indicators": ["SMA_20", "RSI_14", "MACD_12_26_9"], "parameters": {"SMA_20": {"period": 20}, "RSI_14": {"period": 14}}, "recalculate": False}}


# Market Data Import Log Schemas
class ImportLogResponse(BaseResponseSchema):
    """Schema for import log responses"""
    id: UUID
    security_id: Optional[UUID]
    import_date: datetype
    date_from: datetype
    date_to: datetype
    total_records_processed: int
    records_created: int
    records_updated: int
    records_skipped: int
    records_failed: int
    status: str
    data_source: str
    import_type: str
    error_message: Optional[str]
    execution_time_seconds: Optional[int]
    api_calls_made: Optional[int]
    created_at: datetime

    class Config:
        from_attributes = True


# Filter and Search Schemas
class OHLCVFilters(BaseModel):
    """Schema for OHLCV data filtering"""
    security_ids: Optional[List[UUID]] = None
    date_from: Optional[datetype] = None
    date_to: Optional[datetype] = None
    timeframe: Optional[str] = Timeframe.DAILY.value
    min_volume: Optional[int] = Field(None, ge=0, description="Minimum volume filter")
    max_volume: Optional[int] = Field(None, ge=0, description="Maximum volume filter")
    min_price: Optional[Decimal] = Field(None, gt=0, description="Minimum price filter")
    max_price: Optional[Decimal] = Field(None, gt=0, description="Maximum price filter")
    only_green_candles: Optional[bool] = Field(None, description="Only green candles")
    only_red_candles: Optional[bool] = Field(None, description="Only red candles")
    min_range_percent: Optional[float] = Field(None, ge=0, le=100, description="Minimum trading range percentage")

    @validator('max_volume')
    def validate_volume_range(cls, v, values):
        if 'min_volume' in values and values['min_volume'] and v:
            if values['min_volume'] > v:
                raise ValueError('min_volume cannot be greater than max_volume')
        return v

    @validator('max_price')
    def validate_price_range(cls, v, values):
        if 'min_price' in values and values['min_price'] and v:
            if values['min_price'] > v:
                raise ValueError('min_price cannot be greater than max_price')
        return v
