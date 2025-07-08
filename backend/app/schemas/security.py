# backend/app/schemas/security.py
"""
Pydantic schemas for securities-based API requests and responses
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, date
from decimal import Decimal
from pydantic import BaseModel, Field, validator
from uuid import UUID

from app.schemas.base import BaseResponseSchema

# Base Schemas


class ExchangeBase(BaseModel):
    """Base exchange schema with common fields"""
    name: str = Field(..., description="Exchange name")
    code: str = Field(..., description="Exchange code (e.g., NSE, BSE)")
    country: Optional[str] = Field(None, description="Country where exchange is located")
    timezone: Optional[str] = Field(None, description="Exchange timezone")
    currency: str = Field(default="INR", description="Trading currency")
    trading_hours_start: Optional[str] = Field(None, description="Trading start time (HH:MM)")
    trading_hours_end: Optional[str] = Field(None, description="Trading end time (HH:MM)")


class SecurityBase(BaseModel):
    """Base security schema with common fields"""
    symbol: str = Field(..., description="Trading symbol")
    name: str = Field(..., description="Security name")
    isin: Optional[str] = Field(None, description="ISIN code")
    security_type: str = Field(..., description="Security type (STOCK, INDEX, DERIVATIVE)")
    segment: str = Field(..., description="Market segment")
    sector: Optional[str] = Field(None, description="Sector classification")
    industry: Optional[str] = Field(None, description="Industry classification")
    lot_size: int = Field(default=1, description="Minimum lot size")
    tick_size: str = Field(default="0.01", description="Minimum tick size")


class FutureBase(BaseModel):
    """Base future schema with common fields"""
    expiration_date: date = Field(..., description="Contract expiration date")
    contract_size: Decimal = Field(default=1.0, description="Contract size")
    settlement_type: str = Field(default="CASH", description="Settlement type")
    contract_month: str = Field(..., description="Contract month")


# Request Schemas


class ExchangeUpdate(BaseModel):
    """Schema for updating an exchange"""
    name: Optional[str] = None
    country: Optional[str] = None
    timezone: Optional[str] = None
    currency: Optional[str] = None
    trading_hours_start: Optional[str] = None
    trading_hours_end: Optional[str] = None
    is_active: Optional[bool] = None


class SecurityCreate(SecurityBase):
    """Schema for creating a new security"""
    exchange_id: UUID = Field(..., description="Exchange ID")
    external_id: int = Field(..., description="External API ID")
    is_active: bool = Field(default=True, description="Whether security is active")
    is_tradeable: bool = Field(default=True, description="Whether security is tradeable")
    is_derivatives_eligible: bool = Field(default=False, description="Whether derivatives are available")
    has_options: bool = Field(default=False, description="Whether options are available")
    has_futures: bool = Field(default=False, description="Whether futures are available")

    class Config:
        json_schema_extra = {"example": {"symbol": "RELIANCE", "name": "Reliance Industries Limited", "isin": "INE002A01018", "exchange_id": "550e8400-e29b-41d4-a716-446655440000", "external_id": 2885, "security_type": "STOCK", "segment": "EQUITY", "sector": "Energy", "industry": "Oil & Gas", "lot_size": 1, "tick_size": "0.05", "is_active": True, "is_tradeable": True, "is_derivatives_eligible": True, "has_options": True, "has_futures": True}}


class SecurityUpdate(BaseModel):
    """Schema for updating a security"""
    name: Optional[str] = None
    isin: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    lot_size: Optional[int] = None
    tick_size: Optional[str] = None
    is_active: Optional[bool] = None
    is_tradeable: Optional[bool] = None
    is_derivatives_eligible: Optional[bool] = None
    has_options: Optional[bool] = None
    has_futures: Optional[bool] = None


class SecuritySearchRequest(BaseModel):
    """Schema for security search requests"""
    query: str = Field(..., min_length=1, description="Search term")
    security_type: Optional[str] = Field(None, description="Filter by security type")
    segment: Optional[str] = Field(None, description="Filter by segment")
    exchange_id: Optional[UUID] = Field(None, description="Filter by exchange")
    active_only: bool = Field(default=True, description="Only return active securities")

    class Config:
        json_schema_extra = {"example": {"query": "RELIANCE", "security_type": "STOCK", "segment": "EQUITY", "active_only": True}}


class FutureCreate(FutureBase):
    """Schema for creating a new future"""
    security_id: UUID = Field(..., description="Security ID for the future contract")
    underlying_id: UUID = Field(..., description="Underlying security ID")
    is_active: bool = Field(default=True, description="Whether future is active")
    is_tradeable: bool = Field(default=True, description="Whether future is tradeable")
    previous_contract_id: Optional[UUID] = Field(None, description="Previous contract ID for rollover")
    next_contract_id: Optional[UUID] = Field(None, description="Next contract ID for rollover")

    class Config:
        json_schema_extra = {"example": {"security_id": "550e8400-e29b-41d4-a716-446655440001", "underlying_id": "550e8400-e29b-41d4-a716-446655440000", "expiration_date": "2024-03-28", "contract_size": 250, "settlement_type": "CASH", "contract_month": "MAR", "is_active": True, "is_tradeable": True}}


class ImportRequest(BaseModel):
    """Schema for securities import requests"""
    force_refresh: bool = Field(default=False, description="Force refresh of existing securities")

    class Config:
        json_schema_extra = {"example": {"force_refresh": False}}


# Response Schemas


class ExchangeResponse(BaseResponseSchema):
    """Schema for exchange responses"""
    id: UUID
    name: str
    code: str
    country: Optional[str]
    timezone: Optional[str]
    currency: str
    trading_hours_start: Optional[str]
    trading_hours_end: Optional[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
        json_schema_extra = {"example": {"id": "550e8400-e29b-41d4-a716-446655440000", "name": "National Stock Exchange of India", "code": "NSE", "country": "India", "timezone": "Asia/Kolkata", "currency": "INR", "trading_hours_start": "09:15", "trading_hours_end": "15:30", "is_active": True, "created_at": "2024-01-01T10:00:00Z", "updated_at": "2024-01-01T10:00:00Z"}}


class SecurityResponse(BaseResponseSchema):
    """Schema for security responses"""
    id: UUID
    symbol: str
    name: str
    isin: Optional[str]
    external_id: int
    security_type: str
    segment: str
    sector: Optional[str]
    industry: Optional[str]
    lot_size: int
    tick_size: str
    is_active: bool
    is_tradeable: bool
    is_derivatives_eligible: bool
    has_options: bool
    has_futures: bool
    exchange_id: UUID
    created_at: datetime
    updated_at: datetime

    # Optional nested exchange information
    exchange: Optional[ExchangeResponse] = None

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "symbol": "RELIANCE",
                "name": "Reliance Industries Limited",
                "isin": "INE002A01018",
                "external_id": 2885,
                "security_type": "STOCK",
                "segment": "EQUITY",
                "sector": "Energy",
                "industry": "Oil & Gas",
                "lot_size": 1,
                "tick_size": "0.05",
                "is_active": True,
                "is_tradeable": True,
                "is_derivatives_eligible": True,
                "has_options": True,
                "has_futures": True,
                "exchange_id": "550e8400-e29b-41d4-a716-446655440001",
                "created_at": "2024-01-01T10:00:00Z",
                "updated_at": "2024-01-01T10:00:00Z"
            }
        }


class SecurityStatsResponse(BaseResponseSchema):
    """Schema for security stats"""
    total: int = 0
    active: int = 0
    futures: int = 0
    derivatives: int = 0


class FutureResponse(BaseResponseSchema):
    """Schema for future responses"""
    security_id: UUID
    underlying_id: UUID
    expiration_date: date
    contract_size: Decimal
    settlement_type: str
    contract_month: str
    is_active: bool
    is_tradeable: bool
    previous_contract_id: Optional[UUID]
    next_contract_id: Optional[UUID]
    created_at: datetime
    updated_at: datetime

    # Computed properties
    is_expired: bool
    days_to_expiry: int
    is_near_expiry: bool
    contract_name: str

    # Optional nested information
    security: Optional[SecurityResponse] = None
    underlying: Optional[SecurityResponse] = None

    class Config:
        from_attributes = True
        json_schema_extra = {"example": {"security_id": "550e8400-e29b-41d4-a716-446655440001", "underlying_id": "550e8400-e29b-41d4-a716-446655440000", "expiration_date": "2024-03-28", "contract_size": 250, "settlement_type": "CASH", "contract_month": "MAR", "is_active": True, "is_tradeable": True, "is_expired": False, "days_to_expiry": 45, "is_near_expiry": False, "contract_name": "RELIANCE MAR 2024", "created_at": "2024-01-01T10:00:00Z", "updated_at": "2024-01-01T10:00:00Z"}}


class ImportStatusResponse(BaseResponseSchema):
    """Schema for import status responses"""
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
        json_schema_extra = {"example": {"task_id": "550e8400-e29b-41d4-a716-446655440000", "celery_task_id": "12345678-1234-1234-1234-123456789012", "status": "PROGRESS", "message": "Processing securities data...", "progress_percentage": 45, "created_at": "2024-01-01T10:00:00Z", "result_data": {"total_records": 5000, "processed": 2250, "created": 1500, "updated": 750, "errors": 0}}}


# Filter Schemas


class SecurityFilters(BaseModel):
    """Schema for security filtering options"""
    exchange_id: Optional[UUID] = None
    security_type: Optional[str] = None
    segment: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    is_active: Optional[bool] = True
    is_tradeable: Optional[bool] = None
    is_derivatives_eligible: Optional[bool] = None
    has_options: Optional[bool] = None
    has_futures: Optional[bool] = None

    @validator('security_type')
    def validate_security_type(cls, v):
        if v and v not in ['STOCK', 'INDEX', 'DERIVATIVE', 'ETF', 'BOND']:
            raise ValueError('Invalid security type')
        return v

    @validator('segment')
    def validate_segment(cls, v):
        if v and v not in ['EQUITY', 'DERIVATIVE', 'CURRENCY', 'COMMODITY', 'INDEX']:
            raise ValueError('Invalid segment')
        return v


class FutureFilters(BaseModel):
    """Schema for future filtering options"""
    underlying_id: Optional[UUID] = None
    contract_month: Optional[str] = None
    settlement_type: Optional[str] = None
    is_active: Optional[bool] = True
    is_tradeable: Optional[bool] = None
    expiry_from: Optional[date] = None
    expiry_to: Optional[date] = None
    is_expired: Optional[bool] = None

    @validator('settlement_type')
    def validate_settlement_type(cls, v):
        if v and v not in ['CASH', 'PHYSICAL']:
            raise ValueError('Invalid settlement type')
        return v

    @validator('contract_month')
    def validate_contract_month(cls, v):
        valid_months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEPT', 'OCT', 'NOV', 'DEC']
        if v and v not in valid_months:
            raise ValueError('Invalid contract month')
        return v


# Bulk Operation Schemas


class BulkSecurityUpdate(BaseModel):
    """Schema for bulk security updates"""
    security_ids: List[UUID] = Field(..., description="List of security IDs to update")
    updates: SecurityUpdate = Field(..., description="Updates to apply")

    class Config:
        json_schema_extra = {"example": {"security_ids": ["550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440001"], "updates": {"is_active": True, "is_tradeable": True}}}


class BulkOperationResult(BaseModel):
    """Schema for bulk operation results"""
    total: int = Field(..., description="Total records processed")
    successful: int = Field(..., description="Successfully processed records")
    failed: int = Field(..., description="Failed records")
    errors: List[str] = Field(default=[], description="List of error messages")

    class Config:
        json_schema_extra = {"example": {"total": 100, "successful": 98, "failed": 2, "errors": ["Security with ID 550e8400... not found", "Validation error for security 550e8400..."]}}
