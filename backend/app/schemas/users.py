# backend/app/schemas/users.py
"""
Pydantic schemas for user-related API requests and responses.
"""

from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, EmailStr, Field, validator
from uuid import UUID

from app.schemas.base import BaseResponseSchema


class UserBase(BaseModel):
    """Base user schema with common fields"""
    email: EmailStr
    full_name: Optional[str] = None


class UserPreferencesBase(BaseResponseSchema):
    theme: str
    language: str
    currency: str
    preferred_timeline: str
    email_notifications: bool
    signal_notifications: bool
    portfolio_alerts: bool
    system_notifications: bool

    class Config:
        from_attributes = True


class UserCreate(UserBase):
    """Schema for user creation"""
    password: str = Field(..., min_length=8, max_length=128)
    confirm_password: str = Field(..., min_length=8, max_length=128)

    @validator('confirm_password')
    def passwords_match(cls, v, values):
        if 'password' in values and v != values['password']:
            raise ValueError('Passwords do not match')
        return v

    class Config:
        json_schema_extra = {"example": {"email": "user@example.com", "password": "SecurePassword123!", "confirm_password": "SecurePassword123!", "full_name": "John Doe"}}


class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    is_active: Optional[bool] = None


class UserAdminUpdate(UserUpdate):
    is_superuser: Optional[bool] = None


class PasswordChange(BaseModel):
    current_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=8, max_length=128)
    confirm_new_password: str = Field(..., min_length=8, max_length=128)

    @validator('confirm_new_password')
    def passwords_match(cls, v, values):
        if 'new_password' in values and v != values['new_password']:
            raise ValueError('New passwords do not match')
        return v


class UserResponse(BaseResponseSchema):
    id: UUID
    email: EmailStr
    full_name: str
    is_active: bool
    preferences: Optional[UserPreferencesBase] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
        json_schema_extra = {"example": {"id": "550e8400-e29b-41d4-a716-446655440000", "email": "user@example.com", "full_name": "John Doe", "is_active": True, "created_at": "2024-01-01T10:00:00Z", "updated_at": "2024-01-01T10:00:00Z"}}


class UserAdminResponse(UserResponse):
    is_superuser: bool

    class Config:
        from_attributes = True
