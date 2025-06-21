# backend/app/schemas/auth.py

from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime

from .users import UserResponse


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=1)

    class Config:
        json_schema_extra = {"example": {"email": "user@example.com", "password": "password123"}}


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_at: datetime
    user: UserResponse


class Token(BaseModel):
    access_token: str
    token_type: str
    expires_at: datetime


class TokenData(BaseModel):
    email: Optional[str] = None
