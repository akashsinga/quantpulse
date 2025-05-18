# backend/schemas/user.py

from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime


class UserBase(BaseModel):
    email: EmailStr
    username: str = Field(..., min_length=5, max_length=50)
    full_name: Optional[str] = None
    is_admin: bool = False


class UserCreate(UserBase):
    password: str = Field(..., min_length=8)


class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    is_superuser: Optional[bool] = None
    is_active: Optional[bool] = None
    password: Optional[str] = Field(None, min_length=8)


class UserInDB(UserBase):
    id: int
    is_active: bool
    is_superuser: bool = False
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class UserResponse(UserInDB):
    pass


class UserLogin(BaseModel):
    username: str
    password: str
