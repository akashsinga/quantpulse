# backend/app/schemas/auth.py

from pydantic import BaseModel
from .base import BaseResponseSchema
from typing import Optional
from datetime import datetime


class Token(BaseResponseSchema):
    access_token: str
    token_type: str
    expires_at: datetime


class TokenData(BaseModel):
    email: Optional[str] = None
