# backend/api/v1/auth.py
"""Authentication Router"""

from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm

from app.core.database import get_db
from app.core.security import TokenManager
from app.services.auth_service import authenticate_user
from app.core.config import settings
from app.schemas.auth import Token

router = APIRouter()


@router.post("/login", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db=Depends(get_db)):
    user = authenticate_user(db, form_data.username, form_data.password)

    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password", headers={"WWW-Authenticate": "Bearer"})

    access_token_expires = timedelta(minutes=settings.auth.AUTH_ACCESS_TOKEN_EXPIRE_MINUTES)
    expires_at = datetime.now(tz=settings.INDIA_TZ) + access_token_expires

    access_token = TokenManager().create_access_token(data={"sub": user.email, "is_superuser": user.is_superuser}, expires_delta=access_token_expires)

    return Token(access_token=access_token, token_type="bearer", expires_at=expires_at)
