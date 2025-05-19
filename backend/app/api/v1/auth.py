# backend/api/routers/auth.py

from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from app.db.session import get_db_session
from app.services.auth_service import authenticate_user, create_access_token
from app.config import settings
from schemas.auth import Token

router = APIRouter()


@router.post("/login", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db=Depends(get_db_session)):
    user = authenticate_user(db, form_data.username, form_data.password)

    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password", headers={"WWW-Authenticate": "Bearer"})

    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    expires_at = datetime.now(tz=settings.INDIA_TZ) + access_token_expires

    access_token = create_access_token(data={"sub": user.email, "is_superuser": user.is_superuser}, expires_delta=access_token_expires)

    return Token(access_token=access_token, token_type="bearer", expires_at=expires_at)
