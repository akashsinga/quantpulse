# backend/api/v1/auth.py
"""Authentication Router"""

from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from uuid import UUID

from app.core.database import get_db
from app.core.security import TokenManager
from app.services.auth_service import AuthService
from app.repositories.users import UserRepository, UserPreferencesRepository
from app.core.config import settings
from app.schemas.auth import LoginResponse
from app.schemas.base import APIResponse
from app.schemas.users import UserAdminResponse, UserPreferencesBase
from app.utils.logger import get_logger
from app.core.exceptions import NotFoundError, to_http_exception
from app.api.dependencies import get_current_active_user, get_current_superuser

router = APIRouter()
logger = get_logger(__name__)


@router.post("/login", response_model=LoginResponse)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db=Depends(get_db)):
    auth_service = AuthService(db)
    user = auth_service.authenticate_user(form_data.username, form_data.password)

    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password", headers={"WWW-Authenticate": "Bearer"})

    access_token_expires = timedelta(minutes=settings.auth.AUTH_ACCESS_TOKEN_EXPIRE_MINUTES)
    expires_at = datetime.now(tz=settings.INDIA_TZ) + access_token_expires

    access_token = TokenManager().create_access_token(data={"sub": user.email, "is_superuser": user.is_superuser}, expires_delta=access_token_expires)

    return LoginResponse(access_token=access_token, token_type="bearer", expires_at=expires_at, user=user)


# Current user's profile
@router.get('/profile', response_model=APIResponse[UserAdminResponse])
async def get_current_user_profile(db=Depends(get_db), current_user=Depends(get_current_active_user)):
    try:
        user_preferences = UserPreferencesRepository(db).get_by_user_id(current_user.id)
        preferences_response = UserPreferencesBase.model_validate(user_preferences) if user_preferences else None

        user_response = UserAdminResponse(id=current_user.id, email=current_user.email, full_name=current_user.full_name, is_active=current_user.is_active, is_superuser=current_user.is_superuser, preferences=preferences_response, created_at=current_user.created_at, updated_at=current_user.updated_at)

        return APIResponse(data=user_response, message="User profile retrieved successfully")

    except Exception as e:
        logger.error(f"Error getting current user profile: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve profile")


# Specific user's profile (admin only)
@router.get('/profile/{user_id}', response_model=APIResponse[UserAdminResponse])
async def get_user_profile(user_id: UUID, db=Depends(get_db), current_user=Depends(get_current_superuser)):
    try:
        user = UserRepository(db).get_by_id_or_raise(user_id)
        user_preferences = UserPreferencesRepository(db).get_by_user_id(user_id)

        preferences_response = UserPreferencesBase.model_validate(user_preferences) if user_preferences else None

        user_response = UserAdminResponse(id=user.id, email=user.email, full_name=user.full_name, is_active=user.is_active, is_superuser=user.is_superuser, preferences=preferences_response, created_at=user.created_at, updated_at=user.updated_at)

        return APIResponse(data=user_response, message="User profile retrieved successfully")

    except NotFoundError as e:
        raise to_http_exception(e)
    except Exception as e:
        logger.error(f"Error getting user profile {user_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve user profile")
