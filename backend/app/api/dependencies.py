# backend/app/api/dependencies.py
"""
FastAPI dependencies for QuantPulse application.
"""

from typing import Optional, Generator
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.security import token_manager, permission_checker
from app.core.exceptions import AuthenticationError, AuthorizationError, to_http_exception
from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

# OAuth2Scheme for token extraction
oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"{settings.api.API_V1_PREFIX}/auth/login")


async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    """Get the current authenticated user from JWT token"""
    try:
        # Verify and decode token
        payload = token_manager.verify_token(token)
        email: Optional[str] = payload.get("sub")

        if email is None:
            raise AuthenticationError("Token missing user identifier")

        # Imported here to avoid circular imports
        from app.repositories.users import UserRepository

        user_repo = UserRepository(db)
        user = user_repo.get_by_email(email)

        if user is None:
            raise AuthenticationError("User not found")

        if not user.is_active:
            raise AuthenticationError("User account is inactive")

        return user

    except (AuthenticationError, AuthorizationError) as e:
        raise to_http_exception(e)
    except Exception as e:
        logger.error(f"Unexpected error during authentication: {e}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")


async def get_current_active_user(current_user=Depends(get_current_user)):
    """Get current active user (additional check for user activity)."""
    if not current_user.is_active:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Inactive user")
    return current_user


async def get_current_superuser(current_user=Depends(get_current_user)):
    """Get current user and verify superuser status."""
    try:
        permission_checker.require_permission(current_user, "admin_access")
        return current_user
    except AuthorizationError as e:
        raise to_http_exception(e)


def get_pagination_params(skip: int = 0, limit: int = 100, max_limit: int = 1000):
    """
    Get pagination parameters with validation.
    
    Args:
        skip: Number of records to skip
        limit: Maximum number of records to return
        max_limit: Maximum allowed limit
        
    Returns:
        Tuple of (skip, limit)
        
    Raises:
        HTTPException: If parameters are invalid
    """
    if skip < 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Skip parameter must be non-negative")

    if limit <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Limit parameter must be positive")

    if limit > max_limit:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Limit parameter cannot exceed {max_limit}")

    return skip, min(limit, max_limit)


# Common dependency combinations
CurrentUser = Depends(get_current_user)
CurrentActiveUser = Depends(get_current_active_user)
CurrentSuperuser = Depends(get_current_superuser)
DatabaseSession = Depends(get_db)
PaginationParams = Depends(get_pagination_params)
