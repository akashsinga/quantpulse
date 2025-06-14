# app/api/dependencies.py
"""
FastAPI dependencies for QuantPulse application.
Cleaned up and organized from app/api/deps.py
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

# OAuth2 scheme for token extraction
oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"{settings.api.API_V1_PREFIX}/auth/login")


async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    """
    Get the current authenticated user from JWT token.
    
    Args:
        token: JWT token from Authorization header
        db: Database session
        
    Returns:
        User object
        
    Raises:
        HTTPException: If token is invalid or user not found
    """
    try:
        # Verify and decode token
        payload = token_manager.verify_token(token)
        email: Optional[str] = payload.get("sub")

        if email is None:
            raise AuthenticationError("Token missing user identifier")

        # Import here to avoid circular imports
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
    """
    Get current active user (additional check for user activity).
    
    Args:
        current_user: Current user from get_current_user
        
    Returns:
        Active user object
        
    Raises:
        HTTPException: If user is inactive
    """
    if not current_user.is_active:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Inactive user")
    return current_user


async def get_current_superuser(current_user=Depends(get_current_user)):
    """
    Get current user and verify superuser status.
    
    Args:
        current_user: Current user from get_current_user
        
    Returns:
        Superuser object
        
    Raises:
        HTTPException: If user is not a superuser
    """
    try:
        permission_checker.require_permission(current_user, "admin_access")
        return current_user
    except AuthorizationError as e:
        raise to_http_exception(e)


def require_permission(permission: str, resource: Optional[str] = None):
    """
    Create a dependency that requires a specific permission.
    
    Args:
        permission: Required permission
        resource: Optional resource identifier
        
    Returns:
        Dependency function
    """

    def permission_dependency(current_user=Depends(get_current_user)):
        try:
            permission_checker.require_permission(current_user, permission, resource)
            return current_user
        except AuthorizationError as e:
            raise to_http_exception(e)

    return permission_dependency


def get_current_user_optional(token: Optional[str] = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    """
    Get current user if token is provided, otherwise return None.
    Useful for endpoints that work with or without authentication.
    
    Args:
        token: Optional JWT token
        db: Database session
        
    Returns:
        User object or None
    """
    if token is None:
        return None

    try:
        return get_current_user(token, db)
    except HTTPException:
        return None


def get_logger_for_request(request: Request):
    """
    Get logger with request context.
    
    Args:
        request: FastAPI request object
        
    Returns:
        Logger with request ID context
    """
    request_id = getattr(request.state, "request_id", "-")
    return get_logger(__name__, request_id=request_id)


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


def get_db_session() -> Generator[Session, None, None]:
    """
    Database session dependency with proper cleanup.
    
    Yields:
        Database session
    """
    return get_db()


# Common dependency combinations
CurrentUser = Depends(get_current_user)
CurrentActiveUser = Depends(get_current_active_user)
CurrentSuperuser = Depends(get_current_superuser)
DatabaseSession = Depends(get_db_session)
PaginationParams = Depends(get_pagination_params)
