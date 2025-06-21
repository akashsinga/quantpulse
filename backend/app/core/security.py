# backend/app/core/security.py
"""
Security utilities for authentication and authorization.
"""

from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from jose import jwt, JWTError
from passlib.context import CryptContext

from app.core.config import settings
from app.core.exceptions import AuthenticationError, AuthorizationError
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class PasswordManager:
    """Handles password hashing and verification"""

    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash"""
        try:
            return pwd_context.verify(plain_password, hashed_password)
        except Exception as e:
            logger.error(f"Password verification error: {e}")
            return False

    @staticmethod
    def get_hashed_password(password: str) -> str:
        """Generates a password hash for the plain password"""
        return pwd_context.hash(password)

    @staticmethod
    def validate_password_strength(password: str) -> tuple[bool, list[str]]:
        """Validates a password's strength"""
        issues = []

        if len(password) < 8:
            issues.append("Password must be at least 8 characters long")

        if not any(c.isupper() for c in password):
            issues.append("Password must contain at least one uppercase letter")

        if not any(c.islower() for c in password):
            issues.append("Password must contain at least one lowercase letter")

        if not any(c.isdigit() for c in password):
            issues.append("Password must contain at least one digit")

        # Optional: Special characters
        special_chars = "!@#$%^&*()_+-=[]{}|;:,.<>?"
        if not any(c in special_chars for c in password):
            issues.append("Password should contain at least one special character")

        return len(issues) == 0, issues


class TokenManager:
    """Handles JWT token creation and validation"""

    def __init__(self):
        self.secret_key = settings.auth.AUTH_SECRET_KEY
        self.algorithm = settings.auth.AUTH_ALGORITHM
        self.access_token_expire_minutes = settings.auth.AUTH_ACCESS_TOKEN_EXPIRE_MINUTES

    def create_access_token(self, data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
        """Creates a JWT access token"""
        to_encode = data.copy()

        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=self.access_token_expire_minutes)

        to_encode.update({"exp": expire, "iat": datetime.utcnow()})

        try:
            encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
            return encoded_jwt
        except Exception as e:
            logger.error(f"Token creation error : {e}")
            raise AuthenticationError("Failed to create access token")

    def verify_token(self, token: str) -> Dict[str, Any]:
        """Verify and decode a JWT token."""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            raise AuthenticationError("Token has expired")
        except JWTError as e:
            logger.warning(f"Token verification failed: {e}")
            raise AuthenticationError("Invalid token")


class PermissionChecker:
    """Handles authorization and permission checking"""

    @staticmethod
    def check_user_permission(user, permission: str, resource: Optional[str] = None) -> bool:
        # Superuser has all permissions
        if hasattr(user, 'is_superuser') and user.is_superuser:
            return True

        # Check if user is active
        if hasattr(user, 'is_active') and not user.is_active:
            return False

        # Add actual permission checking logic here
        # For now, return False for non-superusers
        return False

    @staticmethod
    def require_permission(user, permission: str, resource: Optional[str] = None):
        """Require permission or raise AuthorizationError"""
        if not PermissionChecker.check_user_permission(user, permission, resource):
            raise AuthorizationError(permission, resource)


password_manager = PasswordManager()
token_manager = TokenManager()
permission_checker = PermissionChecker()
