# app/core/security.py
"""
Security utilities for authentication and authorization.
Extracted and cleaned from app/services/auth_service.py
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
    """Handles password hashing and verification."""

    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        """
        Verify a password against its hash.
        
        Args:
            plain_password: Plain text password
            hashed_password: Hashed password from database
            
        Returns:
            True if password matches, False otherwise
        """
        try:
            return pwd_context.verify(plain_password, hashed_password)
        except Exception as e:
            logger.error(f"Password verification error: {e}")
            return False

    @staticmethod
    def get_password_hash(password: str) -> str:
        """
        Generate a password hash.
        
        Args:
            password: Plain text password
            
        Returns:
            Hashed password
        """
        return pwd_context.hash(password)

    @staticmethod
    def validate_password_strength(password: str) -> tuple[bool, list[str]]:
        """
        Validate password strength.
        
        Args:
            password: Password to validate
            
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
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
    """Handles JWT token creation and validation."""

    def __init__(self):
        self.secret_key = settings.auth.SECRET_KEY
        self.algorithm = settings.auth.ALGORITHM
        self.access_token_expire_minutes = settings.auth.ACCESS_TOKEN_EXPIRE_MINUTES

    def create_access_token(self, data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
        """
        Create a JWT access token.
        
        Args:
            data: Data to encode in token (typically user info)
            expires_delta: Custom expiration time
            
        Returns:
            Encoded JWT token
        """
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
            logger.error(f"Token creation error: {e}")
            raise AuthenticationError("Failed to create access token")

    def verify_token(self, token: str) -> Dict[str, Any]:
        """
        Verify and decode a JWT token.
        
        Args:
            token: JWT token to verify
            
        Returns:
            Decoded token payload
            
        Raises:
            AuthenticationError: If token is invalid or expired
        """
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            raise AuthenticationError("Token has expired")
        except JWTError as e:
            logger.warning(f"Token verification failed: {e}")
            raise AuthenticationError("Invalid token")

    def create_refresh_token(self, data: Dict[str, Any]) -> str:
        """
        Create a refresh token with longer expiration.
        
        Args:
            data: Data to encode in token
            
        Returns:
            Encoded refresh token
        """
        expires_delta = timedelta(days=30)  # Refresh tokens last 30 days
        return self.create_access_token(data, expires_delta)


class PermissionChecker:
    """Handles authorization and permission checking."""

    @staticmethod
    def check_user_permission(user, permission: str, resource: Optional[str] = None) -> bool:
        """
        Check if user has a specific permission.
        
        Args:
            user: User object
            permission: Permission to check
            resource: Optional resource identifier
            
        Returns:
            True if user has permission, False otherwise
        """
        # Superuser has all permissions
        if hasattr(user, 'is_superuser') and user.is_superuser:
            return True

        # Check if user is active
        if hasattr(user, 'is_active') and not user.is_active:
            return False

        # TODO: Implement role-based permission checking
        # This would integrate with a proper RBAC system

        # For now, basic permission mapping
        basic_permissions = {
            'read_own_data': True,  # All users can read their own data
            'write_own_data': True,  # All users can write their own data
        }

        return basic_permissions.get(permission, False)

    @staticmethod
    def require_permission(user, permission: str, resource: Optional[str] = None):
        """
        Require a specific permission, raise exception if not authorized.
        
        Args:
            user: User object
            permission: Required permission
            resource: Optional resource identifier
            
        Raises:
            AuthorizationError: If user lacks permission
        """
        if not PermissionChecker.check_user_permission(user, permission, resource):
            raise AuthorizationError(action=permission, resource=resource)

    @staticmethod
    def check_resource_ownership(user, resource_owner_id: str) -> bool:
        """
        Check if user owns a specific resource.
        
        Args:
            user: User object
            resource_owner_id: ID of resource owner
            
        Returns:
            True if user owns resource or is superuser
        """
        if hasattr(user, 'is_superuser') and user.is_superuser:
            return True

        return str(user.id) == str(resource_owner_id)


class SecurityUtils:
    """Additional security utilities."""

    @staticmethod
    def generate_secure_token(length: int = 32) -> str:
        """
        Generate a secure random token.
        
        Args:
            length: Length of token in bytes
            
        Returns:
            Secure random token as hex string
        """
        import secrets
        return secrets.token_hex(length)

    @staticmethod
    def hash_sensitive_data(data: str) -> str:
        """
        Hash sensitive data (not passwords).
        
        Args:
            data: Data to hash
            
        Returns:
            SHA-256 hash of data
        """
        import hashlib
        return hashlib.sha256(data.encode()).hexdigest()

    @staticmethod
    def validate_email(email: str) -> bool:
        """
        Basic email validation.
        
        Args:
            email: Email to validate
            
        Returns:
            True if email format is valid
        """
        import re
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None


# Global instances
password_manager = PasswordManager()
token_manager = TokenManager()
permission_checker = PermissionChecker()
security_utils = SecurityUtils()
