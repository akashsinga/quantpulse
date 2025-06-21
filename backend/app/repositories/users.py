# backend/app/repositories/users.py
"""
Data access layer for user-related operations.
Handles all database interactions for User models
"""

from typing import Optional, List, Dict, Any
from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy import or_, func

from app.repositories.base import BaseRepository
from app.models.users import User, UserPreferences
from app.core.exceptions import NotFoundError, ValidationError
from app.utils.logger import get_logger

logger = get_logger(__name__)


class UserRepository(BaseRepository[User]):
    """Repository for User model operations"""

    def __init__(self, db: Session):
        super().__init__(db, User)

    def get_by_email(self, email: str) -> Optional[User]:
        """Get user by email address"""
        return self.db.query(User).filter(User.email == email.lower(), User.is_deleted == False).first()

    def get_active_by_email(self, email: str) -> Optional[User]:
        """Get active user by email address"""
        return self.db.query(User).filter(User.email == email.lower(), User.is_active, User.is_deleted == False).first()

    def create_user(self, email: str, hashed_password: str, full_name: Optional[str] = None, **kwargs) -> User:
        """Create a new user"""
        existing_user = self.get_by_email(email)
        if existing_user:
            raise ValidationError("Email already registered", field_errors={"email": ["This email is already registered"]})

        user_data = {"email": email.lower(), "hashed_password": hashed_password, "full_name": full_name, **kwargs}

        user = User(**user_data)
        return self.create(user)

    def update_user(self, user_id: UUID, update_data: Dict[str, Any]) -> User:
        """
        Update user information.
        
        Args:
            user_id: User ID
            update_data: Fields to update
            
        Returns:
            Updated user object
            
        Raises:
            NotFoundError: If user not found
            ValidationError: If email already exists (when updating email)
        """
        user = self.get_by_id(user_id)
        if not user:
            raise NotFoundError("User", str(user_id))

        # Check email uniqueness if email is being updated
        if "email" in update_data:
            new_email = update_data["email"].lower()
            if new_email != user.email:
                existing_user = self.get_by_email(new_email)
                if existing_user:
                    raise ValidationError("Email already in use", field_errors={"email": ["This email is already in use"]})
                update_data["email"] = new_email

        return self.update(user, update_data)

    def deactivate_user(self, user_id: UUID) -> User:
        """Deactivate a user account."""
        return self.update_user(user_id, {"is_active": False})

    def activate_user(self, user_id: UUID) -> User:
        """Activate a user account."""
        return self.update_user(user_id, {"is_active": True})

    def change_password(self, user_id: UUID, new_hashed_password: str) -> User:
        """Change user password."""
        return self.update_user(user_id, {"hashed_password": new_hashed_password})

    def get_users_paginated(self, skip: int = 0, limit: int = 100, search: Optional[str] = None, is_active: Optional[bool] = None, is_superuser: Optional[bool] = None) -> tuple[List[User], int]:
        """
        Get paginated list of users with optional filters.
        
        Args:
            skip: Number of users to skip
            limit: Maximum number of users to return
            search: Search term for email or name
            is_active: Filter by active status
            is_superuser: Filter by superuser status
            
        Returns:
            Tuple of (users, total_count)
        """
        query = self.db.query(User).filter(User.is_deleted == False)

        # Apply filters
        if search:
            search_term = f"%{search.lower()}%"
            query = query.filter(or_(func.lower(User.email).like(search_term), func.lower(User.full_name).like(search_term)))

        if is_active is not None:
            query = query.filter(User.is_active == is_active)

        if is_superuser is not None:
            query = query.filter(User.is_superuser == is_superuser)

        # Get total count
        total = query.count()

        # Apply pagination
        users = query.offset(skip).limit(limit).all()

        return users, total

    def get_user_stats(self, user_id: UUID) -> Dict[str, Any]:
        """
        Get user statistics.
        
        Args:
            user_id: User ID
            
        Returns:
            Dictionary with user stats
        """
        user = self.get_by_id(user_id)
        if not user:
            raise NotFoundError("User", str(user_id))

        # Calculate account age
        from datetime import datetime
        account_age = (datetime.now() - user.created_at).days

        stats = {"total_strategies": 0, "active_strategies": 0, "total_backtests": 0, "total_portfolios": 0, "last_login": None, "account_age_days": account_age}

        return stats


class UserPreferencesRepository(BaseRepository[UserPreferences]):
    """Respository for UserPreferences model operations"""

    def __init__(self, db: Session):
        super().__init__(db, UserPreferences)

    def get_by_user_id(self, user_id: UUID) -> Optional[UserPreferences]:
        """Get user preferences by user ID"""
        return self.db.query(UserPreferences).filter(UserPreferences.user_id == user_id, UserPreferences.is_deleted == False).first()

    def create_or_update_preferences(self, user_id: UUID, preferences_data: Dict[str, Any]) -> UserPreferences:
        """Create or update user preferences"""
        existing = self.get_by_user_id(user_id)

        if existing:
            return self.update(existing, preferences_data)
        else:
            preferences_data["user_id"] = str(user_id)
            preferences = UserPreferences(**preferences_data)
            return self.create(preferences)
