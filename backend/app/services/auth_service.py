# backend/services/auth_service.py

from typing import Optional
from sqlalchemy.orm import Session

from app.models.users import User
from app.core.security import PasswordManager
from app.repositories.users import UserRepository


def authenticate_user(db: Session, email: str, password: str) -> Optional[User]:
    """Authenticate a user with email and password"""
    user = UserRepository(db).get_active_by_email(email)

    if not user:
        return None

    if not PasswordManager.verify_password(password, user.hashed_password):
        return None

    return user
