# backend/services/auth_service.py

from typing import Optional
from sqlalchemy.orm import Session

from app.models.users import User
from app.core.security import PasswordManager
from app.repositories.users import UserRepository


class AuthService:

    def __init__(self, db: Session):
        self.db = db

    def authenticate_user(self, email: str, password: str) -> Optional[User]:
        """Authenticate a user with email and password"""
        user = UserRepository(self.db).get_active_by_email(email)

        if not user:
            return None

        if not PasswordManager.verify_password(password, user.hashed_password):
            return None

        return user
