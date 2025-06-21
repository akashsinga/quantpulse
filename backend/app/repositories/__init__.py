# backend/app/repositories/__init__.py

from .base import BaseRepository
from .users import UserRepository, UserPreferencesRepository

__all__ = ["BaseRepository", "UserRepository", "UserPreferencesRepository"]
