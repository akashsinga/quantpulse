# backend/app/models/__init__.py

from .base import Base, BaseModel, TimestampMixin, SoftDeleteMixin
from .users import User, UserPreferences

__all__ = ["Base", "BaseModel", "TimestampMixin", "SoftDeleteMixin", "User", "UserPreferences"]
