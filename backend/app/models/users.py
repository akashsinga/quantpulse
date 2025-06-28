# backend/app/models/users.py
"""
User domain models for QuantPulse application
"""

from sqlalchemy import Column, ForeignKey, String, Boolean, UUID
from sqlalchemy.orm import relationship
from app.models.base import BaseModel
from app.utils.enum import Timeframe


class User(BaseModel):
    """
    User model representing authenticated users in the system
    Supports both regular users and adminstrators with role-based access
    """
    __tablename__ = "users"

    # Basic user information
    email = Column(String(255), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)
    full_name = Column(String(255), nullable=True)

    # Account Status
    is_active = Column(Boolean, default=True, nullable=False)
    is_superuser = Column(Boolean, default=False, nullable=False)

    # Relationships
    preferences = relationship("UserPreferences", back_populates="user", uselist=False, foreign_keys="UserPreferences.user_id")
    task_runs = relationship("TaskRun", back_populates="user", lazy="dynamic")

    def __repr__(self):
        return f"<User(id={self.id}, email={self.email}, active={self.is_active})>"

    @property
    def display_name(self) -> str:
        """Get display name for user."""
        return self.full_name or self.email.split('@')[0]


class UserPreferences(BaseModel):
    """
    User preferences and settings
    """
    __tablename__ = "user_preferences"

    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)

    # UI Preferences
    theme = Column(String(20), default="light", nullable=False)
    language = Column(String(10), default="en", nullable=False)
    currency = Column(String(3), default="INR", nullable=False)

    # Trading Preferences
    preferred_timeline = Column(String(20), default=Timeframe.DAILY.value, nullable=False)

    # Notification Preferences
    email_notifications = Column(Boolean, default=True, nullable=False)
    signal_notifications = Column(Boolean, default=True, nullable=False)
    portfolio_alerts = Column(Boolean, default=True, nullable=False)
    system_notifications = Column(Boolean, default=True, nullable=False)

    # Relationships
    user = relationship("User", foreign_keys=[user_id], back_populates="preferences")

    def __repr__(self):
        return f"<UserPreferences(user_id={self.user_id}, theme={self.theme})>"
