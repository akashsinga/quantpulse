# app/models/users.py
"""
User domain models for QuantPulse application.
Fixed to properly use BaseModel with UUID types.
"""

from sqlalchemy import Column, String, Boolean, ForeignKey, UUID
from sqlalchemy.orm import relationship

from app.models.base import BaseModel


class User(BaseModel):
    """
    User model representing authenticated users in the system.
    
    Supports both regular users and administrators with role-based access.
    """
    __tablename__ = "users"

    # Basic user information
    email = Column(String(255), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)
    full_name = Column(String(255), nullable=True)

    # Account status
    is_active = Column(Boolean, default=True, nullable=False)
    is_superuser = Column(Boolean, default=False, nullable=False)
    is_verified = Column(Boolean, default=False, nullable=False)

    # Profile information
    phone_number = Column(String(20), nullable=True)
    timezone = Column(String(50), default="Asia/Kolkata", nullable=False)

    # Relationships - using string references to avoid circular imports
    strategies = relationship("Strategy", back_populates="user", lazy="dynamic")
    backtest_runs = relationship("BacktestRun", back_populates="user", lazy="dynamic")
    ml_models = relationship("MLModel", back_populates="user", lazy="dynamic")
    portfolios = relationship("Portfolio", back_populates="user", lazy="dynamic")

    def __repr__(self):
        return f"<User(id={self.id}, email={self.email}, active={self.is_active})>"

    @property
    def is_admin(self) -> bool:
        """Check if user has admin privileges."""
        return self.is_superuser

    @property
    def display_name(self) -> str:
        """Get display name for user."""
        return self.full_name or self.email.split('@')[0]

    def has_permission(self, permission: str) -> bool:
        """
        Check if user has a specific permission.
        
        Args:
            permission: Permission to check
            
        Returns:
            True if user has permission
        """
        # Superusers have all permissions
        if self.is_superuser:
            return True

        # Basic permission mapping - extend as needed
        user_permissions = {'read_own_data', 'write_own_data', 'create_strategy', 'run_backtest', 'view_portfolio'}

        return permission in user_permissions

    def can_access_resource(self, resource_user_id: str) -> bool:
        """
        Check if user can access a resource owned by another user.
        
        Args:
            resource_user_id: ID of the resource owner
            
        Returns:
            True if access is allowed
        """
        # Users can access their own resources
        if str(self.id) == str(resource_user_id):
            return True

        # Superusers can access all resources
        if self.is_superuser:
            return True

        return False

    def to_dict(self, include_sensitive: bool = False) -> dict:
        """
        Convert user to dictionary format.
        
        Args:
            include_sensitive: Whether to include sensitive fields
            
        Returns:
            Dictionary representation of user
        """
        data = {'id': str(self.id), 'email': self.email, 'full_name': self.full_name, 'is_active': self.is_active, 'is_verified': self.is_verified, 'display_name': self.display_name, 'timezone': self.timezone, 'created_at': self.created_at.isoformat(), 'updated_at': self.updated_at.isoformat()}

        if include_sensitive:
            data.update({'is_superuser': self.is_superuser, 'phone_number': self.phone_number, 'notification_preferences': self.notification_preferences})

        return data


class UserSession(BaseModel):
    """
    Model to track user sessions for security and analytics.
    """
    __tablename__ = "user_sessions"

    # Session information
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=False, index=True)
    session_token = Column(String(255), unique=True, nullable=False, index=True)
    refresh_token = Column(String(255), unique=True, nullable=True, index=True)

    # Session metadata
    ip_address = Column(String(45), nullable=True)  # IPv6 compatible
    user_agent = Column(String(500), nullable=True)
    device_info = Column(String(200), nullable=True)

    # Session status
    is_active = Column(Boolean, default=True, nullable=False)
    last_activity = Column(String(50), nullable=True)  # DateTime as ISO string
    expires_at = Column(String(50), nullable=True)  # DateTime as ISO string

    def __repr__(self):
        return f"<UserSession(id={self.id}, user_id={self.user_id}, active={self.is_active})>"

    def is_expired(self) -> bool:
        """Check if session has expired."""
        if not self.expires_at:
            return False

        from datetime import datetime
        try:
            expiry = datetime.fromisoformat(self.expires_at.replace('Z', '+00:00'))
            return datetime.now() > expiry
        except (ValueError, AttributeError):
            return True

    def revoke(self):
        """Revoke the session."""
        self.is_active = False
        from datetime import datetime
        self.updated_at = datetime.now()


class UserPreferences(BaseModel):
    """
    User preferences and settings.
    Separate table for better organization and performance.
    """
    __tablename__ = "user_preferences"

    # User reference
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id'), unique=True, nullable=False, index=True)

    # UI Preferences
    theme = Column(String(20), default="light", nullable=False)  # light, dark, auto
    language = Column(String(10), default="en", nullable=False)
    currency = Column(String(3), default="INR", nullable=False)

    # Trading Preferences
    default_position_size = Column(String(20), default="1000", nullable=False)  # Amount as string
    risk_tolerance = Column(String(20), default="medium", nullable=False)  # low, medium, high
    preferred_timeframe = Column(String(20), default="daily", nullable=False)

    # Notification Preferences
    email_notifications = Column(Boolean, default=True, nullable=False)
    signal_notifications = Column(Boolean, default=True, nullable=False)
    portfolio_alerts = Column(Boolean, default=True, nullable=False)
    system_notifications = Column(Boolean, default=True, nullable=False)

    # Privacy Settings
    profile_visibility = Column(String(20), default="private", nullable=False)  # private, public
    data_sharing = Column(Boolean, default=False, nullable=False)

    def __repr__(self):
        return f"<UserPreferences(user_id={self.user_id}, theme={self.theme})>"

    def to_dict(self) -> dict:
        """Convert preferences to dictionary."""
        return {
            'user_id': str(self.user_id),
            'theme': self.theme,
            'language': self.language,
            'currency': self.currency,
            'default_position_size': self.default_position_size,
            'risk_tolerance': self.risk_tolerance,
            'preferred_timeframe': self.preferred_timeframe,
            'email_notifications': self.email_notifications,
            'signal_notifications': self.signal_notifications,
            'portfolio_alerts': self.portfolio_alerts,
            'system_notifications': self.system_notifications,
            'profile_visibility': self.profile_visibility,
            'data_sharing': self.data_sharing,
            'updated_at': self.updated_at.isoformat()
        }
