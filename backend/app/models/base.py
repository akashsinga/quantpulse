# backend/app/models/base.py
"""
Base model class with common fields and patterns fro all Quantpulse models
"""

import uuid
from datetime import datetime
from sqlalchemy import Column, DateTime, UUID, Boolean
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

# Create the base class
Base = declarative_base()


class BaseModel(Base):
    """
    Abstract base model with common fields for all entities
    Provides:
    - UUID primary key
    - Created/Updated timestamps
    - Soft delete capability
    """
    __abstract__ = True

    # Primary key - UUID
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Timestamps - automatically managed
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # Soft delete
    is_deleted = Column(Boolean, default=False, nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)

    def soft_delete(self):
        """Mark record as deleted without removing from database"""
        self.is_deleted = True
        self.deleted_at = datetime.now()

    def restore(self):
        """Restore a soft-deleted record"""
        self.is_deleted = False
        self.deleted_at = None

    def __repr__(self):
        """Default string reperesentation"""
        return f"<{self.__class__.__name__}(id={self.id})>"


class TimestampMixin:
    """
    Mixin for models that only need timestamps(no UUID primary key).
    Useful for junction tables or models with composite keys
    """
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class SoftDeleteMixin:
    """
    Mixin for soft delete functionality.
    Can be added to models that inherit from TimestampMixin.
    """
    is_deleted = Column(Boolean, default=False, nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)

    def soft_delete(self):
        """Mark record as deleted without removing from database."""
        self.is_deleted = True
        self.deleted_at = datetime.now()

    def restore(self):
        """Restore a soft-deleted record."""
        self.is_deleted = False
        self.deleted_at = None
