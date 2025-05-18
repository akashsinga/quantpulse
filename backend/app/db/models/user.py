# backend/app/db/models/user.py

from sqlalchemy import Column, String, Boolean, DateTime, UUID
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import uuid

from app.db.session import Base


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)
    full_name = Column(String(255))
    is_active = Column(Boolean, default=True)
    is_superuser = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    strategies = relationship("Strategy", back_populates="user")
    backtest_runs = relationship("BacktestRun", back_populates="user")
    ml_models = relationship("MLModel", back_populates="user")
    portfolios = relationship("Portfolio", back_populates="user")
