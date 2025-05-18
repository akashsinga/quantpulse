from sqlalchemy import Column, String, Text, Boolean, DateTime, ForeignKey, UUID
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import uuid

from app.db.session import Base

class Strategy(Base):
    __tablename__ = "strategies"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    timeframe = Column(String(10), nullable=False)  # daily, weekly, monthly
    parameters = Column(JSONB, nullable=False)
    is_active = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    user = relationship("User", back_populates="strategies")
    securities = relationship("StrategySecurity", back_populates="strategy")
    signals = relationship("Signal", back_populates="strategy")
    backtest_runs = relationship("BacktestRun", back_populates="strategy")