from sqlalchemy import Column, String, Text, Date, DateTime, ForeignKey, UUID
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import uuid

from app.db.session import Base

class BacktestRun(Base):
    __tablename__ = "backtest_runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    strategy_id = Column(UUID(as_uuid=True), ForeignKey("strategies.id"), nullable=False)
    name = Column(String(100))
    description = Column(Text)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    parameters = Column(JSONB, nullable=False)
    status = Column(String(20), nullable=False)  # running, completed, failed
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True))

    # Relationships
    user = relationship("User", back_populates="backtest_runs")
    strategy = relationship("Strategy", back_populates="backtest_runs")
    results = relationship("BacktestResult", back_populates="backtest", uselist=False, cascade="all, delete-orphan")
    trades = relationship("BacktestTrade", back_populates="backtest", cascade="all, delete-orphan")