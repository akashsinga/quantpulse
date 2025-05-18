from sqlalchemy import Column, String, Numeric, DateTime, ForeignKey, UUID
from sqlalchemy.orm import relationship
import uuid

from app.db.session import Base


class BacktestTrade(Base):
    __tablename__ = "backtest_trades"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    backtest_id = Column(UUID(as_uuid=True), ForeignKey("backtest_runs.id"), nullable=False, index=True)
    security_id = Column(UUID(as_uuid=True), ForeignKey("securities.id"), nullable=False)
    entry_time = Column(DateTime(timezone=True), nullable=False)
    entry_price = Column(Numeric(18, 6), nullable=False)
    exit_time = Column(DateTime(timezone=True))
    exit_price = Column(Numeric(18, 6))
    direction = Column(String(10), nullable=False)  # long, short
    quantity = Column(Numeric(18, 6), nullable=False)
    profit_loss = Column(Numeric(18, 6))
    profit_loss_pct = Column(Numeric(10, 6))
    exit_reason = Column(String(50))  # signal, stop_loss, take_profit, etc.

    # Relationships
    backtest = relationship("BacktestRun", back_populates="trades")
    security = relationship("Security", back_populates="backtest_trades")
