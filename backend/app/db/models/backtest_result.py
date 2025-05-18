from sqlalchemy import Column, Numeric, Integer, ForeignKey, UUID, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
import uuid

from app.db.session import Base

class BacktestResult(Base):
    __tablename__ = "backtest_results"
    __table_args__ = (PrimaryKeyConstraint('backtest_id'),)

    backtest_id = Column(UUID(as_uuid=True), ForeignKey("backtest_runs.id"), nullable=False)
    total_return = Column(Numeric(10, 6))
    annualized_return = Column(Numeric(10, 6))
    sharpe_ratio = Column(Numeric(10, 6))
    sortino_ratio = Column(Numeric(10, 6))
    max_drawdown = Column(Numeric(10, 6))
    win_rate = Column(Numeric(5, 2))
    profit_factor = Column(Numeric(10, 6))
    total_trades = Column(Integer)
    equity_curve = Column(JSONB)  # time series of portfolio value
    metrics = Column(JSONB)  # additional metrics

    # Relationships
    backtest = relationship("BacktestRun", back_populates="results")