from sqlalchemy import Column, String, Boolean, DateTime, Date, ForeignKey, UUID, UniqueConstraint, Integer, CheckConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import uuid
from enum import Enum as PythonEnum

from app.db.session import Base


class SecurityType(str, PythonEnum):
    STOCK = "stock"
    INDEX = "index"
    FUTURE = "future"
    OPTION = "option"


class Security(Base):
    __tablename__ = "securities"
    __table_args__ = (UniqueConstraint("symbol", "exchange_id", name="uq_symbol_exchange"), CheckConstraint("(security_type IN ('STOCK', 'INDEX') AND futures_id IS NULL) OR (security_type = 'DERIVATIVE')", name="chk_security_type_consistency"))

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol = Column(String(100), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    exchange_id = Column(UUID(as_uuid=True), ForeignKey("exchanges.id"), nullable=False)
    security_type = Column(String(20), nullable=False)  # STOCK, INDEX, DERIVATIVE
    segment = Column(String(20), nullable=False)  # EQUITY, CURRENCY, COMMODITY
    external_id = Column(Integer, nullable=False, index=True)  # Dhan's Security ID
    sector = Column(String(100))
    industry = Column(String(100))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    exchange = relationship("Exchange", back_populates="securities")
    daily_data = relationship("OHLCVDaily", back_populates="security")
    weekly_data = relationship("OHLCVWeekly", back_populates="security")
    indicators = relationship("TechnicalIndicator", back_populates="security")
    strategy_securities = relationship("StrategySecurity", back_populates="security")
    signals = relationship("Signal", back_populates="security")
    backtest_trades = relationship("BacktestTrade", back_populates="security")
    ml_predictions = relationship("MLPrediction", back_populates="security")
    positions = relationship("Position", back_populates="security")

    # Fixed relationships with proper overlaps parameters
    futures = relationship("Future", back_populates="security", uselist=False, foreign_keys="Future.security_id")

    # Add overlaps parameters to fix the SQLAlchemy warnings
    derivative_underlyings = relationship("Security", secondary="futures", primaryjoin="Security.id==Future.security_id", secondaryjoin="Security.id==Future.underlying_id", backref="derivatives", overlaps="futures")
