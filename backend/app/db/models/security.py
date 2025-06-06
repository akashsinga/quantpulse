# backend/app/db/models/security.py

from sqlalchemy import Column, String, Boolean, DateTime, Date, ForeignKey, UUID, UniqueConstraint, Integer
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
    __table_args__ = (UniqueConstraint("symbol", "exchange_id", name="uq_symbol_exchange"), )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol = Column(String(100), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    exchange_id = Column(UUID(as_uuid=True), ForeignKey("exchanges.id"), nullable=False)
    security_type = Column(String(20), nullable=False)  # STOCK, INDEX, DERIVATIVE
    segment = Column(String(20), nullable=False)  # EQUITY, CURRENCY, COMMODITY
    external_id = Column(Integer, nullable=False, unique=True, index=True)
    sector = Column(String(100))
    industry = Column(String(100))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    exchange = relationship("Exchange", back_populates="securities")

    # Strategy and trading relationships
    strategy_securities = relationship("StrategySecurity", back_populates="security")
    signals = relationship("Signal", back_populates="security")
    backtest_trades = relationship("BacktestTrade", back_populates="security")
    ml_predictions = relationship("MLPrediction", back_populates="security")
    positions = relationship("Position", back_populates="security")

    # Unified pipeline relationships
    ohlcv_data = relationship("OHLCVUnified", back_populates="security")
    data_continuity = relationship("DataContinuity", back_populates="security")
    pipeline_jobs = relationship("PipelineJob", back_populates="security")
    quality_metrics = relationship("DataQualityMetric", back_populates="security")

    # Derivatives relationship
    futures = relationship("Future", back_populates="security", uselist=False, foreign_keys="Future.security_id")
