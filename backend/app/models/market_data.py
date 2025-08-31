# backend/app/models/market_data.py
"""
Market data models for OHLCV data and technical indicators.
"""

from sqlalchemy import Column, String, Date, ForeignKey, Numeric, UniqueConstraint, Index, Boolean, DateTime, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship, validates
from datetime import date, datetime
from decimal import Decimal

from app.models.base import BaseModel
from app.utils.enum import Timeframe
from app.core.exceptions import ValidationError


class OHLCVData(BaseModel):
    """OHLCV (Open, High, Low, Close, Volume)"""
    __tablename__ = "ohlcv_data"
    __table_args__ = (
        UniqueConstraint("security_id", "date", "timeframe", name="uq_ohlcv_security_date_timeframe"),
        Index("idx_ohlcv_security", "security_id"),
        Index("idx_ohlcv_date", "date"),
        Index("idx_ohlcv_timeframe", "timeframe"),
        Index("idx_ohlcv_security_date", "security_id", "date"),
        Index("idx_ohlcv_date_range", "date", "security_id"),
    )
    # Foreign key to security
    security_id = Column(UUID(as_uuid=True), ForeignKey("securities.id", ondelete="CASCADE"), nullable=False, index=True)

    # Date and timeframe
    date = Column(Date, nullable=False, index=True)
    timeframe = Column(String(10), nullable=False, default=Timeframe.DAILY.value, index=True)

    # OHLCV data
    open_price = Column(Numeric(18, 4), nullable=False)
    high_price = Column(Numeric(18, 4), nullable=False)
    low_price = Column(Numeric(18, 4), nullable=False)
    close_price = Column(Numeric(18, 4), nullable=False)
    volume = Column(Numeric(20, 0), nullable=False, default=0)

    # Relationships
    security = relationship("Security", back_populates="ohlcv_data")
    technical_indicators = relationship("TechnicalIndicator", back_populates="ohlcv_data", cascade="all, delete-orphan")

    @validates('timeframe')
    def validate_timeframe(self, key, timeframe):
        """Validate timeframe"""
        if timeframe not in [t.value for t in Timeframe]:
            raise ValidationError(f"Invalid timeframe: {timeframe}")
        return timeframe

    @validates('open_price', 'high_price', 'low_price', 'close_price')
    def validate_prices(self, key, price):
        """Validate price values"""
        if price and price <= 0:
            raise ValidationError(f"{key} must be positive")
        return price

    @validates('volume')
    def validate_volume(self, key, volume):
        """Validate volume"""
        if volume and volume < 0:
            raise ValidationError("Volume cannot be negative")
        return volume

    @property
    def price_change(self) -> Decimal:
        """Calculate price change (Close - Open)"""
        return self.close_price - self.open_price

    @property
    def price_change_percent(self) -> Decimal:
        """Calculate price change percentage"""
        if self.open_price and self.open_price != 0:
            return ((self.close_price - self.open_price) / self.open_price) * 100
        return Decimal('0.0')

    @property
    def trading_range(self) -> Decimal:
        """Calculate trading range (High - Low)"""
        return self.high_price - self.low_price

    @property
    def is_green(self) -> bool:
        """Check if candle is green (close > open)"""
        return self.close_price > self.open_price

    @property
    def is_red(self) -> bool:
        """Check if candle is red (close < open)"""
        return self.close_price < self.open_price

    @property
    def is_doji(self) -> bool:
        """Check if candle is doji (close == open)"""
        return abs(self.close_price - self.open_price) <= 0.01

    def __repr__(self):
        return f"<OHLCVData(security_id={self.security_id}, date={self.date}, close={self.close_price})>"

    def to_dict(self, include_relationships: bool = False) -> dict:
        """Convert to dictionary format"""
        data = {
            'id': str(self.id),
            'security_id': str(self.security_id),
            'date': self.date.isoformat(),
            'timeframe': self.timeframe,
            'open': float(self.open_price),
            'high': float(self.high_price),
            'low': float(self.low_price),
            'close': float(self.close_price),
            'volume': int(self.volume) if self.volume else 0,
            'price_change': float(self.price_change),
            'price_change_percent': float(self.price_change_percent),
            'trading_range': float(self.trading_range),
            'is_green': self.is_green,
            'is_red': self.is_red,
            'is_doji': self.is_doji,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }

        if include_relationships and self.security:
            data['security'] = {'symbol': self.security.symbol, 'name': self.security.name, 'exchange_code': self.security.exchange.code if self.security.exchange else None}

        return data


class TechnicalIndicator(BaseModel):
    """Technical Indicators calculated from OHLCV data"""
    __tablename__ = "technical_indicators"
    __table_args__ = (UniqueConstraint("ohlcv_data_id", "indicator_name", name="uq_indicator_ohlcv_name"), Index("idx_indicators_ohlcv", "ohlcv_data_id"), Index("idx_indicators_name", "indicator_name"))

    # Foreign key to OHLCV data
    ohlcv_data_id = Column(UUID(as_uuid=True), ForeignKey("ohlcv_data.id", ondelete="CASCADE"), nullable=False)

    # Indicator details
    indicator_name = Column(String(50), nullable=False, index=True)  # SMA_20, RSI_14, etc.
    indicator_value = Column(Numeric(18, 6), nullable=False)

    # Additional metadata
    calculation_params = Column(String(100), nullable=True)  # JSON string of parameters
    calculation_timestamp = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    ohlcv_data = relationship("OHLCVData", back_populates="technical_indicators")

    def __repr__(self):
        return f"<TechnicalIndicator(ohlcv_data_id={self.ohlcv_data_id}, name={self.indicator_name}, value={self.indicator_value})>"


class MarketDataImportLog(BaseModel):
    """Log of market data import operations"""
    __tablename__ = "market_data_import_logs"
    __table_args__ = (
        Index("idx_import_logs_date", "import_date"),
        Index("idx_import_logs_security", "security_id"),
        Index("idx_import_logs_status", "status"),
    )

    # Import details
    security_id = Column(UUID(as_uuid=True), ForeignKey("securities.id"), nullable=True)  # Null for bulk imports
    import_date = Column(Date, nullable=False, index=True)
    date_from = Column(Date, nullable=False)
    date_to = Column(Date, nullable=False)

    # Import results
    total_records_processed = Column(Integer, default=0, nullable=False)
    records_created = Column(Integer, default=0, nullable=False)
    records_updated = Column(Integer, default=0, nullable=False)
    records_skipped = Column(Integer, default=0, nullable=False)
    records_failed = Column(Integer, default=0, nullable=False)

    # Status and metadata
    status = Column(String(20), nullable=False)  # SUCCESS, FAILURE, PARTIAL
    data_source = Column(String(50), nullable=False, default="DHAN")
    import_type = Column(String(20), nullable=False)  # FULL, INCREMENTAL, BACKFILL
    error_message = Column(String(1000), nullable=True)

    # Performance metrics
    execution_time_seconds = Column(Integer, nullable=True)
    api_calls_made = Column(Integer, nullable=True)

    # Relationships
    security = relationship("Security")

    def __repr__(self):
        return f"<MarketDataImportLog(security_id={self.security_id}, date={self.import_date}, status={self.status})>"
