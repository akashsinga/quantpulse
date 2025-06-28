# backend/app/models/securities.py
"""
Securities domain models for Quantpulse application.
"""

from sqlalchemy import Column, String, Boolean, Integer, UniqueConstraint, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID

from app.models.base import BaseModel
from app.utils.enum import SecurityType, SettlementType, SecuritySegment


class Exchange(BaseModel):
    """
    Exchange model representing stock exchanges.
    """
    __tablename__ = "exchanges"
    # Basic information
    name = Column(String(100), nullable=False)
    code = Column(String(20), unique=True, nullable=False, index=True)
    country = Column(String(100), nullable=True)
    timezone = Column(String(50), nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)

    # Exchange details
    currency = Column(String(3), default="INR", nullable=False)
    trading_hours_start = Column(String(10), nullable=True)  # e.g., "09:15"
    trading_hours_end = Column(String(10), nullable=True)  # e.g., "15:30"

    # Relationships - using string references to avoid circular imports
    securities = relationship("Security", back_populates="exchange", lazy="dynamic")

    def __repr__(self):
        return f"<Exchange(code={self.code}, name={self.name}, active={self.is_active})>"

    @property
    def display_name(self) -> str:
        """Get display name for exchange."""
        return f"{self.name} ({self.code})"

    def is_trading_hours(self, time_str: str) -> bool:
        """
        Check if given time is within trading hours.
        
        Args:
            time_str: Time in HH:MM format
            
        Returns:
            True if within trading hours
        """
        if not self.trading_hours_start or not self.trading_hours_end:
            return True  # Assume always trading if hours not set

        try:
            from datetime import datetime
            time_obj = datetime.strptime(time_str, "%H:%M").time()
            start_time = datetime.strptime(self.trading_hours_start, "%H:%M").time()
            end_time = datetime.strptime(self.trading_hours_end, "%H:%M").time()

            return start_time <= time_obj <= end_time
        except ValueError:
            return True  # Default to True if parsing fails

    def to_dict(self) -> dict:
        """Convert exchange to dictionary format."""
        return {'id': str(self.id), 'name': self.name, 'code': self.code, 'country': self.country, 'timezone': self.timezone, 'currency': self.currency, 'trading_hours_start': self.trading_hours_start, 'trading_hours_end': self.trading_hours_end, 'is_active': self.is_active, 'display_name': self.display_name, 'created_at': self.created_at.isoformat(), 'updated_at': self.updated_at.isoformat()}


class Security(BaseModel):
    """
    Security model representing tradeable financial instruments.
    """
    __tablename__ = "securities"
    __table_args__ = (UniqueConstraint("symbol", "exchange_id", name="uq_symbol_exchange"), )

    # Basic information
    symbol = Column(String(100), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    isin = Column(String(12), nullable=True, index=True)

    # Exchange relationship
    exchange_id = Column(UUID(as_uuid=True), ForeignKey("exchanges.id"), nullable=False)

    # Classification
    security_type = Column(String(20), nullable=False, index=True)  # STOCK, INDEX, DERIVATIVE
    segment = Column(String(20), nullable=False, index=True)  # EQUITY, FNO, etc.
    sector = Column(String(100), nullable=True, index=True)
    industry = Column(String(100), nullable=True)

    # External identifiers
    external_id = Column(Integer, nullable=False, unique=True, index=True)  # Dhan API ID

    # Trading information
    lot_size = Column(Integer, default=1, nullable=False)
    tick_size = Column(String(20), default="0.01", nullable=False)

    # Status and flags
    is_active = Column(Boolean, default=True, nullable=False, index=True)
    is_tradeable = Column(Boolean, default=True, nullable=False)
    is_derivatives_eligible = Column(Boolean, default=False, nullable=False)

    # Market data flags
    has_options = Column(Boolean, default=False, nullable=False)
    has_futures = Column(Boolean, default=False, nullable=False)

    # Relationships
    exchange = relationship("Exchange", back_populates="securities")

    # Derivatives relationship
    futures = relationship("Future", back_populates="security", uselist=False, foreign_keys="Future.security_id")
    underlying_futures = relationship("Future", back_populates="underlying", foreign_keys="Future.underlying_id", lazy="dynamic")

    def __repr__(self):
        return f"<Security(symbol={self.symbol}, type={self.security_type}, exchange={self.exchange.code if self.exchange else 'N/A'})>"

    @property
    def full_symbol(self) -> str:
        """Get full symbol with exchange code."""
        exchange_code = self.exchange.code if self.exchange else "UNK"
        return f"{self.symbol}:{exchange_code}"

    @property
    def display_name(self) -> str:
        """Get display name for security."""
        return f"{self.symbol} - {self.name}"

    @property
    def is_stock(self) -> bool:
        """Check if security is a stock."""
        return self.security_type == SecurityType.STOCK.value

    @property
    def is_index(self) -> bool:
        """Check if security is an index."""
        return self.security_type == SecurityType.INDEX.value

    @property
    def is_derivative(self) -> bool:
        """Check if security is a derivative."""
        return self.security_type == SecurityType.DERIVATIVE.value

    def to_dict(self, include_relationships: bool = False) -> dict:
        """
        Convert security to dictionary format.
        
        Args:
            include_relationships: Whether to include relationship data
            
        Returns:
            Dictionary representation
        """
        data = {
            'id': str(self.id),
            'symbol': self.symbol,
            'name': self.name,
            'isin': self.isin,
            'full_symbol': self.full_symbol,
            'display_name': self.display_name,
            'security_type': self.security_type,
            'segment': self.segment,
            'sector': self.sector,
            'industry': self.industry,
            'external_id': self.external_id,
            'lot_size': self.lot_size,
            'tick_size': self.tick_size,
            'is_active': self.is_active,
            'is_tradeable': self.is_tradeable,
            'is_derivatives_eligible': self.is_derivatives_eligible,
            'has_options': self.has_options,
            'has_futures': self.has_futures,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }

        if include_relationships and self.exchange:
            data['exchange'] = {'id': str(self.exchange.id), 'code': self.exchange.code, 'name': self.exchange.name, 'country': self.exchange.country}

        return data
