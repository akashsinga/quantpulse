# backend/app/models/derivatives.py
"""
Derivatives models for Quantpulse Application.
"""

from sqlalchemy import Column, String, Date, ForeignKey, Numeric, UniqueConstraint, Integer, Index, Boolean
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship, validates
from datetime import date

from app.models.base import BaseModel
from app.utils.enum import SettlementType, ExpiryMonth
from app.core.exceptions import ValidationError


class Future(BaseModel):
    """Futures contract model"""
    __tablename__ = "futures"
    __table_args__ = (
        UniqueConstraint("security_id", name="uq_future_security_id"),
        UniqueConstraint("underlying_id", "contract_month", "expiration_date", "settlement_type", name="uq_future_contract_details"),
        Index("idx_futures_expiration", "expiration_date"),
        Index("idx_future_underlying", "underlying_id"),
        Index("idx_future_active", "is_active"),
        Index("idx_future_contract_month", "contract_month"),
        Index("idx_future_settlement", "settlement_type"),
    )

    # Linked to securities table
    security_id = Column(UUID(as_uuid=True), ForeignKey("securities.id", ondelete="CASCADE"), primary_key=True)
    underlying_id = Column(UUID(as_uuid=True), ForeignKey("securities.id"), nullable=False, index=True)

    # Contract specifications
    expiration_date = Column(Date, nullable=False, index=True)
    contract_size = Column(Numeric(18, 6), nullable=False, default=1.0)
    settlement_type = Column(String(20), nullable=False, default=SettlementType.CASH.value)
    contract_month = Column(String(10), nullable=False, index=True)

    # Status and metadata
    is_active = Column(Boolean, default=True, nullable=False, index=True)
    is_tradeable = Column(Boolean, default=True, nullable=False)

    # Rollover information
    previous_contract_id = Column(UUID(as_uuid=True), ForeignKey("futures.security_id"), nullable=True)
    next_contract_id = Column(UUID(as_uuid=True), nullable=True)

    # Relationships
    security = relationship("Security", foreign_keys=[security_id], back_populates="futures")
    underlying = relationship("Security", foreign_keys=[underlying_id], back_populates="underlying_futures")
    previous_contract = relationship("Future", remote_side=[security_id], foreign_keys=[previous_contract_id], uselist=False, post_update=True)

    @validates('settlement_type')
    def validate_settlement_type(self, key, settlement_type):
        """Validate settlement type."""
        if settlement_type not in [t.value for t in SettlementType]:
            raise ValidationError(f"Invalid settlement type: {settlement_type}")
        return settlement_type

    @validates('contract_month')
    def validate_contract_month(self, key, contract_month):
        """Validate contract month."""
        if contract_month not in [m.value for m in ExpiryMonth]:
            raise ValidationError(f"Invalid contract month: {contract_month}")
        return contract_month

    @validates('expiration_date')
    def validate_expiration_date(self, key, expiration_date):
        """Validate expiration date is in the future."""
        if expiration_date and expiration_date < date.today():
            raise ValidationError("Expiration date cannot be in the past")
        return expiration_date

    def __repr__(self):
        return f"<Future(security_id={self.security_id}, underlying_id={self.underlying_id}, expiry={self.expiration_date})>"

    @property
    def is_expired(self) -> bool:
        """Check if contract has expired."""
        return self.expiration_date < date.today()

    @property
    def days_to_expiry(self) -> int:
        """Get number of days until expiry."""
        if self.expiration_date:
            return (self.expiration_date - date.today()).days
        return 0

    @property
    def is_near_expiry(self, threshold_days: int = 7) -> bool:
        """Check if contract is near expiry."""
        return 0 <= self.days_to_expiry <= threshold_days

    @property
    def contract_name(self) -> str:
        """Get formatted contract name."""
        if self.underlying and hasattr(self.underlying, 'symbol'):
            return f"{self.underlying.symbol} {self.contract_month} {self.expiration_date.year}"
        return f"FUT {self.contract_month} {self.expiration_date.year}"

    def to_dict(self, include_relationships: bool = False) -> dict:
        """
        Convert future to dictionary format.
        
        Args:
            include_relationships: Whether to include relationship data
            
        Returns:
            Dictionary representation
        """
        data = {'security_id': str(self.security_id), 'underlying_id': str(self.underlying_id), 'expiration_date': self.expiration_date.isoformat(), 'settlement_type': self.settlement_type, 'contract_month': self.contract_month, 'is_active': self.is_active, 'is_tradeable': self.is_tradeable, 'is_expired': self.is_expired, 'days_to_expiry': self.days_to_expiry, 'is_near_expiry': self.is_near_expiry, 'created_at': self.created_at.isoformat(), 'updated_at': self.updated_at.isoformat()}

        if include_relationships:
            if self.underlying:
                data['underlying'] = {'id': str(self.underlying.id), 'symbol': self.underlying.symbol, 'name': self.underlying.name, 'security_type': self.underlying.security_type}

            if self.security:
                data['security'] = {'id': str(self.security.id), 'symbol': self.security.symbol, 'name': self.security.name}

        return data
