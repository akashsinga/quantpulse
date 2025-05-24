from sqlalchemy import Column, String, Date, ForeignKey, UUID, Numeric, UniqueConstraint, Integer, Index, Boolean
from sqlalchemy.orm import relationship
from enum import Enum as PythonEnum
import uuid

from app.db.session import Base


class SettlementType(str, PythonEnum):
    CASH = "cash"
    PHYSICAL = "physical"


class ExpiryMonth(str, PythonEnum):
    JAN = "JAN"
    FEB = "FEB"
    MAR = "MAR"
    APR = "APR"
    MAY = "MAY"
    JUN = "JUN"
    JUL = "JUL"
    AUG = "AUG"
    SEPT = "SEPT"
    OCT = "OCT"
    NOV = "NOV"
    DEC = "DEC"


class Future(Base):
    __tablename__ = "futures"
    __table_args__ = (
        # FIXED: Make the unique constraint more specific to prevent the duplicate key violations
        # Include security_id in the unique constraint to allow multiple contracts with same underlying/expiry but different security details
        UniqueConstraint("security_id", name="uq_future_security_id"),
        UniqueConstraint("underlying_id", "contract_month", "expiration_date", "settlement_type", name="uq_future_contract_details"),
        Index("idx_futures_expiration", "expiration_date"),
        Index("idx_future_underlying", "underlying_id"),
        Index("idx_future_active", "is_active"),
        Index("idx_future_contract_month", "contract_month"))

    security_id = Column(UUID(as_uuid=True), ForeignKey("securities.id", ondelete="CASCADE"), primary_key=True)
    underlying_id = Column(UUID(as_uuid=True), ForeignKey("securities.id"), nullable=False)
    expiration_date = Column(Date, nullable=False)
    contract_size = Column(Numeric(18, 6), nullable=False)  # Value per contract.
    lot_size = Column(Integer, nullable=False)  # Number of shares/units per lot
    settlement_type = Column(String(20), nullable=False)  # 'CASH' or 'PHYSICAL'
    contract_month = Column(String(10), nullable=False)  # e.g., 'JAN', 'FEB', 'MAR', etc.
    initial_margin = Column(Numeric(18, 6))
    maintenance_margin = Column(Numeric(18, 6))

    is_active = Column(Boolean, default=True)

    previous_contract_id = Column(UUID(as_uuid=True), ForeignKey("futures.security_id"), nullable=True)

    # Relationships with proper overlaps parameters to fix SQLAlchemy warnings
    security = relationship("Security", foreign_keys=[security_id], back_populates="futures", overlaps="derivative_underlyings,derivatives")
    underlying = relationship("Security", foreign_keys=[underlying_id], overlaps="derivative_underlyings,derivatives")
    previous_contract = relationship("Future", remote_side=[security_id], foreign_keys=[previous_contract_id], uselist=False, post_update=True)

    def __repr__(self):
        return f"<Future(security_id={self.security_id}, underlying_id={self.underlying_id}, expiry={self.expiration_date}, month={self.contract_month})>"
