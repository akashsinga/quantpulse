from sqlalchemy import Column, String, Text, Numeric, DateTime, ForeignKey, UUID
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import uuid

from app.db.session import Base

class Position(Base):
    __tablename__ = "positions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    portfolio_id = Column(UUID(as_uuid=True), ForeignKey("portfolios.id"), nullable=False)
    security_id = Column(UUID(as_uuid=True), ForeignKey("securities.id"), nullable=False)
    signal_id = Column(UUID(as_uuid=True), ForeignKey("signals.id"))  # optional
    entry_time = Column(DateTime(timezone=True), nullable=False)
    entry_price = Column(Numeric(18, 6), nullable=False)
    quantity = Column(Numeric(18, 6), nullable=False)
    direction = Column(String(10), nullable=False)  # long, short
    exit_time = Column(DateTime(timezone=True))
    exit_price = Column(Numeric(18, 6))
    profit_loss = Column(Numeric(18, 6))
    profit_loss_pct = Column(Numeric(10, 6))
    status = Column(String(20), nullable=False)  # open, closed
    notes = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    portfolio = relationship("Portfolio", back_populates="positions")
    security = relationship("Security", back_populates="positions")
    signal = relationship("Signal", back_populates="positions")