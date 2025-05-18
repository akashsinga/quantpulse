from sqlalchemy import Column, String, Text, Numeric, DateTime, ForeignKey, UUID
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import uuid

from app.db.session import Base

class Signal(Base):
    __tablename__ = "signals"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    strategy_id = Column(UUID(as_uuid=True), ForeignKey("strategies.id"), nullable=False)
    security_id = Column(UUID(as_uuid=True), ForeignKey("securities.id"), nullable=False)
    signal_time = Column(DateTime(timezone=True), nullable=False, index=True)
    generation_time = Column(DateTime(timezone=True), server_default=func.now())
    signal_type = Column(String(20), nullable=False)  # buy, sell, exit
    direction = Column(String(10), nullable=False)  # long, short
    confidence = Column(Numeric(5, 2))  # optional, for ML-enhanced signals
    parameters_used = Column(JSONB)  # strategy parameters used
    notes = Column(Text)

    # Relationships
    strategy = relationship("Strategy", back_populates="signals")
    security = relationship("Security", back_populates="signals")
    positions = relationship("Position", back_populates="signal")