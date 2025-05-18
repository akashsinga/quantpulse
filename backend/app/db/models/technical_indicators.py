from sqlalchemy import Column, String, Numeric, DateTime, ForeignKey, UUID, PrimaryKeyConstraint
from sqlalchemy.orm import relationship
import uuid

from app.db.session import Base


class TechnicalIndicator(Base):
    __tablename__ = "technical_indicators"
    __table_args__ = (PrimaryKeyConstraint("time", "security_id", "timeframe", "indicator_name"),)

    time = Column(DateTime(timezone=True), nullable=False, index=True)
    security_id = Column(UUID(as_uuid=True), ForeignKey("securities.id"), nullable=False, index=True)
    timeframe = Column(String(10), nullable=False)  # daily, weekly, monthly
    indicator_name = Column(String(50), nullable=False)  # e.g., "sma_5", "rsi_14"
    value = Column(Numeric(18, 6))

    # Relationships
    security = relationship("Security", back_populates="indicators")

    # Note: This table would be converted to a TimescaleDB hypertable after creation
