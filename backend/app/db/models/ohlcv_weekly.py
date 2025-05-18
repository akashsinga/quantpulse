from sqlalchemy import Column, Numeric, BigInteger, DateTime, ForeignKey, UUID, PrimaryKeyConstraint
from sqlalchemy.orm import relationship
import uuid

from app.db.session import Base


class OHLCVWeekly(Base):
    __tablename__ = "ohlcv_weekly"
    __table_args__ = (PrimaryKeyConstraint("time", "security_id"),)

    time = Column(DateTime(timezone=True), nullable=False, index=True)
    security_id = Column(UUID(as_uuid=True), ForeignKey("securities.id"), nullable=False, index=True)
    open = Column(Numeric(18, 6), nullable=False)
    high = Column(Numeric(18, 6), nullable=False)
    low = Column(Numeric(18, 6), nullable=False)
    close = Column(Numeric(18, 6), nullable=False)
    volume = Column(BigInteger, nullable=False)
    adjusted_close = Column(Numeric(18, 6))

    # Relationships
    security = relationship("Security", back_populates="weekly_data")

    # Note: This table would be converted to a TimescaleDB hypertable after creation
