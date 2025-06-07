# backend/app/db/models/market_holiday.py

from sqlalchemy import Column, String, Text, Boolean, DateTime, Date, ForeignKey, UUID, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import uuid

from app.db.session import Base


class MarketHoliday(Base):
    __tablename__ = "market_holidays"
    __table_args__ = (UniqueConstraint("holiday_date", "exchange_id", "holiday_name", name="uq_holiday_date_exchange_name"), )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    holiday_date = Column(Date, nullable=False, index=True)
    exchange_id = Column(UUID(as_uuid=True), ForeignKey("exchanges.id"), nullable=False, index=True)
    holiday_name = Column(String(255), nullable=False)
    holiday_type = Column(String(20), default="full")  # full, partial, settlement
    description = Column(Text)
    is_recurring = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    exchange = relationship("Exchange", back_populates="holidays")

    def __repr__(self):
        return f"<MarketHoliday({self.holiday_date}, {self.holiday_name}, {self.exchange.code if self.exchange else 'No Exchange'})>"
