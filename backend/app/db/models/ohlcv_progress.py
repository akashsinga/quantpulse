# app/db/models/ohlcv_progress.py

from sqlalchemy import Column, String, Date, DateTime, Integer, UUID, Text
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import uuid

from app.db.session import Base


class OHLCVProgress(Base):
    __tablename__ = "ohlcv_fetch_progress"

    security_id = Column(UUID(as_uuid=True), primary_key=True)
    last_historical_fetch = Column(Date, nullable=True)
    last_daily_fetch = Column(Date, nullable=True)
    status = Column(String(20), nullable=False, default='pending')  # pending, in_progress, success, failed
    retry_count = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<OHLCVProgress(security_id={self.security_id}, status={self.status})>"
