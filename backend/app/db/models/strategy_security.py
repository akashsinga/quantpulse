from sqlalchemy import Column, Boolean, DateTime, ForeignKey, UUID, PrimaryKeyConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import uuid

from app.db.session import Base

class StrategySecurity(Base):
    __tablename__ = "strategy_securities"
    __table_args__ = (PrimaryKeyConstraint('strategy_id', 'security_id'),)

    strategy_id = Column(UUID(as_uuid=True), ForeignKey("strategies.id"), nullable=False)
    security_id = Column(UUID(as_uuid=True), ForeignKey("securities.id"), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    strategy = relationship("Strategy", back_populates="securities")
    security = relationship("Security", back_populates="strategy_securities")