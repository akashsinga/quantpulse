from sqlalchemy import Column, String, Numeric, DateTime, ForeignKey, UUID
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import uuid

from app.db.session import Base

class MLPrediction(Base):
    __tablename__ = "ml_predictions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    model_id = Column(UUID(as_uuid=True), ForeignKey("ml_models.id"), nullable=False)
    security_id = Column(UUID(as_uuid=True), ForeignKey("securities.id"), nullable=False)
    prediction_time = Column(DateTime(timezone=True), nullable=False, index=True)
    target = Column(String(50), nullable=False)  # what's being predicted
    prediction_value = Column(Numeric(18, 6), nullable=False)
    confidence = Column(Numeric(5, 2))
    features_used = Column(JSONB)  # feature values used for prediction
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    model = relationship("MLModel", back_populates="predictions")
    security = relationship("Security", back_populates="ml_predictions")