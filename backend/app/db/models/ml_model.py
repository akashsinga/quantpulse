from sqlalchemy import Column, String, Text, Boolean, DateTime, ForeignKey, UUID
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import uuid

from app.db.session import Base


class MLModel(Base):
    __tablename__ = "ml_models"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    model_type = Column(String(50), nullable=False)  # classifier, regressor
    algorithm = Column(String(50), nullable=False)  # random_forest, lightgbm, etc.
    features = Column(JSONB, nullable=False)  # list of features used
    hyperparameters = Column(JSONB, nullable=False)  # model hyperparameters
    metrics = Column(JSONB)  # performance metrics
    is_active = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    user = relationship("User", back_populates="ml_models")
    predictions = relationship("MLPrediction", back_populates="model")
