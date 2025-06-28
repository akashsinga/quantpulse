# backend/app/models/tasks.py

from sqlalchemy import Column, String, Integer, DateTime, Text, Boolean, JSON, Enum as SQLEnum, ForeignKey, Index
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
from enum import Enum
import uuid

from app.models.base import BaseModel, TimestampMixin
from app.utils.enum import TaskStatus, TaskType


class TaskRun(BaseModel):
    """
    Main task execution tracking model.
    Stores high level task information and status.
    """
    __tablename__ = "task_runs"
    __table_args__ = (
        Index("idx_task_runs_celery_id", "celery_task_id"),
        Index("idx_task_runs_status", "status"),
        Index("idx_task_runs_task_type", "task_type"),
        Index("idx_task_runs_user_id", "user_id"),
        Index("idx_task_runs_created_at", "created_at"),
    )

    # Task identification
    celery_task_id = Column(String(255), unique=True, nullable=False, index=True)
    task_name = Column(String(255), nullable=False)
    task_type = Column(SQLEnum(TaskType), nullable=False, index=True)

    # Task metadata
    title = Column(String(500), nullable=False)
    description = Column(Text, nullable=True)

    # Execution tracking
    status = Column(SQLEnum(TaskStatus), default=TaskStatus.PENDING, nullable=False, index=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    # Progress tracking
    current_step = Column(Integer, default=0, nullable=False)
    total_steps = Column(Integer, default=0, nullable=False)
    progress_percentage = Column(Integer, default=0, nullable=False)
    current_message = Column(Text, nullable=True)

    # Results and errors
    result_data = Column(JSON, nullable=True)
    error_message = Column(Text, nullable=True)
    error_traceback = Column(Text, nullable=True)

    # User context
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True, index=True)

    # Input parameters
    input_parameters = Column(JSON, nullable=True)

    # Performance metrics
    execution_time_seconds = Column(Integer, nullable=True)
    retry_count = Column(Integer, default=0, nullable=False)

    # Relationships
    user = relationship("User", back_populates="task_runs")
    logs = relationship("TaskLog", back_populates="task_run", cascade="all, delete-orphan", lazy="dynamic")
    steps = relationship("TaskStep", back_populates="task_run", cascade="all, delete-orphan", lazy="dynamic")

    def update_progress(self, current: int, total: int, message: str = None) -> None:
        """Update task progress"""
        self.current_step = current
        self.total_steps = total
        self.progress_percentage = int((current / total) * 100) if total > 0 else 0
        if message:
            self.current_message = message

    def to_dict(self) -> dict:
        """Convert to dictionary representation"""
        return {
            'id': str(self.id),
            'celery_task_id': self.celery_task_id,
            'task_name': self.task_name,
            'task_type': self.task_type.value,
            'title': self.title,
            'status': self.status.value,
            'progress_percentage': self.progress_percentage,
            'current_message': self.current_message,
            'created_at': self.created_at.isoformat(),
        }


class TaskStep(BaseModel):
    """Individual step tracking within a task"""
    __tablename__ = "task_steps"

    task_run_id = Column(UUID(as_uuid=True), ForeignKey("task_runs.id", ondelete="CASCADE"), nullable=False)
    step_name = Column(String(255), nullable=False)
    step_order = Column(Integer, nullable=False)
    title = Column(String(500), nullable=False)
    status = Column(SQLEnum(TaskStatus), default=TaskStatus.PENDING, nullable=False)
    result_data = Column(JSON, nullable=True)

    task_run = relationship("TaskRun", back_populates="steps")


class TaskLog(BaseModel):
    """Detailed logging for task execution"""
    __tablename__ = "task_logs"

    task_run_id = Column(UUID(as_uuid=True), ForeignKey("task_runs.id", ondelete="CASCADE"), nullable=False)
    level = Column(String(20), nullable=False)
    message = Column(Text, nullable=False)
    extra_data = Column(JSON, nullable=True)

    task_run = relationship("TaskRun", back_populates="logs")
