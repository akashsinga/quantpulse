# backend/app/db/models/unified_ohlcv.py

from sqlalchemy import Column, String, Numeric, BigInteger, DateTime, CheckConstraint, Index, Text, Integer, Boolean, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import JSONB, TSTZRANGE, UUID
from sqlalchemy.sql import func, text
from sqlalchemy.orm import validates
from datetime import datetime
import uuid
from enum import Enum as PythonEnum

from app.db.session import Base


class Timeframe(str, PythonEnum):
    """Supported timeframes for OHLCV data"""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class JobStatus(str, PythonEnum):
    """Pipeline job statuses"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobType(str, PythonEnum):
    """Pipeline job types"""
    INGEST = "ingest"
    AGGREGATE = "aggregate"
    GAP_FILL = "gap_fill"
    VALIDATE = "validate"
    CLEANUP = "cleanup"


class OHLCVUnified(Base):
    """
    Unified OHLCV table supporting multiple timeframes
    Optimized for TimescaleDB with proper constraints and indexing
    """
    __tablename__ = "ohlcv_unified"

    # Primary fields
    symbol = Column(String(50), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    timeframe = Column(String(10), nullable=False, index=True)

    # OHLCV data with high precision
    open = Column(Numeric(18, 6), nullable=False)
    high = Column(Numeric(18, 6), nullable=False)
    low = Column(Numeric(18, 6), nullable=False)
    close = Column(Numeric(18, 6), nullable=False)
    volume = Column(BigInteger, nullable=False)
    adjusted_close = Column(Numeric(18, 6), nullable=True)

    # Metadata
    source = Column(String(50), nullable=False, default="dhan_api")
    quality_score = Column(Numeric(3, 2), nullable=False, default=1.00)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Constraints
    __table_args__ = (
        # Primary key constraint (use PrimaryKeyConstraint)
        PrimaryKeyConstraint('symbol', 'timestamp', 'timeframe'),

        # OHLC validation constraints
        CheckConstraint("high >= GREATEST(open, close, low)", name="check_high_valid"),
        CheckConstraint("low <= LEAST(open, close, high)", name="check_low_valid"),
        CheckConstraint("volume >= 0", name="check_volume_positive"),
        CheckConstraint("quality_score >= 0.0 AND quality_score <= 1.0", name="check_quality_score_range"),
        CheckConstraint("open > 0 AND high > 0 AND low > 0 AND close > 0", name="check_prices_positive"),
        CheckConstraint("timeframe IN ('daily', 'weekly', 'monthly')", name="check_timeframe_valid"),

        # Performance indexes
        Index("idx_ohlcv_symbol_timestamp", "symbol", "timestamp"),
        Index("idx_ohlcv_timestamp_timeframe", "timestamp", "timeframe"),
        Index("idx_ohlcv_symbol_timeframe", "symbol", "timeframe"),
        Index("idx_ohlcv_source", "source"),
        Index("idx_ohlcv_quality", "quality_score"),
        Index("idx_ohlcv_created_at", "created_at"),

        # Partial indexes for performance - FIXED: Removed time-based conditions that require IMMUTABLE functions
        Index("idx_ohlcv_daily_recent", "symbol", "timestamp", postgresql_where=text("timeframe = 'daily'")),
        Index("idx_ohlcv_weekly_recent", "symbol", "timestamp", postgresql_where=text("timeframe = 'weekly'")),
    )

    @validates('timeframe')
    def validate_timeframe(self, key, timeframe):
        """Validate timeframe is supported"""
        if timeframe not in [t.value for t in Timeframe]:
            raise ValueError(f"Invalid timeframe: {timeframe}")
        return timeframe

    @validates('quality_score')
    def validate_quality_score(self, key, quality_score):
        """Validate quality score is in valid range"""
        if not (0.0 <= float(quality_score) <= 1.0):
            raise ValueError(f"Quality score must be between 0.0 and 1.0, got: {quality_score}")
        return quality_score

    def __repr__(self):
        return f"<OHLCVUnified({self.symbol}, {self.timestamp}, {self.timeframe}, O={self.open}, H={self.high}, L={self.low}, C={self.close}, V={self.volume})>"


class DataContinuity(Base):
    """
    Tracks data continuity and gaps for each symbol/timeframe combination
    Enables efficient gap detection and data quality monitoring
    """
    __tablename__ = "data_continuity"

    # Primary identification
    symbol = Column(String(50), nullable=False)
    timeframe = Column(String(10), nullable=False)

    # Continuity tracking
    first_data_date = Column(DateTime(timezone=True), nullable=True)
    last_update = Column(DateTime(timezone=True), nullable=False)
    expected_next = Column(DateTime(timezone=True), nullable=False)
    total_records = Column(Integer, nullable=False, default=0)

    # Gap statistics
    gap_count = Column(Integer, nullable=False, default=0)
    largest_gap_days = Column(Integer, nullable=False, default=0)
    last_gap_check = Column(DateTime(timezone=True), nullable=True)

    # Quality metrics
    data_quality_score = Column(Numeric(3, 2), nullable=False, default=1.00)
    consecutive_failures = Column(Integer, nullable=False, default=0)
    last_success = Column(DateTime(timezone=True), nullable=True)

    # Metadata and configuration
    auto_fill_enabled = Column(Boolean, nullable=False, default=True)
    priority_level = Column(Integer, nullable=False, default=5)  # 1-10 scale
    extra_data = Column(JSONB, nullable=True)  # Changed from 'metadata' to 'extra_data'

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Constraints and indexes
    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'timeframe'),
        CheckConstraint("gap_count >= 0", name="check_gap_count_positive"),
        CheckConstraint("largest_gap_days >= 0", name="check_largest_gap_positive"),
        CheckConstraint("data_quality_score >= 0.0 AND data_quality_score <= 1.0", name="check_continuity_quality_range"),
        CheckConstraint("priority_level >= 1 AND priority_level <= 10", name="check_priority_range"),
        CheckConstraint("total_records >= 0", name="check_total_records_positive"),
        CheckConstraint("consecutive_failures >= 0", name="check_failures_positive"),
        Index("idx_continuity_last_update", "last_update"),
        Index("idx_continuity_expected_next", "expected_next"),
        Index("idx_continuity_gap_count", "gap_count"),
        Index("idx_continuity_quality", "data_quality_score"),
        Index("idx_continuity_priority", "priority_level"),
        Index("idx_continuity_auto_fill", "auto_fill_enabled"),
    )

    def __repr__(self):
        return f"<DataContinuity({self.symbol}, {self.timeframe}, last={self.last_update}, gaps={self.gap_count})>"


class PipelineJob(Base):
    """
    Tracks all data pipeline jobs for monitoring and coordination
    Enables job scheduling, progress tracking, and failure recovery
    """
    __tablename__ = "pipeline_jobs"

    # Primary identification
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_type = Column(String(50), nullable=False, index=True)

    # Job scope
    symbol = Column(String(50), nullable=True, index=True)  # Null for multi-symbol jobs
    symbols = Column(JSONB, nullable=True)  # For batch jobs
    date_range = Column(TSTZRANGE, nullable=True)  # PostgreSQL timestamp range
    timeframe = Column(String(10), nullable=True)

    # Job status and progress
    status = Column(String(20), nullable=False, default=JobStatus.PENDING.value, index=True)
    progress = Column(Integer, nullable=False, default=0)  # 0-100 percentage
    priority = Column(Integer, nullable=False, default=5)  # 1-10 scale

    # Execution tracking
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    last_heartbeat = Column(DateTime(timezone=True), nullable=True)

    # Results and errors
    result_summary = Column(JSONB, nullable=True)
    error_message = Column(Text, nullable=True)
    error_category = Column(String(50), nullable=True)
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=3)

    # Configuration
    config = Column(JSONB, nullable=True)  # Job-specific configuration
    dependencies = Column(JSONB, nullable=True)  # Job dependencies

    # Additional metadata
    created_by = Column(String(100), nullable=True)  # User or system component
    tags = Column(JSONB, nullable=True)  # For categorization and filtering

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Constraints and indexes
    __table_args__ = (
        CheckConstraint("progress >= 0 AND progress <= 100", name="check_progress_range"),
        CheckConstraint("priority >= 1 AND priority <= 10", name="check_job_priority_range"),
        CheckConstraint("retry_count >= 0", name="check_retry_count_positive"),
        CheckConstraint("max_retries >= 0", name="check_max_retries_positive"),
        CheckConstraint(f"job_type IN ('{JobType.INGEST.value}', '{JobType.AGGREGATE.value}', '{JobType.GAP_FILL.value}', '{JobType.VALIDATE.value}', '{JobType.CLEANUP.value}')", name="check_job_type_valid"),
        CheckConstraint(f"status IN ('{JobStatus.PENDING.value}', '{JobStatus.RUNNING.value}', '{JobStatus.COMPLETED.value}', '{JobStatus.FAILED.value}', '{JobStatus.CANCELLED.value}')", name="check_status_valid"),

        # Performance indexes
        Index("idx_jobs_status_priority", "status", "priority"),
        Index("idx_jobs_type_status", "job_type", "status"),
        Index("idx_jobs_created_at", "created_at"),
        Index("idx_jobs_symbol_status", "symbol", "status"),
        Index("idx_jobs_started_at", "started_at"),
        Index("idx_jobs_heartbeat", "last_heartbeat"),

        # Partial indexes for active jobs - FIXED: Removed NOW() function calls
        Index("idx_jobs_active", "status", "priority", "created_at", postgresql_where=text(f"status IN ('{JobStatus.PENDING.value}', '{JobStatus.RUNNING.value}')")),
        Index("idx_jobs_failed_retryable", "job_type", "retry_count", "created_at", postgresql_where=text(f"status = '{JobStatus.FAILED.value}' AND retry_count < max_retries")),
    )

    @validates('job_type')
    def validate_job_type(self, key, job_type):
        """Validate job type is supported"""
        if job_type not in [t.value for t in JobType]:
            raise ValueError(f"Invalid job type: {job_type}")
        return job_type

    @validates('status')
    def validate_status(self, key, status):
        """Validate status is supported"""
        if status not in [s.value for s in JobStatus]:
            raise ValueError(f"Invalid status: {status}")
        return status

    @property
    def duration_seconds(self) -> int:
        """Calculate job duration in seconds"""
        if self.started_at and self.completed_at:
            return int((self.completed_at - self.started_at).total_seconds())
        elif self.started_at:
            return int((datetime.now() - self.started_at).total_seconds())
        return 0

    @property
    def is_active(self) -> bool:
        """Check if job is currently active"""
        return self.status in [JobStatus.PENDING.value, JobStatus.RUNNING.value]

    @property
    def can_retry(self) -> bool:
        """Check if job can be retried"""
        return self.status == JobStatus.FAILED.value and self.retry_count < self.max_retries

    def __repr__(self):
        return f"<PipelineJob({self.id}, {self.job_type}, {self.status}, symbol={self.symbol})>"


class DataQualityMetric(Base):
    """
    Stores data quality metrics and validation results
    Enables quality monitoring and trend analysis
    """
    __tablename__ = "data_quality_metrics"

    # Primary identification
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol = Column(String(50), nullable=False, index=True)
    timeframe = Column(String(10), nullable=False)
    metric_date = Column(DateTime(timezone=True), nullable=False, index=True)

    # Quality scores (0.0 to 1.0)
    overall_score = Column(Numeric(3, 2), nullable=False)
    completeness_score = Column(Numeric(3, 2), nullable=False)  # Data availability
    consistency_score = Column(Numeric(3, 2), nullable=False)  # OHLC relationships
    accuracy_score = Column(Numeric(3, 2), nullable=False)  # Price/volume validation
    timeliness_score = Column(Numeric(3, 2), nullable=False)  # Update frequency

    # Detailed metrics
    total_records = Column(Integer, nullable=False, default=0)
    valid_records = Column(Integer, nullable=False, default=0)
    invalid_records = Column(Integer, nullable=False, default=0)
    duplicate_records = Column(Integer, nullable=False, default=0)

    # Validation details
    validation_rules_passed = Column(Integer, nullable=False, default=0)
    validation_rules_failed = Column(Integer, nullable=False, default=0)
    anomalies_detected = Column(Integer, nullable=False, default=0)

    # Validation details
    validation_details = Column(JSONB, nullable=True)  # Detailed validation results
    source = Column(String(50), nullable=False)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Constraints and indexes
    __table_args__ = (
        CheckConstraint("overall_score >= 0.0 AND overall_score <= 1.0", name="check_overall_score_range"),
        CheckConstraint("completeness_score >= 0.0 AND completeness_score <= 1.0", name="check_completeness_score_range"),
        CheckConstraint("consistency_score >= 0.0 AND consistency_score <= 1.0", name="check_consistency_score_range"),
        CheckConstraint("accuracy_score >= 0.0 AND accuracy_score <= 1.0", name="check_accuracy_score_range"),
        CheckConstraint("timeliness_score >= 0.0 AND timeliness_score <= 1.0", name="check_timeliness_score_range"),
        CheckConstraint("total_records >= 0", name="check_total_records_positive"),
        CheckConstraint("valid_records >= 0", name="check_valid_records_positive"),
        CheckConstraint("invalid_records >= 0", name="check_invalid_records_positive"),
        CheckConstraint("valid_records + invalid_records <= total_records", name="check_record_counts_consistent"),
        Index("idx_quality_symbol_date", "symbol", "metric_date"),
        Index("idx_quality_date_timeframe", "metric_date", "timeframe"),
        Index("idx_quality_overall_score", "overall_score"),
        Index("idx_quality_source", "source"),
    )

    def __repr__(self):
        return f"<DataQualityMetric({self.symbol}, {self.timeframe}, {self.metric_date}, score={self.overall_score})>"
