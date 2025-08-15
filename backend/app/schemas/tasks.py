# backend/app/schemas/tasks.py
"""
Pydantic schemas for task management API.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator
from uuid import UUID

from app.schemas.base import BaseResponseSchema
from app.utils.enum import TaskStatus, TaskType


class TaskRunBase(BaseModel):
    task_name: str
    task_type: TaskType
    title: str
    description: Optional[str] = None


class TaskRunResponse(BaseResponseSchema):
    id: UUID
    celery_task_id: str
    task_name: str
    task_type: TaskType
    title: str
    description: Optional[str]
    status: TaskStatus
    progress_percentage: int
    current_message: Optional[str]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    user_id: Optional[UUID]
    execution_time_seconds: Optional[int]
    retry_count: int

    class Config:
        from_attributes = True

    @property
    def duration_display(self) -> str:
        if self.execution_time_seconds is None:
            return "N/A"

        seconds = self.execution_time_seconds
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            minutes = seconds // 60
            return f"{minutes}m {seconds % 60}s"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours}h {minutes}m"

    @property
    def status_display(self) -> str:
        """Human-readable status"""
        status_mapping = {TaskStatus.PENDING: "Pending", TaskStatus.RECEIVED: "Received", TaskStatus.STARTED: "Started", TaskStatus.PROGRESS: "In Progress", TaskStatus.SUCCESS: "Completed", TaskStatus.FAILURE: "Failed", TaskStatus.RETRY: "Retrying", TaskStatus.REVOKED: "Cancelled", TaskStatus.CANCELLED: "Cancelled"}
        return status_mapping.get(self.status, str(self.status))

    @property
    def is_running(self) -> bool:
        """Check if task is currently running"""
        return self.status in [TaskStatus.PENDING, TaskStatus.RECEIVED, TaskStatus.STARTED, TaskStatus.PROGRESS]

    @property
    def is_completed(self) -> bool:
        """Check if task is completed (success or failure)"""
        return self.status in [TaskStatus.SUCCESS, TaskStatus.FAILURE, TaskStatus.CANCELLED, TaskStatus.REVOKED]


class TaskLogResponse(BaseResponseSchema):
    id: UUID
    task_run_id: UUID
    level: str
    message: str
    extra_data: Optional[Dict[str, Any]]
    created_at: datetime

    class Config:
        from_attributes = True

    @property
    def level_display(self) -> str:
        """Human-readable log level"""
        return self.level.upper()

    @property
    def severity_color(self) -> str:
        """Color for UI display based on log level"""
        level_colors = {"DEBUG": "secondary", "INFO": "info", "WARNING": "warning", "ERROR": "danger", "CRITICAL": "danger"}
        return level_colors.get(self.level.upper(), "secondary")


class TaskStepResponse(BaseResponseSchema):
    id: UUID
    task_run_id: UUID
    step_name: str
    step_order: int
    title: str
    status: TaskStatus
    result_data: Optional[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class TaskRunDetailResponse(TaskRunResponse):
    input_parameters: Optional[Dict[str, Any]]
    result_data: Optional[Dict[str, Any]]
    error_message: Optional[str]
    error_traceback: Optional[str]

    # Related data
    logs: List[TaskLogResponse] = []
    steps: List[TaskStepResponse] = []

    class Config:
        from_attributes = True

    @property
    def has_error_details(self) -> bool:
        """Check if task has error information"""
        return bool(self.error_message or self.error_traceback)

    @property
    def summary_stats(self) -> Dict[str, Any]:
        """Summary statistics for the task"""
        return {"total_steps": len(self.steps), "completed_steps": len([s for s in self.steps if s.status == TaskStatus.SUCCESS]), "failed_steps": len([s for s in self.steps if s.status == TaskStatus.FAILURE]), "total_logs": len(self.logs), "error_logs": len([l for l in self.logs if l.level.upper() in ["ERROR", "CRITICAL"]]), "warning_logs": len([l for l in self.logs if l.level.upper() == "WARNING"])}


class TaskRetryRequest(BaseModel):
    reason: Optional[str] = Field(None, description="Reason for retrying the task")

    class Config:
        json_schema_extra = {"example": {"reason": "Retry after fixing data issue"}}


class TaskCancelRequest(BaseModel):
    reason: Optional[str] = Field(None, description="Reason for cancelling the task")

    class Config:
        json_schema_extra = {"example": {"reason": "No longer needed"}}


class TaskFilters(BaseModel):
    status: Optional[TaskStatus] = None
    task_type: Optional[TaskType] = None
    user_id: Optional[UUID] = None
    task_name: Optional[str] = None
    created_after: Optional[str] = None
    created_before: Optional[str] = None
    sort_by: str = "created_at"
    sort_order: str = "desc"

    @validator('created_after', 'created_before')
    def validate_date_format(cls, v):
        """Validate date format for filtering"""
        if v is not None:
            try:
                datetime.fromisoformat(v.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError('Date must be in ISO format (YYYY-MM-DDTHH:MM:SS)')
        return v

    @validator('sort_order')
    def validate_sort_order(cls, v):
        """Validate sort order"""
        if v not in ['asc', 'desc']:
            raise ValueError('Sort order must be "asc" or "desc"')
        return v

    @validator('sort_by')
    def validate_sort_by(cls, v):
        """Validate sort field"""
        allowed_fields = ['created_at', 'updated_at', 'started_at', 'completed_at', 'status', 'task_type', 'title', 'progress_percentage']
        if v not in allowed_fields:
            raise ValueError(f'Sort field must be one of: {", ".join(allowed_fields)}')
        return v


class TaskStatsResponse(BaseModel):
    total_tasks: int
    running_tasks: int
    recent_tasks_24h: int
    recent_failures_7d: int
    success_rate_percentage: float
    status_breakdown: Dict[str, int]
    type_breakdown: Dict[str, int]
    running_task_details: List[Dict[str, Any]]

    class Config:
        json_schema_extra = {"example": {"total_tasks": 150, "running_tasks": 3, "recent_tasks_24h": 12, "recent_failures_7d": 2, "success_rate_percentage": 94.5, "status_breakdown": {"SUCCESS": 120, "FAILURE": 8, "PENDING": 2, "PROGRESS": 1}, "type_breakdown": {"SECURITIES_IMPORT": 140, "DATA_ENRICHMENT": 10}, "running_task_details": []}}


class TaskBulkActionRequest(BaseModel):
    task_ids: List[UUID] = Field(..., min_items=1, max_items=50, description="List of task IDs")
    action: str = Field(..., description="Action to perform: 'cancel', 'retry', 'delete'")
    reason: Optional[str] = Field(None, description="Reason for the bulk action")
    force: bool = Field(False, description="Force action even if some tasks cannot be processed")

    @validator('action')
    def validate_action(cls, v):
        """Validate bulk action type"""
        allowed_actions = ['cancel', 'retry', 'delete']
        if v not in allowed_actions:
            raise ValueError(f'Action must be one of: {", ".join(allowed_actions)}')
        return v

    class Config:
        json_schema_extra = {"example": {"task_ids": ["550e8400-e29b-41d4-a716-446655440000"], "action": "cancel", "reason": "Batch cancellation for maintenance", "force": False}}


class TaskBulkActionResponse(BaseModel):
    total_requested: int
    successful: int
    failed: int
    errors: List[str]
    processed_task_ids: List[UUID]

    class Config:
        json_schema_extra = {"example": {"total_requested": 5, "successful": 4, "failed": 1, "errors": ["Task 550e8400... cannot be cancelled (already completed)"], "processed_task_ids": ["550e8400-e29b-41d4-a716-446655440001"]}}
