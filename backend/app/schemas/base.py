# backend/app/schemas/base.py
"""
Base Pydantic schemas for QuantPulse application
Provides common response patterns and base classes.
"""

from typing import Optional, Any, List, Dict, Generic, TypeVar
from datetime import datetime
from pydantic import BaseModel, Field
from uuid import UUID

# Type variable for generic responses
T = TypeVar('T')


class BaseResponseSchema(BaseModel):
    """Base response schema with common fields"""

    class Config:
        from_attributes = True
        json_encoders = {datetime: lambda v: v.isoformat(), UUID: lambda v: str(v)}


class PaginationMeta(BaseModel):
    """Pagination metadata for list responses."""
    total: int = Field(..., description="Total number of items")
    page: int = Field(..., description="Current page number")
    per_page: int = Field(..., description="Items per page")
    pages: int = Field(..., description="Total number of pages")
    has_next: bool = Field(..., description="Whether there is a next page")
    has_prev: bool = Field(..., description="Whether there is a previous page")

    @classmethod
    def create(cls, total: int, page: int, per_page: int) -> 'PaginationMeta':
        """Create pagination metadata."""
        pages = (total + per_page - 1) // per_page if per_page > 0 else 0

        return cls(total=total, page=page, per_page=per_page, pages=pages, has_next=page < pages, has_prev=page > 1)


class APIResponse(BaseModel, Generic[T]):
    """Generic API response wrapper."""
    success: bool = Field(default=True, description="Whether the request was successful")
    message: str = Field(default="", description="Response message")
    data: Optional[T] = Field(default=None, description="Response data")
    errors: List[str] = Field(default_factory=list, description="List of errors")
    meta: Optional[Dict[str, Any]] = Field(default=None, description="Additional metadata")

    class Config:
        json_schema_extra = {"example": {"success": True, "message": "Operation completed successfully", "data": {}, "errors": [], "meta": None}}


class PaginatedResponse(BaseModel, Generic[T]):
    """Paginated response wrapper."""
    success: bool = Field(default=True, description="Whether the request was successful")
    message: str = Field(default="", description="Response message")
    data: List[T] = Field(..., description="List of items")
    pagination: PaginationMeta = Field(..., description="Pagination metadata")
    errors: List[str] = Field(default_factory=list, description="List of errors")

    class Config:
        json_schema_extra = {"example": {"success": True, "message": "Items retrieved successfully", "data": [], "pagination": {"total": 100, "page": 1, "per_page": 10, "pages": 10, "has_next": True, "has_prev": False}, "errors": []}}


class ErrorResponse(BaseModel):
    """Error response schema."""
    success: bool = Field(default=False, description="Always false for error responses")
    message: str = Field(..., description="Error message")
    errors: List[str] = Field(default_factory=list, description="List of detailed errors")
    error_code: Optional[str] = Field(default=None, description="Machine-readable error code")
    details: Optional[Dict[str, Any]] = Field(default=None, description="Additional error details")

    class Config:
        json_schema_extra = {"example": {"success": False, "message": "Validation failed", "errors": ["Email is required", "Password must be at least 8 characters"], "error_code": "VALIDATION_ERROR", "details": {"field_errors": {"email": ["This field is required"], "password": ["Must be at least 8 characters"]}}}}


class SuccessResponse(BaseModel):
    """Simple success response schema."""
    success: bool = Field(default=True, description="Always true for success responses")
    message: str = Field(..., description="Success message")

    class Config:
        json_schema_extra = {"example": {"success": True, "message": "Operation completed successfully"}}


# ============ Common filter schemas ========


class DateRangeFilter(BaseModel):
    """Date range filter schema."""
    start_date: Optional[datetime] = Field(default=None, description="Start date")
    end_date: Optional[datetime] = Field(default=None, description="End date")

    def validate_range(self) -> bool:
        """Validate that start_date is before end_date."""
        if self.start_date and self.end_date:
            return self.start_date <= self.end_date
        return True


class SortOptions(BaseModel):
    """Sort options schema."""
    sort_by: str = Field(default="created_at", description="Field to sort by")
    sort_order: str = Field(default="desc", pattern="^(asc|desc)$", description="Sort order")

    @property
    def is_descending(self) -> bool:
        """Check if sort order is descending."""
        return self.sort_order.lower() == "desc"


class SearchFilter(BaseModel):
    """Search filter schema."""
    query: Optional[str] = Field(default=None, description="Search query")
    fields: List[str] = Field(default_factory=list, description="Fields to search in")

    def has_query(self) -> bool:
        """Check if search query is provided."""
        return bool(self.query and self.query.strip())


# ====== Request validation schemas =======
class IDRequest(BaseModel):
    """Request schema for operations requiring an ID."""
    id: UUID = Field(..., description="Resource ID")


class BulkIDRequest(BaseModel):
    """Request schema for bulk operations with IDs."""
    ids: List[UUID] = Field(..., min_items=1, max_items=100, description="List of resource IDs")
