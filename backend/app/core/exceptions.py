# backend/app/core/exceptions.py
"""
Custom exceptions hierarchy for QuantPulse.
Provides structured error handling with proper HTTP status codes.
"""

from typing import Any, Dict, Optional, List
from fastapi import HTTPException, status


class QuantPulseException(Exception):
    """
    Base exception for all QuantPulse specific errors.
    All custom exceptions should inherit from this class.
    """

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None, error_code: Optional[str] = None):
        self.message = message
        self.details = details or {}
        self.error_code = error_code
        super().__init__(self.message)

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary format"""
        return {"error_type": self.__class__.__name__, "message": self.message, "details": self.details, "error_code": self.error_code}


class ValidationError(QuantPulseException):
    """Raised when data validation fails."""

    def __init__(self, message: str = "Validation failed", field_errors: Optional[Dict[str, Any]] = None, **kwargs):
        self.field_errors = field_errors
        super().__init__(message, details={"field_errors": self.field_errors}, **kwargs)


class NotFoundError(QuantPulseException):
    """Raised when a requested resource is not found"""

    def __init__(self, resource: str, identifier: Optional[str] = None, **kwargs):
        message = f"{resource} not found"
        if identifier:
            message += f" with identifier: {identifier}"

        details = {"resource": resource}
        if identifier:
            details["identifier"] = identifier

        super().__init__(message, details=details, **kwargs)


class AuthenticationError(QuantPulseException):
    """Raised when authentication fails."""

    def __init__(self, message: str = "Authentication failed", **kwargs):
        super().__init__(message, **kwargs)


class AuthorizationError(QuantPulseException):
    """Raised when user lacks permission for an action."""

    def __init__(self, action: str, resource: Optional[str] = None, **kwargs):
        message = f"Not authorized to {action}"
        if resource:
            message += f" {resource}"

        details = {"action": action}
        if resource:
            details["resource"] = resource

        super().__init__(message, details=details, **kwargs)


class BusinessLogicError(QuantPulseException):
    """Raised when business logic rules are violated."""
    pass


class ExternalAPIError(QuantPulseException):
    """Raised when external API calls fail."""

    def __init__(self, api_name: str, message: str = "External API error", status_code: Optional[int] = None, response_data: Optional[Dict[str, Any]] = None, **kwargs):
        self.api_name = api_name
        self.status_code = status_code
        self.response_data = response_data

        details = {"api_name": api_name, "status_code": status_code, "response_data": response_data}

        super().__init__(message, details=details, **kwargs)


class DatabaseError(QuantPulseException):
    """Raised when database operations fail."""

    def __init__(self, operation: str, message: str = "Database operation failed", **kwargs):
        details = {"operation": operation}
        super().__init__(message, details=details, **kwargs)


class ConfigurationError(QuantPulseException):
    """Raised when configuration is invalid or missing."""

    def __init__(self, config_key: str, message: Optional[str] = None, **kwargs):
        if not message:
            message = f"Invalid or missing configuration: {config_key}"

        details = {"config_key": config_key}
        super().__init__(message, details=details, **kwargs)


def to_http_exception(exc: QuantPulseException) -> HTTPException:
    """
    Convert QuantPulse exceptions to FastAPI HTTPException.
    
    Args:
        exc: QuantPulse exception to convert
        
    Returns:
        HTTPException with appropriate status code and details
    """
    status_code_map = {
        ValidationError: status.HTTP_400_BAD_REQUEST,
        NotFoundError: status.HTTP_404_NOT_FOUND,
        AuthenticationError: status.HTTP_401_UNAUTHORIZED,
        AuthorizationError: status.HTTP_403_FORBIDDEN,
        BusinessLogicError: status.HTTP_422_UNPROCESSABLE_ENTITY,
        ExternalAPIError: status.HTTP_502_BAD_GATEWAY,
        DatabaseError: status.HTTP_500_INTERNAL_SERVER_ERROR,
        ConfigurationError: status.HTTP_500_INTERNAL_SERVER_ERROR,
    }

    status_code = status_code_map.get(type(exc), status.HTTP_500_INTERNAL_SERVER_ERROR)

    return HTTPException(status_code=status_code, detail={"error": exc.to_dict(), "success": False})


class ErrorDetail:
    """Standard error detail structure."""

    def __init__(self, error_type: str, message: str, details: Optional[Dict[str, Any]] = None, error_code: Optional[str] = None):
        self.error_type = error_type
        self.message = message
        self.details = details or {}
        self.error_code = error_code


class ErrorResponse:
    """Standard error response structure."""

    def __init__(self, success: bool = False, error: Optional[ErrorDetail] = None, errors: Optional[List[str]] = None):
        self.success = success
        self.error = error
        self.errors = errors or []
