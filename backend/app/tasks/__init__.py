# backend/app/tasks/__init__.py
"""
Background tasks module for QuantPulse

This module contains all Celery tasks for background processing:
- Securities import
- Data fetching
- Strategy backtesting
- ML model training
etc.
"""

# Import the main task
from .securities_import import import_securities_task

# Import helper modules (for easier access if needed)
from . import securities_import_helpers
from . import securities_import_db

__all__ = ['import_securities_task', 'securities_import_helpers', 'securities_import_db']
