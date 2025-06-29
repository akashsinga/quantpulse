# backend/app/tasks/__init__.py
"""
Background tasks module for QuantPulse

This module contains all Celery tasks for background processing:
- Securities import
"""

from .import_securities import import_securities_from_dhan

__all__ = ['import_securities_from_dhan']
