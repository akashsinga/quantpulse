# backend/app/core/celery_app.py

from celery import Celery
from app.config import settings

# Create Celery instance
celery_app = Celery("quantpulse", broker=settings.REDIS_URL, backend=settings.REDIS_URL)

# Basic Celery Configuration
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    result_expires=3600,  # 1 hour
    task_track_started=True,  # Track when tasks start
    task_send_sent_event=True,  # Send task sent events
    worker_send_task_events=True,  # Send task events from worker
)

# Auto-discover tasks from the tasks module
celery_app.autodiscover_tasks(['app.tasks'])

print("Celery app configured successfully")
