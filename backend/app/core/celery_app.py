# backend/app/core/celery_app.py

from celery import Celery
from app.config import settings

# Create Celery instance with simpler configuration
celery_app = Celery("quantpulse")

# Configure broker and backend
celery_app.conf.broker_url = settings.REDIS_URL
celery_app.conf.result_backend = settings.REDIS_URL

# Basic Celery Configuration
celery_app.conf.update(
    # Serialization
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',

    # Timezone
    timezone='UTC',
    enable_utc=True,

    # Task tracking and events
    task_track_started=True,
    task_send_sent_event=True,
    worker_send_task_events=True,

    # Result backend
    result_expires=3600,  # 1 hour

    # Worker configuration
    worker_prefetch_multiplier=4,
    worker_max_tasks_per_child=1000,

    # Task execution limits
    task_soft_time_limit=3600,  # 1 hour
    task_time_limit=7200,  # 2 hours
    task_acks_late=True,
    task_reject_on_worker_lost=True,

    # Queue configuration
    task_default_queue='default',
    task_default_exchange='default',
    task_default_exchange_type='direct',
    task_default_routing_key='default',

    # Task routing for different queues
    task_routes={
        'import_securities': {
            'queue': 'import'
        },
    },
)

# Auto-discover tasks from the tasks module
celery_app.autodiscover_tasks(['app.tasks'])

# Make sure celery_app is available at module level
__all__ = ['celery_app']
