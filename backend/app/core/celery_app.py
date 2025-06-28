# backend/app/core/celery_app.py

import os

from celery import Celery
from celery.signals import worker_init, worker_process_init
from kombu import Queue

from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


def create_celery_app() -> Celery:
    """Create and configure Celery application"""

    celery_app = Celery("quantpulse", broker=settings.celery.REDIS_URL, backend=settings.celery.REDIS_URL, include=['app.tasks.securities_import'])

    celery_app.conf.update(
        # Task routes
        task_routes={
            'app.tasks.securities_import.*': {
                'queue': 'securities'
            },
        },

        # Queue definitions
        task_queues=[
            Queue('default', routing_key='default'),
            Queue('securities', routing_key='securities'),
        ],

        # Task execution settings
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='Asia/Kolkata',
        enable_utc=True,

        # Task result settings
        result_expires=3600 * 24,  # 24 hours
        result_backend_transport_options={
            'retry_on_timeout': True,
        },

        # Task execution behavior
        task_acks_late=True,
        worker_prefetch_multiplier=1,
        task_reject_on_worker_lost=True,

        # Retry settings
        task_default_retry_delay=60,  # 1 minute
        task_max_retries=3,

        # Worker settings
        worker_disable_rate_limits=False,
        worker_pool_restarts=True,

        # Monitoring
        worker_send_task_events=True,
        task_send_sent_event=True,

        # Security
        worker_hijack_root_logger=False,
        worker_log_color=False,
    )

    return celery_app


@worker_init.connect
def worker_init_handler(sender=None, conf=None, **kwargs):
    """Initialize worker process"""
    logger.info("Celery worker initializing...")


@worker_process_init.connect
def worker_process_init_handler(sender=None, **kwargs):
    """Initialize worker process - setup database connections"""
    logger.info("Celery worker process started")
