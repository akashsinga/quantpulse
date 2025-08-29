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

    celery_app = Celery("quantpulse", broker=settings.celery.REDIS_URL, backend=settings.celery.REDIS_URL)

    celery_app.conf.update(
        # Task routes
        task_routes={
            'import_securities_from_dhan': {
                'queue': 'securities'
            },
            'enrich_sectors_from_dhan': {
                'queue': 'enrichment'
            }
        },

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

        # Add imports for task discovery
        imports=['app.tasks.import_securities', 'app.tasks.enrich_sectors'])

    return celery_app


celery_app = create_celery_app()

# Import tasks after celery_app is created to avoid circular imports
try:
    from app.tasks import import_securities
    from app.tasks import enrich_sectors
    logger.info("Successfully imported task modules")
except ImportError as e:
    logger.error(f"Failed to import task modules: {e}")


@worker_init.connect
def worker_init_handler(sender=None, conf=None, **kwargs):
    """Initialize worker process"""
    logger.info("Celery worker initializing...")


@worker_process_init.connect
def worker_process_init_handler(sender=None, **kwargs):
    """Initialize worker process - setup database connections"""
    logger.info("Celery worker process started")
