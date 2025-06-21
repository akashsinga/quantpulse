# backend/app/core/database.py
"""
Database connection and session management fpr QuantPulse
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
from typing import Generator

from app.models.base import Base
from app.utils.logger import get_logger
from app.core.config import settings

logger = get_logger(__name__)


class DatabaseManager:
    """Manages database connections and sessions"""

    def __init__(self, database_url: str, **engine_kwargs):
        """Initialize database manager"""
        self.database_url = database_url
        self.engine = self._create_engine(**engine_kwargs)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def _create_engine(self, **kwargs):
        """Create SQLAlchemy engine with optimized settings"""
        default_config = {'pool_pre_ping': True, 'pool_recycle': settings.database.DB_POOL_RECYCLE, 'pool_size': settings.database.DB_POOL_SIZE, 'max_overflow': settings.database.DB_MAX_OVERFLOW, 'pool_timeout': settings.database.DB_POOL_TIMEOUT, 'poolclass': QueuePool, 'echo': settings.database.DB_ECHO}

        # Override defaults with provided kwargs
        config = {**default_config, **kwargs}

        return create_engine(self.database_url, **config)

    def create_tables(self):
        """Create all tables defined in models."""
        logger.info("Creating database tables...")
        Base.metadata.create_all(bind=self.engine)
        logger.info("Database tables created successfully")

    def drop_tables(self):
        """Drop all tables (use with caution!)."""
        logger.warning("Dropping all database tables...")
        Base.metadata.drop_all(bind=self.engine)
        logger.warning("All database tables dropped")

    @contextmanager
    def get_session(self) -> Generator:
        """
        Context manager for database sessions
        """
        session = self.SessionLocal()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_session_dependency(self):
        """
        Dependency for FastAPI route injection
        """
        session = self.SessionLocal()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


db_manager: DatabaseManager = None


def init_database(database_url: str, **kwargs) -> DatabaseManager:
    """Initialize the global database manager"""
    global db_manager
    if db_manager is not None:
        logger.warning("Database already initialized, returning existing instance")
        return db_manager
    db_manager = DatabaseManager(database_url, **kwargs)
    return db_manager


def get_db():
    """Dependency function for FastAPI"""
    if db_manager is None:
        raise RuntimeError("Database not initialized. Call init_database() first")
    return db_manager.get_session_dependency()
