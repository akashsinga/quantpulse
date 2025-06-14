# backend/app/core/database.py
"""
Database connection and session management for QuantPulse
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
from typing import Generator

from app.models.base import Base
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseManager:
    """Manages database connections and sessions"""

    def __init__(self, database_url: str, **engine_kwargs):
        """
        Initialize database manager.
        Args:
            database_url: Database connection sharing
            **engine_kwargs: Additional engine configuration
        """
        self.database_url = database_url
        self.engine = self._create_engine(**engine_kwargs)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def _create_engine(self, **kwargs):
        """Create SQLAlchemy engine with optimized settings"""
        default_config = {'pool_pre_ping': True, 'pool_recycle': 1800, 'pool_size': 20, 'max_overflow': 20, 'pool_timeout': 30, 'poolclass': QueuePool, 'echo': False}
        # Override defaults with provided kwargs
        config = {**default_config, **kwargs}
        return create_engine(self.database_url, **config)

    @contextmanager
    def get_session(self) -> Generator:
        """
        Context manager for database sessions
        Usage:
            with db_manager.get_session() as session:
                session.query()
        """
        session = self.SessionLocal()
        try:
            yield session
        except Exception:
            session.rollback()
        finally:
            session.close()

    def get_session_dependency(self):
        """
        Dependency for FastAPI route injection
        Usage:
            @app.get("/")
            def route(db: Session = Depends(db_manager.get_session_dependency)):
                # Use db session
        """
        session = self.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    def create_tables(self):
        """Create all tables defined in models"""
        logger.info("Creating database tables ...")
        Base.metadata.create_all(bind=self.engine)
        logger.success("Database tables created succesfully")

    def drop_tables(self):
        """Drop all tables"""
        logger.warning("Dropping all database tables...")
        Base.metadata.drop_all(bind=self.engine)
        logger.warning("All database tables dropped")

    def check_connection(self) -> bool:
        """Check if database connection is working"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                return result == 1
        except Exception as e:
            logger.error(f"Database connection check failed: {e}")
            return False

    def get_database_info(self) -> dict:
        """Get database information for diagnostics"""
        try:
            with self.engine.connect() as conn:
                # Basic database info
                db_version = conn.execute(text("SELECT version()")).scalar()

                # Check TimescaleDB
                timescaledb_version = None

                try:
                    result = conn.execute(text("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")).scalar()
                    timescaledb_version = result
                except:
                    pass

                # Connection pool info
                pool_info = {"pool_size": self.engine.pool.size(), "checked_in": self.engine.pool.checkedin(), "checked_out": self.engine.pool.checkedout(), "overflow": self.engine.pool.overflow()}

                return {"database_version": db_version, "timescaledb_version": timescaledb_version, "timescaledb_enabled": timescaledb_version is not None, "connection_pool": pool_info, "database_url_host": self.database_url.split('@')[1] if '@' in self.database_url else "hidden"}
        except Exception as e:
            return {"error": str(e)}

    def cleanup(self):
        """Clean up database connections."""
        try:
            self.engine.dispose()
            logger.info("Database connections cleaned up")
        except Exception as e:
            logger.error(f"Error cleaning up database connections: {e}")


# Global database manager instance (will be initialized in main app)
db_manager: DatabaseManager = None


def init_database(database_url: str, **kwargs) -> DatabaseManager:
    """
    Initialize the global database manager.
    
    Args:
        database_url: Database connection string
        **kwargs: Additional engine configuration
        
    Returns:
        DatabaseManager instance
    """
    global db_manager
    db_manager = DatabaseManager(database_url, **kwargs)
    return db_manager


def get_db():
    """
    Dependency function for FastAPI.
    
    Usage:
        @app.get("/")
        def route(db: Session = Depends(get_db)):
            # Use db session
    """
    if db_manager is None:
        raise RuntimeError("Database not initialized. Call init_database() first.")

    return db_manager.get_session_dependency()
