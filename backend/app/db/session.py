# backend/app/db/session.py - CLEANED FOR UNIFIED PIPELINE

from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from app.config import settings

# Create SQLAlchemy engine for synchronous operations
engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,  # Check connection health before using
    pool_recycle=settings.DB_POOL_RECYCLE,  # Recycle connections
    pool_size=settings.DB_POOL_SIZE,  # Set pool size
    max_overflow=settings.DB_MAX_OVERFLOW,  # Set overflow
    pool_timeout=settings.DB_POOL_TIMEOUT,  # Set timeout
    pool_use_lifo=True,  # Use LIFO for better performance with many short operations
    echo=settings.DB_ECHO  # SQL logging
)

# SessionLocal for synchronous operations
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for SQLAlchemy models
Base = declarative_base()


# Context manager for synchronous DB sessions
@contextmanager
def get_db():
    """Context manager for database sessions"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db_session():
    """Get a DB session for FastAPI dependency injection"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Function to create TimescaleDB hypertables after table creation
def init_timescaledb(conn):
    """
    Convert time-series tables to TimescaleDB hypertables
    Updated for unified OHLCV pipeline
    """
    try:
        # Convert ohlcv_unified to hypertable with symbol partitioning
        conn.execute(text("""
            SELECT create_hypertable(
                'ohlcv_unified', 
                'timestamp',
                partitioning_column => 'symbol',
                number_partitions => 4,
                if_not_exists => TRUE
            );
        """))

        # Convert data_quality_metrics to hypertable
        conn.execute(text("""
            SELECT create_hypertable(
                'data_quality_metrics', 
                'metric_date',
                if_not_exists => TRUE
            );
        """))

        # Add compression policy for older OHLCV data (compress data older than 30 days)
        conn.execute(text("""
            SELECT add_compression_policy(
                'ohlcv_unified', 
                INTERVAL '30 days',
                if_not_exists => TRUE
            );
        """))

        # Add retention policy for quality metrics (keep for 2 years)
        conn.execute(text("""
            SELECT add_retention_policy(
                'data_quality_metrics', 
                INTERVAL '2 years',
                if_not_exists => TRUE
            );
        """))

        print("TimescaleDB hypertables and policies configured successfully")

    except Exception as e:
        print(f"Warning: Could not configure TimescaleDB features: {e}")
        # Continue without TimescaleDB optimizations


# Function to initialize database (create tables, setup TimescaleDB)
def init_db():
    """Initialize the database by creating all tables and TimescaleDB features."""
    # Create all tables
    Base.metadata.create_all(bind=engine)

    # Set up TimescaleDB hypertables and policies
    with engine.begin() as conn:
        # Check if TimescaleDB extension is installed
        result = conn.execute(text("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb');")).scalar()

        if result:
            print("TimescaleDB extension detected, setting up hypertables...")
            init_timescaledb(conn)
        else:
            print("WARNING: TimescaleDB extension is not installed. Using regular PostgreSQL tables.")

    print("Database initialized successfully.")


def check_database_connection():
    """Check if database connection is working"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            return result == 1
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False


def get_database_info():
    """Get database information for diagnostics"""
    try:
        with engine.connect() as conn:
            # Basic database info
            db_version = conn.execute(text("SELECT version()")).scalar()

            # Check TimescaleDB
            timescaledb_version = None
            try:
                timescaledb_version = conn.execute(text("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")).scalar()
            except:
                pass

            # Connection pool info
            pool_info = {
                "pool_size": engine.pool.size(),
                "checked_in": engine.pool.checkedin(),
                "checked_out": engine.pool.checkedout(),
                "overflow": engine.pool.overflow(),
            }

            return {"database_version": db_version, "timescaledb_version": timescaledb_version, "timescaledb_enabled": timescaledb_version is not None, "connection_pool": pool_info, "database_url": settings.DATABASE_URL.split('@')[1] if '@' in settings.DATABASE_URL else "hidden"}
    except Exception as e:
        return {"error": str(e)}


def cleanup_database_connections():
    """Clean up database connections (useful for testing)"""
    try:
        engine.dispose()
        print("Database connections cleaned up")
    except Exception as e:
        print(f"Error cleaning up database connections: {e}")
