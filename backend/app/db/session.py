# backend/db/session.py

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from contextlib import contextmanager

from app.config import settings

# Create SQLAlchemy engine for synchronous operations
engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True, pool_recycle=settings.DB_POOL_RECYCLE, pool_size=settings.DB_POOL_SIZE, max_overflow=settings.DB_MAX_OVERFLOW, pool_timeout=settings.DB_POOL_TIMEOUT, pool_use_lifo=True, echo=settings.DB_ECHO)  # Check connection health before using  # Recycle connections  # Set pool size  # Set overflow  # Set timeout  # Use LIFO for better performance with many short operations

# Create async engine for FastAPI
async_engine = create_async_engine(
    settings.ASYNC_DATABASE_URL,
    pool_pre_ping=True,
    echo=settings.DB_ECHO,
)

# SessionLocal for synchronous operations (scripts, admin tasks)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

AsyncSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=async_engine, class_=AsyncSession)

# Base class for SQLAlchemy models
Base = declarative_base()


# Context manager for synchronous DB sessions
@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Async dependency for FastAPI endpoints
async def get_async_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


# Function to create TimescaleDB hypertables after table creation
def init_timescaledb(conn):
    """
    This function should be called after all tables are created to convert
    time-series tables to TimescaleDB hypertables
    """
    # Convert ohlcv_daily to hypertable
    conn.execute("SELECT create_hypertable('ohlcv_daily', 'time', if_not_exists => TRUE);")

    # Convert ohlcv_weekly to hypertable
    conn.execute("SELECT create_hypertable('ohlcv_weekly', 'time', if_not_exists => TRUE);")

    # Convert technical_indicators to hypertable
    conn.execute("SELECT create_hypertable('technical_indicators', 'time', if_not_exists => TRUE);")


# Function to initialize database (create tables, setup TimescaleDB)
def init_db():
    """Initialize the database by creating all tables."""
    # Create all tables
    Base.metadata.create_all(bind=engine)

    # Set up TimescaleDB hypertables
    with engine.begin() as conn:
        # Check if TimescaleDB extension is installed
        result = conn.execute("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb');").scalar()

        if result:
            # Set up TimescaleDB hypertables
            init_timescaledb(conn)
        else:
            print("WARNING: TimescaleDB extension is not installed. Hypertables not created.")

    print("Database initialized successfully.")
