from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool, text

from alembic import context

from utils.logger import get_logger

import os
import sys

logger = get_logger(__name__)

# Add the parent directory to the path so we can import from our app
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

# Import environment variables
from dotenv import load_dotenv

load_dotenv()

# Import database configuration
try:
    from app.config import settings

    database_url = settings.DATABASE_URL
except (ImportError, AttributeError):
    # Fallback if settings can't be imported
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost/quantpulse")

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Override the sqlalchemy.url from alembic.ini with our database URL
config.set_main_option("sqlalchemy.url", database_url)

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import all models to ensure they're known to the metadata
try:
    from app.db.session import Base

    # Import all models to ensure Base.metadata includes them
    import app.db.models

    target_metadata = Base.metadata
except ImportError:
    logger.warning("WARNING: Could not import models. Running with empty metadata.")
    target_metadata = None


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True, dialect_opts={"paramstyle": "named"})

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    # Modified to fix the NoneType error
    section = config.get_section(config.config_file_name)
    if section is None:
        # Create a fallback configuration if the section is not found
        section = {"sqlalchemy.url": database_url}

    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()

            # After running migrations, set up TimescaleDB hypertables
            # Only run this part if we're using PostgreSQL and TimescaleDB is installed
            try:
                if context.get_context().dialect.name == "postgresql":
                    # Check if TimescaleDB extension is installed
                    result = connection.execute(text("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb');")).scalar()

                    if result:
                        logger.info("Setting up TimescaleDB hypertables...")

                        # Helper function to check if a table is already a hypertable
                        def is_hypertable(table_name):
                            result = connection.execute(text(f"SELECT EXISTS(SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = '{table_name}');")).scalar()
                            return result

                        # Convert tables to hypertables if they exist and aren't already hypertables
                        for table_name in ["ohlcv_daily", "ohlcv_weekly", "technical_indicators"]:
                            # First check if the table exists
                            table_exists = connection.execute(text(f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');")).scalar()

                            if table_exists and not is_hypertable(table_name):
                                connection.execute(text(f"SELECT create_hypertable('{table_name}', 'time', if_not_exists => TRUE);"))
                                logger.info(f"Created hypertable for {table_name}")
            except Exception as e:
                logger.warning(f"Warning: Failed to set up TimescaleDB hypertables: {e}")


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
