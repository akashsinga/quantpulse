# backend/app/scripts/init_system.py

from app.core.security import password_manager
from app.utils.logger import get_logger
from app.repositories.users import UserRepository, UserPreferencesRepository
from app.repositories.securities import ExchangeRepository
from app.core.exceptions import ValidationError
from app.utils.enum import Timeframe

logger = get_logger(__name__)


def create_superuser(db_manager, email, password, full_name=None):
    """Create a superuser with default preferences"""
    with db_manager.get_session() as db:
        user_repository = UserRepository(db)
        user_preferences_repository = UserPreferencesRepository(db)

        try:
            user = user_repository.create_user(email=email, hashed_password=password_manager.get_hashed_password(password), full_name=full_name, is_superuser=True)
            logger.info(f"Superuser {email} created successfully")

            user_preferences = user_preferences_repository.create_or_update_preferences(user.id, {"theme": "light", "language": "en", "currency": "INR", "preferred_timeline": Timeframe.DAILY.value, "email_notifications": True, "signal_notifications": True, "portfolio_alerts": True, "system_notifications": True})
            logger.info(f"Superuser preferences created successfully for {email}")

        except ValidationError as e:
            logger.info(f"User {email} already exists")
        except Exception as e:
            logger.error(f"Error creating superuser: {e}")


def seed_exchanges(db_manager):
    """Seed all Indian exchanges for comprehensive market coverage"""
    exchanges_data = [{
        "name": "National Stock Exchange of India",
        "code": "NSE",
        "country": "India",
        "timezone": "Asia/Kolkata",
        "currency": "INR",
        "trading_hours_start": "09:15",
        "trading_hours_end": "15:30",
        "is_active": True
    }, {
        "name": "Bombay Stock Exchange",
        "code": "BSE",
        "country": "India",
        "timezone": "Asia/Kolkata",
        "currency": "INR",
        "trading_hours_start": "09:15",
        "trading_hours_end": "15:30",
        "is_active": True
    }, {
        "name": "Multi Commodity Exchange",
        "code": "MCX",
        "country": "India",
        "timezone": "Asia/Kolkata",
        "currency": "INR",
        "trading_hours_start": "09:00",
        "trading_hours_end": "23:30",
        "is_active": True
    }, {
        "name": "National Commodity and Derivatives Exchange",
        "code": "NCDEX",
        "country": "India",
        "timezone": "Asia/Kolkata",
        "currency": "INR",
        "trading_hours_start": "09:00",
        "trading_hours_end": "17:00",
        "is_active": True
    }]

    with db_manager.get_session() as db:
        exchange_repository = ExchangeRepository(db)
        created_exchanges = []

        for exchange_data in exchanges_data:
            try:
                # Check if exchange already exists
                existing_exchange = exchange_repository.get_by_code(exchange_data["code"])

                if existing_exchange:
                    logger.info(f"{exchange_data['code']} exchange already exists")
                    created_exchanges.append(existing_exchange)
                    continue

                # Create exchange
                exchange = exchange_repository.create_exchange(**exchange_data)
                created_exchanges.append(exchange)
                logger.info(f"{exchange_data['code']} exchange created successfully with ID: {exchange.id}")

            except Exception as e:
                logger.error(f"Error creating {exchange_data['code']} exchange: {e}")

        return created_exchanges


def initialize_system(db_manager):
    """Initialize the complete system with all required seed data"""
    logger.info("Starting system initialization...")

    try:
        # Create superuser
        create_superuser(db_manager, "admin@quantpulse.app", "password", "System Administrator")

        # Seed all exchanges
        exchanges = seed_exchanges(db_manager)
        logger.info(f"Exchange setup completed: {len(exchanges)} exchanges initialized")

        # Log summary of initialized exchanges
        with db_manager.get_session() as db:
            exchange_repository = ExchangeRepository(db)
            all_exchanges = exchange_repository.get_all()

            logger.info("=== SYSTEM INITIALIZATION SUMMARY ===")
            logger.info(f"Total exchanges initialized: {len(all_exchanges)}")
            for exchange in all_exchanges:
                logger.info(f"  - {exchange.code}: {exchange.name} ({'Active' if exchange.is_active else 'Inactive'})")

        logger.info("System initialization completed successfully.")

    except Exception as e:
        logger.error(f"System initialization failed: {e}")
        raise


if __name__ == "__main__":
    # This allows the script to be run standalone for testing
    from app.core.database import init_database
    from app.core.config import settings

    db_manager = init_database(settings.database.DB_URL)
    db_manager.create_tables()
    initialize_system(db_manager)
