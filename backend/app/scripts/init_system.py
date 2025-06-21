# backend/app/scripts/init_system.py

from re import I
import uuid

from app.core.security import password_manager
from app.utils.logger import get_logger
from app.repositories.users import UserRepository, UserPreferencesRepository
from app.core.exceptions import ValidationError
from app.utils.enum import Timeframe

logger = get_logger(__name__)


def create_superuser(db_manager, email, password, full_name=None):
    with db_manager.get_session() as db:
        # Check if user already exists
        user_respository = UserRepository(db)
        user_preferences_respository = UserPreferencesRepository(db)
        try:
            user = user_respository.create_user(email=email, hashed_password=password_manager.get_hashed_password(password), full_name=full_name, is_superuser=True)
            logger.success(f"Superuser added successfully")
            user_preferences = user_preferences_respository.create_or_update_preferences(user.id, {"theme": "light", "language": "en", "currency": "INR", "preferred_timeline": Timeframe.DAILY.value, "email_notifications": True, "signal_notifications": True, "portfolio_alerts": True, "system_notifications": True})
            logger.success(f"Superuser preferences successfully")
        except ValidationError as e:
            logger.info(f"User {email} already exists")
        except Exception as e:
            logger.error(f"Error adding super user : {e}")


def initialize_system(db_manager):
    """Initialize the complete system"""
    logger.info("Starting system initialization...")

    create_superuser(db_manager, "admin@quantpulse.app", "password", "System Adminstrator")

    logger.info("System initialization completed successfully.")


if __name__ == "__main__":
    initialize_system()
