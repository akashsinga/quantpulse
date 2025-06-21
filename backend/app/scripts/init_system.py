# backend/app/scripts/init_system.py

from re import I
import uuid

from app.core.database import get_db
from app.models.users import User
from app.core.security import password_manager
from app.utils.logger import get_logger
from app.repositories.users import UserRepository

logger = get_logger(__name__)


def create_superuser(email, password, full_name=None):
    with get_db() as db:
        # Check if user already exists
        user_respository = UserRepository(db)
        try:
            user_respository.create_user(email=email, hashed_password=password_manager.get_hashed_password(password), full_name=full_name, is_superuser=True)
            logger.success(f"Superuser added succesfully")
        except Exception as e:
            logger.info(f"User {email} already exists")


def initialize_system():
    """Initialize the complete system"""
    logger.info("Starting system initialization...")

    create_superuser("admin@quantpulse.app", "password", "System Adminstrator")

    logger.info("System initialization completed successfully.")


if __name__ == "__main__":
    initialize_system()
