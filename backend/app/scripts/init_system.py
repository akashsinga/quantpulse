from app.db.session import get_db
from app.db.models.user import User
from app.services.auth_service import get_password_hash
import uuid
from app.utils.logger import get_logger
from app.scripts.populate_holidays import populate_market_holidays

logger = get_logger(__name__)


def create_superuser(email, password, full_name=None):
    with get_db() as db:
        # Check if user already exists
        existing_user = db.query(User).filter(User.email == email).first()
        if existing_user:
            logger.info(f"User {email} already exists.")
            return

        # Create new superuser
        user = User(id=uuid.uuid4(), email=email, hashed_password=get_password_hash(password), full_name=full_name, is_active=True, is_superuser=True)
        db.add(user)
        db.commit()
        logger.info(f"Superuser {email} created successfully.")


def initialize_system():
    """Initialize the complete system"""
    logger.info("Starting system initialization...")

    # Create superuser
    create_superuser("admin@quantpulse.app", "password", "System Adminstrator")

    # Populate market holidays
    logger.info("Populating market holidays")
    populate_market_holidays()

    logger.info("System initialization completed successfully")


if __name__ == "__main__":
    initialize_system()
