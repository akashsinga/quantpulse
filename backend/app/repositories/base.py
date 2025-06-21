# backend/app/repositories/base.py
"""
Base repository pattern for QuantPulse application.
Provides common CRUD operations and patterns for all repositories.
"""

from typing import TypeVar, Generic, Type, Optional, List, Dict, Any, Union
from uuid import UUID
from sqlalchemy.orm import Session

from app.models.base import BaseModel
from app.core.exceptions import NotFoundError, DatabaseError
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Type variable for model classes
ModelType = TypeVar("ModelType", bound=BaseModel)


class BaseRepository(Generic[ModelType]):
    """
    Base repository providing common CRUD operations.
    All model repositories should inherit from this class.
    """

    def __init__(self, db: Session, model: Type[ModelType]):
        """
        Initialize repository.
        Args:
            db: Database session
            model: SQLAlchemy model class
        """
        self.db = db
        self.model = model

    def get_by_id(self, id: UUID) -> Optional[ModelType]:
        """
        Get model instance by ID.
        Args:
            id: Model ID
        Returns:
            Model instance or None if not found
        """
        try:
            return self.db.query(self.model).filter(self.model.id == id, self.model.is_deleted == False).first()
        except Exception as e:
            logger.error(f"Error getting {self.model.__name__} by ID {id}: {e}")
            raise DatabaseError(f"get_{self.model.__name__.lower()}_by_id", str(e))

    def get_by_id_or_raise(self, id: UUID) -> ModelType:
        """
        Get model instance by ID or raise exception.
        Args:
            id: Model ID
        Returns:
            Model instance
        Raises:
            NotFoundError: If model not found
        """
        instance = self.get_by_id(id)
        if not instance:
            raise NotFoundError(self.model.__name__, str(id))
        return instance

    def get_all(self, skip: int = 0, limit: int = 100, include_deleted: bool = False) -> List[ModelType]:
        """
        Get all model instances with pagination.
        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return
            include_deleted: Whether to include soft-deleted records
        Returns:
            List of model instances
        """
        try:
            query = self.db.query(self.model)

            if not include_deleted:
                query = query.filter(self.model.is_deleted == False)

            return query.offset(skip).limit(limit).all()
        except Exception as e:
            logger.error(f"Error getting all {self.model.__name__}: {e}")
            raise DatabaseError(f"get_all_{self.model.__name__.lower()}", str(e))

    def count(self, include_deleted: bool = False) -> int:
        """
        Count total number of records.
        Args:
            include_deleted: Whether to include soft-deleted records
        Returns:
            Total count
        """
        try:
            query = self.db.query(self.model)

            if not include_deleted:
                query = query.filter(self.model.is_deleted == False)

            return query.count()
        except Exception as e:
            logger.error(f"Error counting {self.model.__name__}: {e}")
            raise DatabaseError(f"count_{self.model.__name__.lower()}", str(e))

    def create(self, obj: ModelType) -> ModelType:
        """
        Create a new model instance.
        Args:
            obj: Model instance to create
        Returns:
            Created model instance
        """
        try:
            self.db.add(obj)
            self.db.commit()
            self.db.refresh(obj)
            logger.info(f"Created {self.model.__name__} with ID: {obj.id}")
            return obj
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error creating {self.model.__name__}: {e}")
            raise DatabaseError(f"create_{self.model.__name__.lower()}", str(e))

    def create_bulk(self, objects: List[ModelType]) -> List[ModelType]:
        """
        Create multiple model instances in bulk.
        Args:
            objects: List of model instances to create
        Returns:
            List of created model instances
        """
        try:
            self.db.add_all(objects)
            self.db.commit()

            # Refresh all objects to get their IDs
            for obj in objects:
                self.db.refresh(obj)

            logger.info(f"Created {len(objects)} {self.model.__name__} instances")
            return objects
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error bulk creating {self.model.__name__}: {e}")
            raise DatabaseError(f"create_bulk_{self.model.__name__.lower()}", str(e))

    def update(self, obj: ModelType, update_data: Dict[str, Any]) -> ModelType:
        """
        Update a model instance.
        Args:
            obj: Model instance to update
            update_data: Dictionary of fields to update
        Returns:
            Updated model instance
        """
        try:
            # Update fields
            for field, value in update_data.items():
                if hasattr(obj, field):
                    setattr(obj, field, value)

            self.db.commit()
            self.db.refresh(obj)
            logger.info(f"Updated {self.model.__name__} with ID: {obj.id}")
            return obj
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error updating {self.model.__name__} {obj.id}: {e}")
            raise DatabaseError(f"update_{self.model.__name__.lower()}", str(e))

    def update_by_id(self, id: UUID, update_data: Dict[str, Any]) -> ModelType:
        """
        Update a model instance by ID.
        Args:
            id: Model ID
            update_data: Dictionary of fields to update
        Returns:
            Updated model instance
        Raises:
            NotFoundError: If model not found
        """
        obj = self.get_by_id_or_raise(id)
        return self.update(obj, update_data)

    def delete(self, obj: ModelType, soft_delete: bool = True) -> bool:
        """
        Delete a model instance.
        Args:
            obj: Model instance to delete
            soft_delete: Whether to use soft delete (default) or hard delete
        Returns:
            True if deleted successfully
        """
        try:
            if soft_delete and hasattr(obj, 'soft_delete'):
                obj.soft_delete()
                self.db.commit()
                logger.info(f"Soft deleted {self.model.__name__} with ID: {obj.id}")
            else:
                self.db.delete(obj)
                self.db.commit()
                logger.info(f"Hard deleted {self.model.__name__} with ID: {obj.id}")

            return True
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error deleting {self.model.__name__} {obj.id}: {e}")
            raise DatabaseError(f"delete_{self.model.__name__.lower()}", str(e))

    def delete_by_id(self, id: UUID, soft_delete: bool = True) -> bool:
        """
        Delete a model instance by ID.
        Args:
            id: Model ID
            soft_delete: Whether to use soft delete (default) or hard delete
        Returns:
            True if deleted successfully
        Raises:
            NotFoundError: If model not found
        """
        obj = self.get_by_id_or_raise(id)
        return self.delete(obj, soft_delete)

    def restore(self, obj: ModelType) -> ModelType:
        """
        Restore a soft-deleted model instance.
        Args:
            obj: Model instance to restore
        Returns:
            Restored model instance
        """
        try:
            if hasattr(obj, 'restore'):
                obj.restore()
                self.db.commit()
                self.db.refresh(obj)
                logger.info(f"Restored {self.model.__name__} with ID: {obj.id}")
                return obj
            else:
                raise ValueError(f"{self.model.__name__} does not support soft delete/restore")
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error restoring {self.model.__name__} {obj.id}: {e}")
            raise DatabaseError(f"restore_{self.model.__name__.lower()}", str(e))

    def exists(self, id: UUID) -> bool:
        """
        Check if a model instance exists.
        Args:
            id: Model ID
        Returns:
            True if exists, False otherwise
        """
        try:
            return self.db.query(self.model).filter(self.model.id == id, self.model.is_deleted == False).first() is not None
        except Exception as e:
            logger.error(f"Error checking existence of {self.model.__name__} {id}: {e}")
            return False

    def get_by_field(self, field_name: str, value: Any) -> Optional[ModelType]:
        """
        Get model instance by a specific field.
        Args:
            field_name: Name of the field
            value: Value to search for
        Returns:
            Model instance or None if not found
        """
        try:
            if not hasattr(self.model, field_name):
                raise ValueError(f"{self.model.__name__} has no field '{field_name}'")

            field = getattr(self.model, field_name)
            return self.db.query(self.model).filter(field == value, self.model.is_deleted == False).first()
        except Exception as e:
            logger.error(f"Error getting {self.model.__name__} by {field_name}: {e}")
            raise DatabaseError(f"get_{self.model.__name__.lower()}_by_{field_name}", str(e))

    def get_many_by_field(self, field_name: str, value: Any, skip: int = 0, limit: int = 100) -> List[ModelType]:
        """
        Get multiple model instances by a specific field.
        Args:
            field_name: Name of the field
            value: Value to search for
            skip: Number of records to skip
            limit: Maximum number of records to return
        Returns:
            List of model instances
        """
        try:
            if not hasattr(self.model, field_name):
                raise ValueError(f"{self.model.__name__} has no field '{field_name}'")

            field = getattr(self.model, field_name)
            return self.db.query(self.model).filter(field == value, self.model.is_deleted == False).offset(skip).limit(limit).all()
        except Exception as e:
            logger.error(f"Error getting {self.model.__name__} by {field_name}: {e}")
            raise DatabaseError(f"get_many_{self.model.__name__.lower()}_by_{field_name}", str(e))

    def get_by_fields(self, filters: Dict[str, Any]) -> Optional[ModelType]:
        """
        Get model instance by multiple fields.
        Args:
            filters: Dictionary of field names and values
        Returns:
            Model instance or None if not found
        """
        try:
            query = self.db.query(self.model).filter(self.model.is_deleted == False)

            for field_name, value in filters.items():
                if not hasattr(self.model, field_name):
                    raise ValueError(f"{self.model.__name__} has no field '{field_name}'")

                field = getattr(self.model, field_name)
                query = query.filter(field == value)

            return query.first()
        except Exception as e:
            logger.error(f"Error getting {self.model.__name__} by multiple fields: {e}")
            raise DatabaseError(f"get_{self.model.__name__.lower()}_by_fields", str(e))

    def get_ordered_by(self, order_by: str, desc: bool = False, skip: int = 0, limit: int = 100) -> List[ModelType]:
        """
        Get model instances ordered by a specific field.
        Args:
            order_by: Field name to order by
            desc: Whether to order in descending order
            skip: Number of records to skip
            limit: Maximum number of records to return
        Returns:
            List of ordered model instances
        """
        try:
            if not hasattr(self.model, order_by):
                raise ValueError(f"{self.model.__name__} has no field '{order_by}'")

            field = getattr(self.model, order_by)
            query = self.db.query(self.model).filter(self.model.is_deleted == False)

            if desc:
                query = query.order_by(field.desc())
            else:
                query = query.order_by(field.asc())

            return query.offset(skip).limit(limit).all()
        except Exception as e:
            logger.error(f"Error getting ordered {self.model.__name__}: {e}")
            raise DatabaseError(f"get_ordered_{self.model.__name__.lower()}", str(e))

    def commit(self):
        """Commit the current transaction."""
        try:
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error committing transaction: {e}")
            raise DatabaseError("commit_transaction", str(e))

    def rollback(self):
        """Rollback the current transaction."""
        try:
            self.db.rollback()
        except Exception as e:
            logger.error(f"Error rolling back transaction: {e}")
            raise DatabaseError("rollback_transaction", str(e))

    def refresh(self, obj: ModelType) -> ModelType:
        """
        Refresh model instance from database.
        Args:
            obj: Model instance to refresh
        Returns:
            Refreshed model instance
        """
        try:
            self.db.refresh(obj)
            return obj
        except Exception as e:
            logger.error(f"Error refreshing {self.model.__name__} {obj.id}: {e}")
            raise DatabaseError(f"refresh_{self.model.__name__.lower()}", str(e))
