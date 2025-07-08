# backend/app/api/v1/exchanges.py
"""Exchanges Router"""

from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.api.dependencies import get_current_superuser
from app.repositories.securities import ExchangeRepository
from app.schemas.base import APIResponse
from app.schemas.security import ExchangeResponse
from app.utils.logger import get_logger

router = APIRouter()
logger = get_logger(__name__)


@router.get("", response_model=APIResponse[List[ExchangeResponse]])
async def get_exchanges(active_only: bool = True, db: Session = Depends(get_db), current_user=Depends(get_current_superuser)):
    """Get list of all exchanges."""
    try:
        exchange_repo = ExchangeRepository(db)

        if active_only:
            exchanges = exchange_repo.get_many_by_field("is_active", True)
        else:
            exchanges = exchange_repo.get_all()

        exchange_responses = [ExchangeResponse.model_validate(exchange) for exchange in exchanges]

        return APIResponse(data=exchange_responses, message="Exchanges retrieved successfully")

    except Exception as e:
        logger.error(f"Error getting exchanges: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve exchanges")
