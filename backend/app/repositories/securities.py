# backend/app/repositories/securities.py
"""
Data access layer for securities, exchanges and derivatives operations.
Handles all database interactions for market data models.
"""

from tkinter import SE
from typing import Optional, List, Dict, Any
from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
from datetime import date

from app.repositories.base import BaseRepository
from app.models.securities import Security, Exchange
from app.models.derivatives import Future
from app.core.exceptions import NotFoundError, ValidationError
from app.utils.logger import get_logger
from app.utils.enum import SecurityType, SecuritySegment

logger = get_logger(__name__)


class ExchangeRepository(BaseRepository[Exchange]):
    """Repository for Exchange model operations"""

    def __init__(self, db: Session):
        super().__init__(db, Exchange)

    def get_by_code(self, code: str) -> Optional[Exchange]:
        """Get exchange by code"""
        return self.db.query(Exchange).filter(Exchange.code == code.upper(), Exchange.is_deleted == False).first()

    def get_active_by_code(self, code: str) -> Optional[Exchange]:
        """Get Active Exchange by code"""
        return self.db.query(Exchange).filter(Exchange.code == code.upper(), Exchange.is_active, Exchange.is_deleted == False).first()

    def create_exchange(self, name: str, code: str, country: Optional[str] = None, **kwargs) -> Exchange:
        """Create a new exchange"""
        existing_exchange = self.get_by_code(code)

        if existing_exchange:
            return existing_exchange

        exchange_data = {"name": name, "code": code, "country": country, **kwargs}

        exchange = Exchange(**exchange_data)
        return self.create(exchange)

    def activate_exchange(self, exchange_id: UUID) -> Exchange:
        """Activate an exchange"""
        return self.update_by_id(exchange_id, {"is_active": True})

    def deactivate_exchange(self, exchange_id: UUID) -> Exchange:
        """Deactivate an exchange"""
        return self.update_by_id(exchange_id, {"is_active": False})


class SecurityRepository(BaseRepository[Security]):
    """Repository for Security model operations"""

    def __init__(self, db: Session):
        super().__init__(db, Security)

    def get_by_symbol(self, symbol: str, exchange_id: Optional[UUID] = None) -> Optional[Security]:
        """Get security by symbol"""
        query = self.db.query(Security).filter(Security.symbol == symbol.upper(), Security.is_deleted == False)

        if exchange_id:
            query = query.filter(Security.exchange_id == exchange_id)

        return query.first()

    def get_by_external_id(self, external_id: int) -> Optional[Security]:
        """Get security by external ID (Dhan API ID)"""
        return self.db.query(Security).filter(Security.external_id == external_id, Security.is_deleted == False).first()

    def create_security(self, symbol: str, name: str, exchange_id: UUID, external_id: int, security_type: str, segment: str, **kwargs) -> Security:
        """Create a new security"""
        existing_security = self.get_by_symbol(symbol, exchange_id)

        if existing_security:
            return existing_security

        existing_external = self.get_by_external_id(external_id)

        if existing_external:
            return existing_external

        security_data = {"symbol": symbol.upper(), "name": name, "exchange_id": exchange_id, "external_id": external_id, "security_type": security_type, "segment": segment, **kwargs}

        security = Security(**security_data)
        return self.create(security)

    def get_securities_by_exchange(self, exchange_id: UUID, skip: int = 0, limit: int = 100, active_only: bool = True) -> List[Security]:
        """Get securities by exchange"""
        query = self.db.query(Security).filter(Security.exchange_id == exchange_id, Security.is_deleted == False)

        if active_only:
            query = query.filter(Security.is_active == True)

        return query.offset(skip).limit(limit).all()

    def get_securities_by_type(self, security_type: str, skip: int = 0, limit: int = 100, active_only: bool = True) -> List[Security]:
        """Get securities by type"""
        query = self.db.query(Security).filter(Security.security_type == security_type, Security.is_deleted == False)

        if active_only:
            query = query.filter(Security.is_active == True)

        return query.offset(skip).limit(limit).all()

    def get_securities_by_segment(self, segment: str, skip: int = 0, limit: int = 100, active_only: bool = True) -> List[Security]:
        """Get securities by segment"""
        query = self.db.query(Security).filter(Security.segment == segment, Security.is_deleted == False)

        if active_only:
            query = query.filter(Security.is_active == True)

        return query.offset(skip).limit(limit).all()

    def get_securities_by_sector(self, sector: str, skip: int = 0, limit: int = 100, active_only: bool = True) -> List[Security]:
        """Get securities by sector"""
        query = self.db.query(Security).filter(func.lower(Security.sector) == sector.lower(), Security.is_deleted == False)

        if active_only:
            query = query.filter(Security.is_active == True)

        return query.offset(skip).limit(limit).all()

    def search_securities(self, search_term: str, skip: int = 0, limit: int = 100, filters: Optional[Dict[str, Any]] = None) -> tuple[List[Security], int]:
        """
        Search securities by symbol or name with optional filters.
        
        Args:
            search_term: Search term for symbol or name
            skip: Number of securities to skip
            limit: Maximum number of securities to return
            filters: Optional filters (security_type, segment, exchange_id, etc.)
            
        Returns:
            Tuple of (securities, total_count)
        """
        search_pattern = f"%{search_term.lower()}%"

        query = self.db.query(Security).filter(or_(func.lower(Security.symbol).like(search_pattern), func.lower(Security.name).like(search_pattern)), Security.is_deleted == False)

        # Apply filters
        if filters:
            if filters.get("security_type"):
                query = query.filter(Security.security_type == filters["security_type"])
            if filters.get("segment"):
                query = query.filter(Security.segment == filters["segment"])
            if filters.get("exchange_id"):
                query = query.filter(Security.exchange_id == filters["exchange_id"])
            if filters.get("sector"):
                query = query.filter(func.lower(Security.sector) == filters["sector"].lower())
            if filters.get("is_active") is not None:
                query = query.filter(Security.is_active == filters["is_active"])
            if filters.get("is_tradeable") is not None:
                query = query.filter(Security.is_tradeable == filters["is_tradeable"])
            if filters.get("is_derivatives_eligible") is not None:
                query = query.filter(Security.is_derivatives_eligible == filters["is_derivatives_eligible"])

        # Get total count
        total = query.count()

        # Apply pagination and ordering
        securities = query.order_by(Security.symbol).offset(skip).limit(limit).all()

        return securities, total

    def get_derivative_eligible_securities(self, skip: int = 0, limit: int = 100) -> List[Security]:
        """Get securities eligible for derivatives"""
        return self.db.query(Security).filter(Security.is_derivatives_eligible == True, Security.is_active == True, Security.is_deleted == False).offset(skip).limit(limit).all()

    def activate_security(self, security_id: UUID) -> Security:
        """Activate a security"""
        return self.update_by_id(security_id, {"is_active": True})

    def deactivate_security(self, security_id: UUID) -> Security:
        """Deactivate a security"""
        return self.update_by_id(security_id, {"is_active": False})

    def update_trading_status(self, security_id: UUID, is_tradeable: bool) -> Security:
        """Update trading status of a security"""
        return self.update_by_id(security_id, {"is_tradeable": is_tradeable})

    def update_derivatives_eligibility(self, security_id: UUID, is_eligible: bool) -> Security:
        """Update derivatives eligibility of a security"""
        return self.update_by_id(security_id, {"is_derivatives_eligible": is_eligible})


class FutureRepository(BaseRepository[Future]):
    """Repository for Future model operations"""

    def __init__(self, db: Session):
        super().__init__(db, Future)

    def get_by_security_id(self, security_id: UUID) -> Optional[Future]:
        """Get future by security ID"""
        return self.db.query(Future).filter(Future.security_id == security_id, Future.is_deleted == False).first()

    def get_by_underlying(self, underlying_id: UUID, skip: int = 0, limit: int = 100, active_only: bool = True) -> List[Future]:
        """Get futures by underlying security"""
        query = self.db.query(Future).filter(Future.underlying_id == underlying_id, Future.is_deleted == False)

        if active_only:
            query = query.filter(Future.is_active == True)

        return query.offset(skip).limit(limit).all()

    def get_by_contract_month(self, contract_month: str, skip: int = 0, limit: int = 100, active_only: bool = True) -> List[Future]:
        """Get futures by contract month"""
        query = self.db.query(Future).filter(Future.contract_month == contract_month, Future.is_deleted == False)

        if active_only:
            query = query.filter(Future.is_active == True)

        return query.offset(skip).limit(limit).all()

    def create_future(self, security_id: UUID, underlying_id: UUID, expiration_date: date, contract_month: str, **kwargs) -> Future:
        """Create a new future contract"""
        # Check if future already exists for this security
        existing_future = self.get_by_security_id(security_id)
        if existing_future:
            return existing_future

        future_data = {"security_id": security_id, "underlying_id": underlying_id, "expiration_date": expiration_date, "contract_month": contract_month, **kwargs}

        future = Future(**future_data)
        return self.create(future)

    def get_active_futures(self, skip: int = 0, limit: int = 100) -> List[Future]:
        """Get all active futures"""
        return self.db.query(Future).filter(Future.is_active == True, Future.is_deleted == False).offset(skip).limit(limit).all()

    def get_expired_futures(self, skip: int = 0, limit: int = 100) -> List[Future]:
        """Get expired futures"""
        return self.db.query(Future).filter(Future.expiration_date < date.today(), Future.is_deleted == False).offset(skip).limit(limit).all()

    def activate_future(self, future_id: UUID) -> Future:
        """Activate a future contract"""
        future = self.get_by_id_or_raise(future_id)
        return self.update(future, {"is_active": True})

    def deactivate_future(self, future_id: UUID) -> Future:
        """Deactivate a future contract"""
        future = self.get_by_id_or_raise(future_id)
        return self.update(future, {"is_active": False})

    def update_trading_status(self, future_id: UUID, is_tradeable: bool) -> Future:
        """Update trading status of a future"""
        future = self.get_by_id_or_raise(future_id)
        return self.update(future, {"is_tradeable": is_tradeable})

    def link_rollover_contracts(self, current_contract_id: UUID, next_contract_id: UUID) -> Future:
        """Link current contract to next contract for rollover"""
        current_contract = self.get_by_id_or_raise(current_contract_id)
        next_contract = self.get_by_id_or_raise(next_contract_id)

        # Update current contract with next contract reference
        current_contract = self.update(current_contract, {"next_contract_id": next_contract_id})

        # Update next contract with previous contract reference
        self.update(next_contract, {"previous_contract_id": current_contract_id})

        return current_contract

    def get_futures_chain(self, underlying_id: UUID) -> List[Future]:
        """Get complete futures chain for an underlying"""
        return self.db.query(Future).filter(Future.underlying_id == underlying_id, Future.is_deleted == False).order_by(Future.expiration_date).all()

    def search_futures(self, search_term: str, skip: int = 0, limit: int = 100, filters: Optional[Dict[str, Any]] = None) -> tuple[List[Future], int]:
        """
        Search futures with optional filters.
        
        Args:
            search_term: Search term for underlying symbol
            skip: Number of futures to skip
            limit: Maximum number of futures to return
            filters: Optional filters (contract_month, settlement_type, etc.)
            
        Returns:
            Tuple of (futures, total_count)
        """
        search_pattern = f"%{search_term.lower()}%"

        # Join with underlying security for search
        query = self.db.query(Future).join(Security, Future.underlying_id == Security.id).filter(or_(func.lower(Security.symbol).like(search_pattern), func.lower(Security.name).like(search_pattern)), Future.is_deleted == False)

        # Apply filters
        if filters:
            if filters.get("contract_month"):
                query = query.filter(Future.contract_month == filters["contract_month"])
            if filters.get("settlement_type"):
                query = query.filter(Future.settlement_type == filters["settlement_type"])
            if filters.get("is_active") is not None:
                query = query.filter(Future.is_active == filters["is_active"])
            if filters.get("is_tradeable") is not None:
                query = query.filter(Future.is_tradeable == filters["is_tradeable"])
            if filters.get("expiry_from"):
                query = query.filter(Future.expiration_date >= filters["expiry_from"])
            if filters.get("expiry_to"):
                query = query.filter(Future.expiration_date <= filters["expiry_to"])

        # Get total count
        total = query.count()

        # Apply pagination and ordering
        futures = query.order_by(Future.expiration_date).offset(skip).limit(limit).all()

        return futures, total
