# backend/api/deps.py

from fastapi import Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from db.session import get_db_session
from config import settings
from db.models.user import User
from schemas.auth import TokenData
from utils.logger import get_logger

oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"{settings.API_V1_PREFIX}/auth/login")


async def get_current_user(token: str = Depends(oauth2_scheme), db=Depends(get_db_session)) -> User:
    """Get the current user from the token"""
    credentials_exception = HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Could not validate credentials", headers={"WWW-Authenticate": "Bearer"})

    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
        token_data = TokenData(email=email)
    except JWTError:
        raise credentials_exception

    user = db.query(User).filter(User.email == token_data.email, User.is_active).first()

    if user is None:
        raise credentials_exception

    return user


def get_logger_for_request(request: Request):
    request_id = getattr(request.state, "request_id", "-")
    return get_logger(__name__, request_id=request_id)

async def get_current_superadmin(current_user: User = Depends(get_current_user)) -> User:
    """Check if current user is a superadmin"""
    if not current_user.is_superuser:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only superadmins can perform this operation")
    return current_user
