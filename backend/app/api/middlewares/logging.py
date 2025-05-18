# backend/app/api/middlewares/logging.py

import time
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from utils.logger import get_logger

logger = get_logger(__name__)


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        start_time = time.time()
        response = await call_next(request)
        duration = round(time.time() - start_time, 4)
        request_id = getattr(request.state, "request_id", "-")
        logger.info(f"{request.method} {request.url.path} - {response.status_code} - {duration}s - Request ID: {request_id}")
        return response
