# backend/app/api/main.py

import uvicorn

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.api.middlewares.logging import LoggingMiddleware
from app.api.middlewares.request import RequestMiddleware

from app.config import settings
from app.api.v1 import auth
from app.api.v1 import securities
from app.api.v1 import ohlcv

import os
import sys

from app.utils.logger import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize features, connections
    logger.info(f"Starting {settings.APP_NAME} Server")

    logger.info(f"{settings.APP_NAME} Started")

    yield

    # Shutdown code
    logger.info(f"Shutting down {settings.APP_NAME} Server")
    # Shutdown worker pools
    logger.info("Shutting down worker pools")


app = FastAPI(title=settings.APP_NAME, description="Predictive Market Analytics API", version="1.0.0", lifespan=lifespan)

# Adding Middlewares
app.add_middleware(CORSMiddleware, allow_origins=settings.CORS_ORIGINS, allow_credentials=True, allow_methods=["*"], allow_headers=["*"], expose_headers=["Content-Type", "Authorization"], max_age=86400)
app.add_middleware(LoggingMiddleware)
app.add_middleware(RequestMiddleware)

# Routers
app.include_router(auth.router, prefix=f"{settings.API_V1_PREFIX}/auth", tags=["auth"])
app.include_router(securities.router, prefix=f"{settings.API_V1_PREFIX}/securities", tags=["securities"])
app.include_router(ohlcv.router, prefix=f"{settings.API_V1_PREFIX}/ohlcv", tags=["ohlcv"])


@app.get("/", tags=["root"])
async def root():
    """Root endpoint to checkAPI status"""
    return {"status": "online", "api_version": "1.0.0", "system_name": settings.APP_NAME, "documentation": "/docs"}


if __name__ == "__main__":
    uvicorn.run("backend.api.main:app", host="0.0.0.0", port=8000, reload=True)
