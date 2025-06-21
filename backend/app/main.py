# backend/app/main.py

import uvicorn

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.api.middlewares import RequestMiddleware

from app.core.config import settings
from app.core.database import init_database
from app.utils.logger import get_logger

# Router Imports
from app.api.v1 import auth

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting {settings.app.APP_NAME} Server")

    db_manager = init_database(settings.database.DB_URL)

    if settings.ENVIRONMENT == "development":
        db_manager.create_tables()
        logger.info("Database tables created (Dev)")

    yield

    logger.info(f"{settings.app.APP_NAME} Started")

    logger.info(f"Shutting down {settings.app.APP_NAME} Server")


app = FastAPI(title=settings.app.APP_NAME, description="Predictive Stock Analytics", version="1.0.0", lifespan=lifespan)

# Middlewares
app.add_middleware(CORSMiddleware, allow_origins=settings.api.CORS_ORIGINS, allow_credentials=True, allow_methods=["*"], allow_headers=["*"], expose_headers=["Content-Type", "Authorization"], max_age=86400)
app.add_middleware(RequestMiddleware)

# Routers
app.include_router(auth.router, prefix=f"{settings.api.API_V1_PREFIX}/auth", tags=["auth"])


@app.get("/", tags=["root"])
async def root():
    """Root endpoint to check API status"""
    return {"status": "online", "api_version": "1.0.0", "system_name": settings.app.APP_NAME, "documentation": "/docs"}


if __name__ == "__main__":
    uvicorn.run("backend.app.main:app", host="0.0.0.0", port=8000, reload=True)
