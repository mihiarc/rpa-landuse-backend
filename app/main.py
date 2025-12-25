"""FastAPI application entry point for RPA Land Use Analytics."""

import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1 import health, chat, analytics, explorer, extraction, auth, citation
from app.config import get_settings
from app.dependencies import cleanup_services

# Add parent directory to path for landuse imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

settings = get_settings()

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager for startup and shutdown events."""
    logger.info("Starting RPA Land Use Analytics API...")

    # Startup: Validate configuration
    if not settings.has_openai_key:
        logger.warning("OpenAI API key not configured - chat functionality will be limited")

    if settings.auth_enabled:
        logger.info("Authentication enabled")
    else:
        logger.warning("Authentication not configured - API is publicly accessible")

    # Check database configuration
    db_path = settings.database_path
    if db_path.startswith("md:"):
        # MotherDuck cloud database
        import os
        if os.environ.get("motherduck_token"):
            logger.info(f"MotherDuck database configured: {db_path}")
        else:
            logger.warning("MotherDuck token not configured - database features may not work")
    else:
        # Local file database
        if not Path(db_path).exists():
            logger.warning(f"Database not found at {db_path} - some features may not work")
        else:
            logger.info(f"Database found at {db_path}")

    yield

    # Shutdown: Cleanup services
    logger.info("Shutting down RPA Land Use Analytics API...")
    cleanup_services()


# Create FastAPI application
app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description=settings.api_description,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(health.router, prefix="/api/v1", tags=["health"])
app.include_router(auth.router, prefix="/api/v1", tags=["auth"])
app.include_router(chat.router, prefix="/api/v1", tags=["chat"])
app.include_router(analytics.router, prefix="/api/v1", tags=["analytics"])
app.include_router(explorer.router, prefix="/api/v1", tags=["explorer"])
app.include_router(extraction.router, prefix="/api/v1", tags=["extraction"])
app.include_router(citation.router, prefix="/api/v1", tags=["citation"])


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": settings.api_title,
        "version": settings.api_version,
        "description": settings.api_description,
        "docs": "/docs",
        "health": "/api/v1/health",
    }
