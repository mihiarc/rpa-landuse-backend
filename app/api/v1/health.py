"""Health check endpoints."""

import logging
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter

from app.config import get_settings
from app.models.responses import HealthResponse

router = APIRouter()
logger = logging.getLogger(__name__)
settings = get_settings()


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Check the health status of all system components."""
    # Check database
    db_path = Path(settings.database_path)
    db_exists = db_path.exists()
    db_size = db_path.stat().st_size if db_exists else 0

    database_status = {
        "connected": db_exists,
        "path": str(db_path),
        "size_mb": round(db_size / (1024 * 1024), 2) if db_exists else 0,
        "message": "Database available" if db_exists else "Database not found",
    }

    # Check OpenAI API key
    openai_status = {
        "configured": settings.has_openai_key,
        "model": settings.llm_model_name,
        "message": "API key configured" if settings.has_openai_key else "API key not set",
    }

    # Determine overall status
    if db_exists and settings.has_openai_key:
        overall_status = "healthy"
    elif db_exists:
        overall_status = "degraded"
    else:
        overall_status = "unhealthy"

    return HealthResponse(
        status=overall_status,
        database=database_status,
        openai=openai_status,
        timestamp=datetime.utcnow(),
    )


@router.get("/health/ready")
async def readiness_check():
    """Kubernetes-style readiness probe."""
    db_path = Path(settings.database_path)
    if not db_path.exists():
        return {"ready": False, "reason": "Database not available"}
    return {"ready": True}


@router.get("/health/live")
async def liveness_check():
    """Kubernetes-style liveness probe."""
    return {"alive": True}
