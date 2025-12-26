"""Health check endpoints."""

import logging
import os
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter

from app.config import get_settings
from app.models.responses import HealthResponse

router = APIRouter()
logger = logging.getLogger(__name__)
settings = get_settings()


def check_database_status() -> dict:
    """Check database availability for both local files and MotherDuck."""
    db_path = settings.database_path

    if db_path.startswith("md:"):
        # MotherDuck cloud database
        has_token = bool(os.environ.get("motherduck_token"))
        return {
            "connected": has_token,
            "path": db_path,
            "size_mb": 0,  # Size not available for cloud databases
            "message": "MotherDuck configured" if has_token else "MotherDuck token not set",
        }
    else:
        # Local file database
        local_path = Path(db_path)
        db_exists = local_path.exists()
        db_size = local_path.stat().st_size if db_exists else 0
        return {
            "connected": db_exists,
            "path": str(local_path),
            "size_mb": round(db_size / (1024 * 1024), 2) if db_exists else 0,
            "message": "Database available" if db_exists else "Database not found",
        }


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Check the health status of all system components."""
    # Check database
    database_status = check_database_status()
    db_connected = database_status["connected"]

    # Check LLM API key (Anthropic preferred, OpenAI fallback)
    has_llm_key = settings.has_anthropic_key or settings.has_openai_key

    if settings.has_anthropic_key:
        llm_provider = "Anthropic"
        llm_model = "claude-sonnet-4-20250514"
    elif settings.has_openai_key:
        llm_provider = "OpenAI"
        llm_model = settings.llm_model_name
    else:
        llm_provider = "None"
        llm_model = "not configured"

    llm_status = {
        "configured": has_llm_key,
        "model": llm_model,
        "provider": llm_provider,
        "message": f"{llm_provider} API key configured" if has_llm_key else "No LLM API key set",
    }

    # Determine overall status
    if db_connected and has_llm_key:
        overall_status = "healthy"
    elif db_connected:
        overall_status = "degraded"
    else:
        overall_status = "unhealthy"

    return HealthResponse(
        status=overall_status,
        database=database_status,
        openai=llm_status,  # Keep field name for backward compatibility
        timestamp=datetime.utcnow(),
    )


@router.get("/health/ready")
async def readiness_check():
    """Kubernetes-style readiness probe."""
    db_status = check_database_status()
    if not db_status["connected"]:
        return {"ready": False, "reason": "Database not available"}
    return {"ready": True}


@router.get("/health/live")
async def liveness_check():
    """Kubernetes-style liveness probe."""
    return {"alive": True}
