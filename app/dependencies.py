"""FastAPI dependency injection for services."""

import logging
from functools import lru_cache
from typing import Generator, Optional

from fastapi import Cookie, HTTPException, status

from app.config import Settings
from app.services.agent_service import AgentService
from app.services.database_service import DatabaseService

logger = logging.getLogger(__name__)


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Singleton instances
_agent_service: AgentService | None = None
_database_service: DatabaseService | None = None


def get_agent_service() -> AgentService:
    """
    Get or create the AgentService singleton.

    The agent service is expensive to initialize (loads LLM),
    so we reuse a single instance.
    """
    global _agent_service

    if _agent_service is None:
        settings = get_settings()
        _agent_service = AgentService(database_path=settings.database_path)
        logger.info("AgentService singleton created")

    return _agent_service


def get_database_service() -> Generator[DatabaseService, None, None]:
    """
    Get DatabaseService instance.

    Yields a database service and ensures cleanup on request completion.
    """
    settings = get_settings()
    service = DatabaseService(
        database_path=settings.database_path,
        read_only=True
    )
    try:
        yield service
    finally:
        service.close()


def get_database_service_singleton() -> DatabaseService:
    """
    Get or create the DatabaseService singleton.

    For endpoints that need persistent connection (analytics).
    """
    global _database_service

    if _database_service is None:
        settings = get_settings()
        _database_service = DatabaseService(
            database_path=settings.database_path,
            read_only=True
        )
        logger.info("DatabaseService singleton created")

    return _database_service


def cleanup_services():
    """Clean up all singleton services."""
    global _agent_service, _database_service

    if _agent_service:
        _agent_service.cleanup()
        _agent_service = None
        logger.info("AgentService cleaned up")

    if _database_service:
        _database_service.close()
        _database_service = None
        logger.info("DatabaseService cleaned up")


async def require_auth(
    access_token: Optional[str] = Cookie(default=None),
) -> None:
    """
    Dependency that requires valid authentication.

    If auth is not configured, allows all requests.
    If auth is configured, requires valid access token.
    """
    from app.api.v1.auth import verify_token

    settings = get_settings()

    if not settings.auth_enabled:
        return

    if not access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )

    if not verify_token(access_token, "access"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )


class AcademicUserInfo:
    """Information about the current academic user."""

    def __init__(
        self,
        email: str,
        tier: str,
        queries_remaining: int,
        daily_limit: int,
    ):
        self.email = email
        self.tier = tier
        self.queries_remaining = queries_remaining
        self.daily_limit = daily_limit

    @property
    def is_academic(self) -> bool:
        """Check if user is on academic tier."""
        return self.tier == "academic"

    @property
    def has_quota(self) -> bool:
        """Check if user has remaining quota (always True for non-academic)."""
        if not self.is_academic:
            return True
        return self.queries_remaining > 0


async def get_academic_user(
    access_token: Optional[str] = Cookie(default=None),
) -> AcademicUserInfo:
    """
    Get academic user info and validate quota.

    This dependency extracts user info from the JWT token and checks
    the current query quota for academic users.

    For non-academic users (admin login), returns unlimited quota.
    For academic users, checks and returns remaining daily quota.

    Raises:
        HTTPException 401: If not authenticated
        HTTPException 429: If academic user has exceeded daily quota
    """
    from app.api.v1.auth import decode_token, get_academic_service, verify_token

    settings = get_settings()

    # If auth is completely disabled, return unlimited access
    if not settings.auth_enabled and not settings.academic_tier_enabled:
        return AcademicUserInfo(
            email="anonymous",
            tier="unlimited",
            queries_remaining=999999,
            daily_limit=0,
        )

    # Check for valid token
    if not access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )

    if not verify_token(access_token, "access"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )

    # Decode token to get user info
    payload = decode_token(access_token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )

    email = payload.get("email", "")
    tier = payload.get("tier", "admin")

    # Non-academic users (admin login) have unlimited access
    if tier != "academic":
        return AcademicUserInfo(
            email=email or "admin",
            tier=tier,
            queries_remaining=999999,
            daily_limit=0,
        )

    # For academic users, check quota
    academic_service = get_academic_service()
    queries_remaining = academic_service.get_queries_remaining(email)

    if queries_remaining <= 0:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Daily query limit ({settings.academic_daily_query_limit}) exceeded. "
            "Your quota resets at midnight UTC.",
        )

    return AcademicUserInfo(
        email=email,
        tier=tier,
        queries_remaining=queries_remaining,
        daily_limit=settings.academic_daily_query_limit,
    )


def increment_academic_usage(email: str) -> None:
    """
    Increment query usage for an academic user.

    Call this after a successful query to track usage.
    """
    from app.api.v1.auth import get_academic_service

    academic_service = get_academic_service()
    academic_service.increment_usage(email)
