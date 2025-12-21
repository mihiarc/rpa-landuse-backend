"""FastAPI dependency injection for services."""

import logging
from functools import lru_cache
from typing import Generator

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
