"""Application configuration using Pydantic settings."""

from functools import lru_cache
from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    # API Settings
    api_title: str = "RPA Land Use Analytics API"
    api_version: str = "1.0.0"
    api_description: str = "AI-powered analytics for USDA Forest Service RPA Assessment data"
    debug: bool = False

    # CORS Settings
    cors_origins: List[str] = Field(
        default=["http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:3001", "http://127.0.0.1:3001"],
        description="Allowed CORS origins",
    )

    # OpenAI Settings
    openai_api_key: Optional[str] = Field(default=None, description="OpenAI API key")

    # Database Settings
    database_path: str = Field(
        default="data/processed/landuse_analytics.duckdb",
        alias="LANDUSE_DATABASE__PATH",
    )
    database_read_only: bool = True
    database_max_connections: int = 10
    database_cache_ttl: int = 3600

    # LLM Settings
    llm_model_name: str = Field(default="gpt-4o-mini", alias="LANDUSE_LLM__MODEL_NAME")
    llm_temperature: float = Field(default=0.1, alias="LANDUSE_LLM__TEMPERATURE")
    llm_max_tokens: int = Field(default=4000, alias="LANDUSE_LLM__MAX_TOKENS")

    # Agent Settings
    agent_max_iterations: int = Field(default=8, alias="LANDUSE_AGENT__MAX_ITERATIONS")
    agent_max_execution_time: int = Field(default=120, alias="LANDUSE_AGENT__MAX_EXECUTION_TIME")
    agent_max_query_rows: int = Field(default=1000, alias="LANDUSE_AGENT__MAX_QUERY_ROWS")
    agent_conversation_history_limit: int = Field(
        default=20, alias="LANDUSE_AGENT__CONVERSATION_HISTORY_LIMIT"
    )

    # Rate Limiting
    rate_limit_calls: int = Field(default=60, alias="LANDUSE_SECURITY__RATE_LIMIT_CALLS")
    rate_limit_window: int = Field(default=60, alias="LANDUSE_SECURITY__RATE_LIMIT_WINDOW")

    # Logging
    log_level: str = Field(default="INFO", alias="LANDUSE_LOGGING__LEVEL")

    @property
    def has_openai_key(self) -> bool:
        """Check if OpenAI API key is configured."""
        return bool(self.openai_api_key)


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
