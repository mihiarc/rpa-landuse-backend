"""Request models for API endpoints."""

from typing import List, Optional

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    """Request model for chat queries."""

    question: str = Field(..., min_length=1, max_length=2000, description="Natural language question")
    session_id: Optional[str] = Field(default=None, description="Session ID for conversation continuity")


class SqlQueryRequest(BaseModel):
    """Request model for SQL query execution."""

    query: str = Field(..., min_length=1, max_length=10000, description="SQL query to execute")
    limit: int = Field(default=100, ge=1, le=10000, description="Maximum rows to return")


class ExtractionRequest(BaseModel):
    """Request model for data extraction."""

    template_id: Optional[str] = Field(default=None, description="Predefined extraction template ID")
    custom_query: Optional[str] = Field(default=None, max_length=10000, description="Custom SQL query")
    scenarios: Optional[List[str]] = Field(default=None, description="Climate scenarios to filter")
    states: Optional[List[str]] = Field(default=None, description="States to filter")
    land_use_types: Optional[List[str]] = Field(default=None, description="Land use types to filter")
    land_use_from: Optional[List[str]] = Field(default=None, description="Source land use types")
    land_use_to: Optional[List[str]] = Field(default=None, description="Target land use types")
    time_periods: Optional[List[int]] = Field(default=None, description="Time periods (years) to include")
    format: str = Field(default="csv", pattern="^(csv|json|parquet|excel)$", description="Export format")
    limit: int = Field(default=10000, ge=1, le=5000000, description="Maximum rows to export")


class AnalyticsRequest(BaseModel):
    """Request model for analytics queries."""

    analysis_type: str = Field(..., description="Type of analysis to perform")
    scenarios: Optional[List[str]] = Field(default=None)
    states: Optional[List[str]] = Field(default=None)
    time_periods: Optional[List[str]] = Field(default=None)
