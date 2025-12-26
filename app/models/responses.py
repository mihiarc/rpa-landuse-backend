"""Response models for API endpoints."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    """Response model for health check endpoint."""

    status: str = Field(..., description="Overall health status")
    database: Dict[str, Any] = Field(..., description="Database connection status")
    llm: Dict[str, Any] = Field(..., description="LLM API status (Anthropic/OpenAI)")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ChatResponse(BaseModel):
    """Response model for chat queries."""

    success: bool = Field(..., description="Whether the query succeeded")
    response: str = Field(..., description="Natural language response")
    sql_query: Optional[str] = Field(default=None, description="SQL query executed (if any)")
    execution_time: float = Field(..., description="Query execution time in seconds")
    session_id: Optional[str] = Field(default=None, description="Session ID for continuity")


class StreamChunk(BaseModel):
    """Model for streaming response chunks."""

    type: str = Field(..., description="Chunk type: start, content, sql, complete, error")
    content: Optional[str] = Field(default=None, description="Chunk content")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional metadata")


class SchemaResponse(BaseModel):
    """Response model for database schema."""

    tables: List[Dict[str, Any]] = Field(..., description="List of tables with their schemas")
    views: List[Dict[str, Any]] = Field(..., description="List of views")
    total_rows: int = Field(..., description="Total rows in fact tables")


class QueryResultResponse(BaseModel):
    """Response model for SQL query results."""

    success: bool = Field(..., description="Whether the query succeeded")
    columns: List[str] = Field(default_factory=list, description="Column names")
    data: List[Dict[str, Any]] = Field(default_factory=list, description="Query result rows")
    row_count: int = Field(default=0, description="Number of rows returned")
    execution_time: float = Field(..., description="Query execution time in seconds")
    error: Optional[str] = Field(default=None, description="Error message if failed")
    suggestion: Optional[str] = Field(default=None, description="Suggestion for fixing errors")


class AnalyticsResponse(BaseModel):
    """Response model for analytics data."""

    data: List[Dict[str, Any]] = Field(..., description="Analytics data")
    summary: Optional[Dict[str, Any]] = Field(default=None, description="Summary statistics")
    chart_config: Optional[Dict[str, Any]] = Field(default=None, description="Chart configuration hints")


class ExtractionResponse(BaseModel):
    """Response model for data extraction."""

    success: bool = Field(..., description="Whether extraction succeeded")
    row_count: int = Field(..., description="Number of rows extracted")
    columns: Optional[List[str]] = Field(default=None, description="Column names")
    file_size: Optional[int] = Field(default=None, description="File size in bytes")
    download_url: Optional[str] = Field(default=None, description="URL to download the file")
    preview: Optional[List[Dict[str, Any]]] = Field(default=None, description="Preview of first rows")
    error: Optional[str] = Field(default=None, description="Error message if failed")
