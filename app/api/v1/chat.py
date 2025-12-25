"""Chat API endpoints for natural language queries."""

import json
import logging
from typing import AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from app.config import get_settings
from app.dependencies import (
    AcademicUserInfo,
    get_academic_user,
    get_agent_service,
    increment_academic_usage,
    require_auth,
)
from app.models.requests import ChatRequest
from app.models.responses import ChatResponse, StreamChunk
from app.services.agent_service import AgentService

router = APIRouter(prefix="/chat", dependencies=[Depends(require_auth)])
logger = logging.getLogger(__name__)
settings = get_settings()


@router.post("/query", response_model=ChatResponse)
async def query(
    request: ChatRequest,
    agent_service: AgentService = Depends(get_agent_service),
    academic_user: AcademicUserInfo = Depends(get_academic_user),
) -> ChatResponse:
    """
    Execute a natural language query against the land use database.

    Uses the LandUseAgent with Claude to answer questions about
    land use projections and transitions.

    Rate limited for academic users (50 queries/day by default).
    """
    if not settings.has_anthropic_key:
        raise HTTPException(
            status_code=503,
            detail="Anthropic API key not configured. Please set ANTHROPIC_API_KEY environment variable.",
        )

    try:
        response = await agent_service.query(
            question=request.question,
            session_id=request.session_id,
        )

        # Increment usage for academic users after successful query
        if academic_user.is_academic:
            increment_academic_usage(academic_user.email)
            logger.info(
                f"Academic user {academic_user.email} used query "
                f"({academic_user.queries_remaining - 1} remaining)"
            )

        return ChatResponse(
            success=True,
            response=response.content,
            sql_query=response.sql_query,
            execution_time=response.execution_time,
            session_id=request.session_id,
        )

    except Exception as e:
        logger.exception(f"Error processing chat query: {request.question[:50]}...")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stream")
async def stream_query(
    request: ChatRequest,
    agent_service: AgentService = Depends(get_agent_service),
    academic_user: AcademicUserInfo = Depends(get_academic_user),
):
    """
    Stream response for natural language query using Server-Sent Events (SSE).

    Provides real-time response streaming for a more interactive experience.
    """
    if not settings.has_anthropic_key:
        raise HTTPException(
            status_code=503,
            detail="Anthropic API key not configured.",
        )

    async def generate() -> AsyncGenerator[str, None]:
        """Generate SSE events from agent streaming response."""
        try:
            # Send start event
            yield f"data: {json.dumps({'type': 'start', 'session_id': request.session_id})}\n\n"

            # Stream from agent
            async for chunk in agent_service.stream_query(
                question=request.question,
                session_id=request.session_id,
            ):
                if chunk.type == "error":
                    error_data = {"type": "error", "content": chunk.content}
                    yield f"data: {json.dumps(error_data)}\n\n"
                    break

                elif chunk.type == "content":
                    chunk_data = StreamChunk(type="content", content=chunk.content)
                    yield f"data: {chunk_data.model_dump_json()}\n\n"

                elif chunk.type == "complete":
                    # Increment usage for academic users after successful completion
                    if academic_user.is_academic:
                        increment_academic_usage(academic_user.email)
                        logger.info(
                            f"Academic user {academic_user.email} used streaming query "
                            f"({academic_user.queries_remaining - 1} remaining)"
                        )

                    complete_data = {
                        "type": "complete",
                        "metadata": chunk.metadata or {},
                    }
                    yield f"data: {json.dumps(complete_data)}\n\n"

            # Send done signal
            yield "data: [DONE]\n\n"

        except Exception as e:
            logger.exception("Error during streaming")
            error_data = {"type": "error", "content": str(e)}
            yield f"data: {json.dumps(error_data)}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.delete("/history")
async def clear_history(
    session_id: str,
    agent_service: AgentService = Depends(get_agent_service),
):
    """Clear conversation history for a session."""
    cleared = agent_service.clear_session(session_id)

    if cleared:
        return {"success": True, "message": f"History cleared for session {session_id}"}
    else:
        return {"success": True, "message": f"No history found for session {session_id}"}


@router.get("/status")
async def chat_status(
    agent_service: AgentService = Depends(get_agent_service),
):
    """Get chat service status."""
    return {
        "available": settings.has_anthropic_key,
        "model": agent_service.model_name,
        "initialized": agent_service.is_initialized,
    }
