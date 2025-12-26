"""Agent service wrapping the LandUseAgent for FastAPI integration."""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import AsyncIterator, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class QueryResponse:
    """Response from agent query."""

    content: str
    sql_query: Optional[str] = None
    execution_time: float = 0.0


@dataclass
class StreamChunk:
    """Chunk of streaming response."""

    type: str  # 'start', 'content', 'tool_call', 'tool_result', 'complete', 'error'
    content: Optional[str] = None
    metadata: Optional[Dict] = None


class AgentService:
    """
    Service layer wrapping the LandUseAgent for async API use.

    Handles:
    - Async streaming of agent responses
    - Session/conversation management
    - Error handling
    """

    def __init__(self, database_path: Optional[str] = None):
        """
        Initialize the agent service.

        Args:
            database_path: Optional path to the DuckDB database
        """
        self._agent = None
        self._sessions: Dict[str, list] = {}  # session_id -> conversation history
        self._database_path = database_path
        self._initialized = False

    def _get_agent(self):
        """Lazy-load the LandUseAgent."""
        if self._agent is None:
            try:
                from landuse.agents.landuse_agent import LandUseAgent
                from landuse.core.app_config import AppConfig

                # Create config with optional database path override
                if self._database_path:
                    import os
                    os.environ["LANDUSE_DATABASE__PATH"] = self._database_path

                config = AppConfig()
                self._agent = LandUseAgent(config)
                self._initialized = True
                logger.info(f"LandUseAgent initialized with model: {self._agent.model_name}")
            except Exception as e:
                logger.error(f"Failed to initialize LandUseAgent: {e}")
                raise RuntimeError(f"Agent initialization failed: {e}")

        return self._agent

    @property
    def is_initialized(self) -> bool:
        """Check if agent is initialized."""
        return self._initialized

    @property
    def model_name(self) -> str:
        """Get the model name."""
        try:
            return self._get_agent().model_name
        except Exception:
            return "unknown"

    async def query(self, question: str, session_id: Optional[str] = None) -> QueryResponse:
        """
        Execute a natural language query asynchronously.

        Args:
            question: The natural language question
            session_id: Optional session ID for conversation continuity

        Returns:
            QueryResponse with the agent's response
        """
        start_time = time.time()

        try:
            agent = self._get_agent()

            # Build message history for context
            messages = []
            if session_id and session_id in self._sessions:
                for item in self._sessions[session_id][-10:]:  # Last 10 exchanges
                    messages.append({"role": "user", "content": item["question"]})
                    messages.append({"role": "assistant", "content": item["response"]})

            messages.append({"role": "user", "content": question})

            # Stream and collect the full response
            response_text = ""
            async for event in agent.stream(messages):
                if event["type"] == "text":
                    response_text = event["content"]

            execution_time = time.time() - start_time

            # Store in session if provided
            if session_id:
                if session_id not in self._sessions:
                    self._sessions[session_id] = []
                self._sessions[session_id].append({
                    "question": question,
                    "response": response_text,
                    "timestamp": time.time()
                })

            return QueryResponse(
                content=response_text,
                sql_query=None,
                execution_time=execution_time,
            )

        except Exception as e:
            logger.exception(f"Error processing query: {question[:50]}...")
            execution_time = time.time() - start_time
            return QueryResponse(
                content=f"Error processing query: {str(e)}",
                execution_time=execution_time,
            )

    async def stream_query(
        self,
        question: str,
        session_id: Optional[str] = None
    ) -> AsyncIterator[StreamChunk]:
        """
        Stream response chunks for real-time interaction.

        Args:
            question: The natural language question
            session_id: Optional session ID

        Yields:
            StreamChunk objects with response content
        """
        start_time = time.time()

        try:
            agent = self._get_agent()

            # Build message history for context
            messages = []
            if session_id and session_id in self._sessions:
                for item in self._sessions[session_id][-10:]:  # Last 10 exchanges
                    messages.append({"role": "user", "content": item["question"]})
                    messages.append({"role": "assistant", "content": item["response"]})

            messages.append({"role": "user", "content": question})

            full_response = ""

            # Stream from agent with heartbeat to prevent proxy timeouts
            async def stream_with_heartbeat():
                """Wrap agent stream with periodic heartbeats."""
                import asyncio

                queue: asyncio.Queue = asyncio.Queue()
                finished = False

                async def producer():
                    nonlocal finished
                    try:
                        async for event in agent.stream(messages):
                            await queue.put(event)
                    except Exception as e:
                        logger.exception(f"Producer error: {e}")
                        await queue.put({"type": "error", "content": str(e)})
                    finally:
                        finished = True
                        await queue.put(None)  # Signal end

                # Start producer task
                producer_task = asyncio.create_task(producer())
                heartbeat_interval = 5  # seconds

                try:
                    while True:
                        try:
                            event = await asyncio.wait_for(queue.get(), timeout=heartbeat_interval)
                            if event is None:
                                break
                            yield event
                        except asyncio.TimeoutError:
                            if finished:
                                break
                            yield {"type": "heartbeat"}
                finally:
                    producer_task.cancel()
                    try:
                        await producer_task
                    except asyncio.CancelledError:
                        pass

            async for event in stream_with_heartbeat():
                event_type = event.get("type")

                if event_type == "text":
                    content = event.get("content", "")
                    full_response = content

                    # Yield the full content to preserve markdown formatting
                    # (newlines, tables, lists, etc.)
                    yield StreamChunk(
                        type="content",
                        content=content
                    )

                elif event_type == "tool_call":
                    yield StreamChunk(
                        type="tool_call",
                        content=f"Querying: {event.get('tool_name', 'data')}",
                        metadata={"tool_name": event.get("tool_name"), "args": event.get("args")}
                    )

                elif event_type == "tool_result":
                    yield StreamChunk(
                        type="tool_result",
                        metadata={"tool_call_id": event.get("tool_call_id")}
                    )

                elif event_type == "heartbeat":
                    # Send heartbeat to keep connection alive
                    yield StreamChunk(
                        type="heartbeat",
                        content="."
                    )

                elif event_type == "finish":
                    # Store in session
                    if session_id and full_response:
                        if session_id not in self._sessions:
                            self._sessions[session_id] = []
                        self._sessions[session_id].append({
                            "question": question,
                            "response": full_response,
                            "timestamp": time.time()
                        })

                    execution_time = time.time() - start_time
                    yield StreamChunk(
                        type="complete",
                        metadata={"execution_time": execution_time}
                    )

        except Exception as e:
            logger.exception(f"Streaming error: {e}")
            yield StreamChunk(type="error", content=str(e))

    def clear_session(self, session_id: str) -> bool:
        """
        Clear conversation history for a specific session.

        Args:
            session_id: The session ID to clear

        Returns:
            True if cleared successfully
        """
        logger.info(f"Clearing session {session_id}")

        # Clear local session storage for this session
        if session_id in self._sessions:
            del self._sessions[session_id]

        # Clear agent's internal history
        if self._agent:
            try:
                self._agent.clear_history()
                logger.info(f"Cleared agent conversation history")
            except Exception as e:
                logger.warning(f"Error clearing agent history: {e}")

        return True

    def get_session_history(self, session_id: str) -> list:
        """Get conversation history for a session."""
        return self._sessions.get(session_id, [])

    def cleanup(self):
        """Clean up resources."""
        if self._agent:
            try:
                self._agent.__exit__(None, None, None)
            except Exception:
                pass
            self._agent = None

        self._sessions.clear()
        self._initialized = False
