"""Agent service wrapping the LanduseAgent for FastAPI integration."""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
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

    type: str  # 'start', 'content', 'sql', 'complete', 'error'
    content: Optional[str] = None
    metadata: Optional[Dict] = None


class AgentService:
    """
    Service layer wrapping the LanduseAgent for async API use.

    Handles:
    - Async execution of synchronous agent methods
    - Session/conversation management
    - Streaming response generation
    - Error handling and retry logic
    """

    def __init__(self, database_path: Optional[str] = None):
        """
        Initialize the agent service.

        Args:
            database_path: Optional path to the DuckDB database
        """
        self._agent = None
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._sessions: Dict[str, list] = {}  # session_id -> conversation history
        self._database_path = database_path
        self._initialized = False

    def _get_agent(self):
        """Lazy-load the LanduseAgent."""
        if self._agent is None:
            try:
                from landuse.agents.landuse_agent import LanduseAgent
                from landuse.core.app_config import AppConfig

                # Create config with optional database path override
                if self._database_path:
                    import os
                    os.environ["LANDUSE_DATABASE__PATH"] = self._database_path

                config = AppConfig()
                self._agent = LanduseAgent(config)
                self._initialized = True
                logger.info(f"LanduseAgent initialized with model: {self._agent.model_name}")
            except Exception as e:
                logger.error(f"Failed to initialize LanduseAgent: {e}")
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
        loop = asyncio.get_event_loop()

        try:
            # Run synchronous agent in thread pool
            agent = self._get_agent()
            response = await loop.run_in_executor(
                self._executor,
                lambda: agent.query(question, use_graph=False)
            )

            execution_time = time.time() - start_time

            # Store in session if provided
            if session_id:
                if session_id not in self._sessions:
                    self._sessions[session_id] = []
                self._sessions[session_id].append({
                    "question": question,
                    "response": response,
                    "timestamp": time.time()
                })

            return QueryResponse(
                content=response,
                sql_query=None,  # TODO: Extract SQL from agent if available
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
            # Use simulated streaming with non-streaming query
            # (Native LangGraph streaming has compatibility issues with tool messages)
            response = await self.query(question, session_id)

            # Stream in word chunks for a streaming effect
            words = response.content.split()
            chunk_size = 5

            for i in range(0, len(words), chunk_size):
                chunk_words = words[i:i + chunk_size]
                yield StreamChunk(
                    type="content",
                    content=" ".join(chunk_words) + " "
                )
                await asyncio.sleep(0.03)

            # Send completion
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
        Clear conversation history for a session.

        Completely reinitializes the agent to ensure all history is cleared.

        Args:
            session_id: The session ID to clear

        Returns:
            True if cleared successfully
        """
        logger.info(f"Clearing session {session_id} - reinitializing agent")

        # Clear local sessions
        self._sessions.clear()

        # Completely destroy and reinitialize the agent
        # This ensures ALL history is cleared (conversation manager, graph state, etc.)
        if self._agent:
            try:
                self._agent.__exit__(None, None, None)
            except Exception as e:
                logger.warning(f"Error closing agent: {e}")
            self._agent = None
            self._initialized = False
            logger.info("Agent destroyed - will reinitialize on next query")

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

        self._executor.shutdown(wait=False)
        self._sessions.clear()
        self._initialized = False
