"""Academic user service for email registration and query tracking."""

import logging
import os
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Optional

import duckdb
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class AcademicUser(BaseModel):
    """Academic user data model."""

    email: str
    created_at: datetime
    last_access: Optional[datetime] = None


class QueryUsage(BaseModel):
    """Daily query usage tracking."""

    email: str
    query_date: date
    query_count: int


class AcademicUserService:
    """
    Service for managing academic users and their query quotas.

    Uses DuckDB/MotherDuck for persistent cloud storage of user data.
    Supports both local DuckDB files and MotherDuck cloud connections.
    """

    def __init__(self, db_path: str, daily_limit: int = 50):
        """
        Initialize academic user service.

        Args:
            db_path: Path to DuckDB file OR MotherDuck connection string (md:database_name)
            daily_limit: Maximum AI queries per day
        """
        self.db_path = db_path
        self.is_motherduck = db_path.startswith("md:")
        self.daily_limit = daily_limit
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
        self._ensure_database()

    def _ensure_database(self) -> None:
        """Create database and tables if they don't exist."""
        with self._get_connection() as conn:
            # Users table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS academic_users (
                    email VARCHAR PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_access TIMESTAMP
                )
            """)

            # Query usage table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS query_usage (
                    email VARCHAR,
                    query_date DATE,
                    query_count INTEGER DEFAULT 0,
                    PRIMARY KEY (email, query_date)
                )
            """)

            logger.info(f"Academic user database initialized at {self.db_path}")

    @contextmanager
    def _get_connection(self):
        """Get a database connection with context management."""
        if self.is_motherduck:
            # Check for MotherDuck token
            if not os.environ.get("motherduck_token"):
                raise RuntimeError("MotherDuck token not configured. Set motherduck_token environment variable.")
            # MotherDuck connection - need read_only=False for writes
            conn = duckdb.connect(self.db_path, read_only=False)
            logger.debug(f"Connected to MotherDuck: {self.db_path}")
        else:
            # Local DuckDB file
            conn = duckdb.connect(self.db_path, read_only=False)
            logger.debug(f"Connected to local DuckDB: {self.db_path}")
        try:
            yield conn
        finally:
            conn.close()

    def register_email(self, email: str) -> AcademicUser:
        """
        Register a new academic user by email.

        If the email already exists, updates last_access and returns existing user.

        Args:
            email: User's email address

        Returns:
            AcademicUser instance
        """
        email = email.lower().strip()
        now = datetime.now(timezone.utc)

        with self._get_connection() as conn:
            # Try to get existing user
            result = conn.execute(
                "SELECT email, created_at, last_access FROM academic_users WHERE email = ?",
                [email],
            )
            row = result.fetchone()

            if row:
                # Update last access
                conn.execute(
                    "UPDATE academic_users SET last_access = ? WHERE email = ?",
                    [now, email],
                )
                logger.info(f"Returning access for existing academic user: {email}")
                created_at = row[1] if isinstance(row[1], datetime) else datetime.fromisoformat(str(row[1]))
                return AcademicUser(
                    email=row[0],
                    created_at=created_at,
                    last_access=now,
                )

            # Create new user
            conn.execute(
                "INSERT INTO academic_users (email, created_at, last_access) VALUES (?, ?, ?)",
                [email, now, now],
            )
            logger.info(f"Registered new academic user: {email}")

            return AcademicUser(email=email, created_at=now, last_access=now)

    def get_user(self, email: str) -> Optional[AcademicUser]:
        """
        Get an academic user by email.

        Args:
            email: User's email address

        Returns:
            AcademicUser if found, None otherwise
        """
        email = email.lower().strip()

        with self._get_connection() as conn:
            result = conn.execute(
                "SELECT email, created_at, last_access FROM academic_users WHERE email = ?",
                [email],
            )
            row = result.fetchone()

            if not row:
                return None

            created_at = row[1] if isinstance(row[1], datetime) else datetime.fromisoformat(str(row[1]))
            last_access = None
            if row[2]:
                last_access = row[2] if isinstance(row[2], datetime) else datetime.fromisoformat(str(row[2]))

            return AcademicUser(
                email=row[0],
                created_at=created_at,
                last_access=last_access,
            )

    def get_queries_remaining(self, email: str) -> int:
        """
        Get remaining queries for today.

        Args:
            email: User's email address

        Returns:
            Number of queries remaining today
        """
        email = email.lower().strip()
        today = date.today()

        with self._get_connection() as conn:
            result = conn.execute(
                "SELECT query_count FROM query_usage WHERE email = ? AND query_date = ?",
                [email, today],
            )
            row = result.fetchone()

            if not row:
                return self.daily_limit

            return max(0, self.daily_limit - row[0])

    def increment_usage(self, email: str) -> int:
        """
        Increment query count for today.

        Args:
            email: User's email address

        Returns:
            New query count for today
        """
        email = email.lower().strip()
        today = date.today()

        with self._get_connection() as conn:
            # Upsert query count - DuckDB syntax
            conn.execute(
                """
                INSERT INTO query_usage (email, query_date, query_count)
                VALUES (?, ?, 1)
                ON CONFLICT (email, query_date)
                DO UPDATE SET query_count = query_usage.query_count + 1
                """,
                [email, today],
            )

            # Get new count
            result = conn.execute(
                "SELECT query_count FROM query_usage WHERE email = ? AND query_date = ?",
                [email, today],
            )
            row = result.fetchone()
            return row[0] if row else 1

    def check_quota(self, email: str) -> tuple[bool, int]:
        """
        Check if user has remaining quota.

        Args:
            email: User's email address

        Returns:
            Tuple of (has_quota, queries_remaining)
        """
        remaining = self.get_queries_remaining(email)
        return remaining > 0, remaining

    def get_user_stats(self, email: str) -> dict:
        """
        Get comprehensive stats for a user.

        Args:
            email: User's email address

        Returns:
            Dict with user statistics
        """
        email = email.lower().strip()
        today = date.today()

        with self._get_connection() as conn:
            # Get user info
            result = conn.execute(
                "SELECT email, created_at, last_access FROM academic_users WHERE email = ?",
                [email],
            )
            user_row = result.fetchone()

            if not user_row:
                return {"error": "User not found"}

            # Get today's usage
            result = conn.execute(
                "SELECT query_count FROM query_usage WHERE email = ? AND query_date = ?",
                [email, today],
            )
            usage_row = result.fetchone()
            queries_used = usage_row[0] if usage_row else 0

            # Get total queries all time
            result = conn.execute(
                "SELECT SUM(query_count) as total FROM query_usage WHERE email = ?",
                [email],
            )
            total_row = result.fetchone()
            total_queries = total_row[0] or 0

            return {
                "email": user_row[0],
                "created_at": str(user_row[1]),
                "last_access": str(user_row[2]) if user_row[2] else None,
                "queries_used_today": queries_used,
                "queries_remaining_today": max(0, self.daily_limit - queries_used),
                "daily_limit": self.daily_limit,
                "total_queries_all_time": total_queries,
            }

    def get_total_users(self) -> int:
        """Get total number of registered academic users."""
        with self._get_connection() as conn:
            result = conn.execute("SELECT COUNT(*) FROM academic_users")
            row = result.fetchone()
            return row[0] if row else 0
