"""Academic user service for email registration and query tracking."""

import logging
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Optional

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

    Uses SQLite for lightweight user storage, separate from the
    analytics DuckDB database.
    """

    def __init__(self, db_path: str, daily_limit: int = 50):
        """
        Initialize academic user service.

        Args:
            db_path: Path to SQLite database file
            daily_limit: Maximum AI queries per day
        """
        self.db_path = db_path
        self.daily_limit = daily_limit
        self._ensure_database()

    def _ensure_database(self) -> None:
        """Create database and tables if they don't exist."""
        db_dir = Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Users table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS academic_users (
                    email TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_access TIMESTAMP
                )
            """)

            # Query usage table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS query_usage (
                    email TEXT,
                    query_date DATE,
                    query_count INTEGER DEFAULT 0,
                    PRIMARY KEY (email, query_date),
                    FOREIGN KEY (email) REFERENCES academic_users(email)
                )
            """)

            conn.commit()
            logger.info(f"Academic user database initialized at {self.db_path}")

    @contextmanager
    def _get_connection(self):
        """Get a database connection with context management."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
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
        now = datetime.utcnow()

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Try to get existing user
            cursor.execute(
                "SELECT email, created_at, last_access FROM academic_users WHERE email = ?",
                (email,),
            )
            row = cursor.fetchone()

            if row:
                # Update last access
                cursor.execute(
                    "UPDATE academic_users SET last_access = ? WHERE email = ?",
                    (now, email),
                )
                conn.commit()
                logger.info(f"Returning access for existing academic user: {email}")
                return AcademicUser(
                    email=row["email"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                    last_access=now,
                )

            # Create new user
            cursor.execute(
                "INSERT INTO academic_users (email, created_at, last_access) VALUES (?, ?, ?)",
                (email, now, now),
            )
            conn.commit()
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
            cursor = conn.cursor()
            cursor.execute(
                "SELECT email, created_at, last_access FROM academic_users WHERE email = ?",
                (email,),
            )
            row = cursor.fetchone()

            if not row:
                return None

            return AcademicUser(
                email=row["email"],
                created_at=datetime.fromisoformat(row["created_at"]),
                last_access=(
                    datetime.fromisoformat(row["last_access"])
                    if row["last_access"]
                    else None
                ),
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
            cursor = conn.cursor()
            cursor.execute(
                "SELECT query_count FROM query_usage WHERE email = ? AND query_date = ?",
                (email, today),
            )
            row = cursor.fetchone()

            if not row:
                return self.daily_limit

            return max(0, self.daily_limit - row["query_count"])

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
            cursor = conn.cursor()

            # Upsert query count
            cursor.execute(
                """
                INSERT INTO query_usage (email, query_date, query_count)
                VALUES (?, ?, 1)
                ON CONFLICT(email, query_date)
                DO UPDATE SET query_count = query_count + 1
                """,
                (email, today),
            )
            conn.commit()

            # Get new count
            cursor.execute(
                "SELECT query_count FROM query_usage WHERE email = ? AND query_date = ?",
                (email, today),
            )
            row = cursor.fetchone()
            return row["query_count"] if row else 1

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
            cursor = conn.cursor()

            # Get user info
            cursor.execute(
                "SELECT email, created_at, last_access FROM academic_users WHERE email = ?",
                (email,),
            )
            user_row = cursor.fetchone()

            if not user_row:
                return {"error": "User not found"}

            # Get today's usage
            cursor.execute(
                "SELECT query_count FROM query_usage WHERE email = ? AND query_date = ?",
                (email, today),
            )
            usage_row = cursor.fetchone()
            queries_used = usage_row["query_count"] if usage_row else 0

            # Get total queries all time
            cursor.execute(
                "SELECT SUM(query_count) as total FROM query_usage WHERE email = ?",
                (email,),
            )
            total_row = cursor.fetchone()
            total_queries = total_row["total"] or 0

            return {
                "email": user_row["email"],
                "created_at": user_row["created_at"],
                "last_access": user_row["last_access"],
                "queries_used_today": queries_used,
                "queries_remaining_today": max(0, self.daily_limit - queries_used),
                "daily_limit": self.daily_limit,
                "total_queries_all_time": total_queries,
            }

    def get_total_users(self) -> int:
        """Get total number of registered academic users."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as count FROM academic_users")
            row = cursor.fetchone()
            return row["count"] if row else 0
