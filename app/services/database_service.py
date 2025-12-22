"""Database service for direct DuckDB operations."""

import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb

logger = logging.getLogger(__name__)


class DatabaseService:
    """
    Service for direct database operations.

    Used for analytics, explorer, and extraction endpoints that need
    direct database access without going through the agent.

    Supports both local file paths and MotherDuck cloud connections.
    """

    def __init__(self, database_path: str, read_only: bool = True):
        """
        Initialize database service.

        Args:
            database_path: Path to DuckDB file OR MotherDuck connection string (md:database_name)
            read_only: Whether to open in read-only mode
        """
        self.database_path = database_path
        self.is_motherduck = database_path.startswith("md:")
        self.read_only = read_only
        self._connection: Optional[duckdb.DuckDBPyConnection] = None

    @property
    def is_available(self) -> bool:
        """Check if database is available."""
        if self.is_motherduck:
            # For MotherDuck, check if token is configured
            return bool(os.environ.get("motherduck_token"))
        return Path(self.database_path).exists()

    @property
    def file_size_mb(self) -> float:
        """Get database file size in MB (returns 0 for MotherDuck)."""
        if self.is_motherduck:
            return 0.0
        path = Path(self.database_path)
        if path.exists():
            return path.stat().st_size / (1024 * 1024)
        return 0.0

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection."""
        if self._connection is None:
            if not self.is_available:
                if self.is_motherduck:
                    raise RuntimeError("MotherDuck token not configured. Set motherduck_token environment variable.")
                raise RuntimeError(f"Database not found at {self.database_path}")

            if self.is_motherduck:
                # MotherDuck connection - use read_only to match agent's connection config
                self._connection = duckdb.connect(self.database_path, read_only=self.read_only)
                logger.info(f"Connected to MotherDuck: {self.database_path} (read_only={self.read_only})")
            else:
                # Local file connection
                self._connection = duckdb.connect(
                    str(self.database_path),
                    read_only=self.read_only
                )
                logger.info(f"Connected to database: {self.database_path}")

        return self._connection

    def execute_query(
        self,
        query: str,
        limit: int = 1000
    ) -> Tuple[List[str], List[Dict[str, Any]], float]:
        """
        Execute a SQL query.

        Args:
            query: SQL query to execute
            limit: Maximum rows to return

        Returns:
            Tuple of (columns, data, execution_time)
        """
        start_time = time.time()

        try:
            conn = self._get_connection()

            # Add LIMIT if not present and query is SELECT
            query_upper = query.strip().upper()
            if query_upper.startswith("SELECT") and "LIMIT" not in query_upper:
                query = f"{query.rstrip().rstrip(';')} LIMIT {limit}"

            result = conn.execute(query)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            # Convert to list of dicts
            data = [dict(zip(columns, row)) for row in rows]

            execution_time = time.time() - start_time
            return columns, data, execution_time

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Query execution error: {e}")
            raise

    def get_schema(self) -> Dict[str, Any]:
        """
        Get database schema information.

        Returns:
            Dict with tables, views, and metadata
        """
        try:
            conn = self._get_connection()

            # Get tables
            tables_query = """
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = 'main'
                ORDER BY table_name
            """
            tables_result = conn.execute(tables_query).fetchall()

            tables = []
            views = []

            for table_name, table_type in tables_result:
                # Get column info
                columns_query = f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """
                columns = conn.execute(columns_query).fetchall()

                # Get row count
                try:
                    count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                    row_count = count_result[0] if count_result else 0
                except Exception:
                    row_count = 0

                table_info = {
                    "name": table_name,
                    "type": "dimension" if table_name.startswith("dim_") else "fact",
                    "row_count": row_count,
                    "columns": [
                        {"name": col[0], "type": col[1]}
                        for col in columns
                    ]
                }

                if table_type == "VIEW":
                    views.append({
                        "name": table_name,
                        "description": f"View: {table_name}"
                    })
                else:
                    tables.append(table_info)

            # Calculate total rows in fact tables
            total_rows = sum(
                t["row_count"] for t in tables
                if t["type"] == "fact"
            )

            return {
                "tables": tables,
                "views": views,
                "total_rows": total_rows
            }

        except Exception as e:
            logger.error(f"Schema retrieval error: {e}")
            raise

    def get_analytics_data(
        self,
        analysis_type: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get pre-built analytics data.

        Args:
            analysis_type: Type of analysis (forest, agricultural, urban, scenarios)
            filters: Optional filters to apply

        Returns:
            List of data records
        """
        filters = filters or {}

        queries = {
            "forest_transitions": """
                SELECT
                    g.state_name,
                    l_to.landuse_name as to_landuse,
                    SUM(CAST(f.acres AS DOUBLE)) as total_acres
                FROM fact_landuse_transitions f
                JOIN dim_geography g ON f.geography_id = g.geography_id
                JOIN dim_landuse l_from ON f.from_landuse_id = l_from.landuse_id
                JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
                WHERE l_from.landuse_name = 'Forest'
                  AND l_from.landuse_id != l_to.landuse_id
                GROUP BY g.state_name, l_to.landuse_name
                ORDER BY total_acres DESC
                LIMIT 100
            """,
            "agricultural_impact": """
                SELECT
                    g.state_name,
                    l_from.landuse_name as from_landuse,
                    SUM(CAST(f.acres AS DOUBLE)) as loss_acres
                FROM fact_landuse_transitions f
                JOIN dim_geography g ON f.geography_id = g.geography_id
                JOIN dim_landuse l_from ON f.from_landuse_id = l_from.landuse_id
                JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
                WHERE l_from.landuse_name IN ('Crop', 'Pasture')
                  AND l_from.landuse_id != l_to.landuse_id
                GROUP BY g.state_name, l_from.landuse_name
                ORDER BY loss_acres DESC
                LIMIT 100
            """,
            "urbanization_sources": """
                SELECT
                    l_from.landuse_name as source,
                    SUM(CAST(f.acres AS DOUBLE)) as total_acres,
                    ROUND(100.0 * SUM(CAST(f.acres AS DOUBLE)) / SUM(SUM(CAST(f.acres AS DOUBLE))) OVER (), 1) as percentage
                FROM fact_landuse_transitions f
                JOIN dim_landuse l_from ON f.from_landuse_id = l_from.landuse_id
                JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
                WHERE l_to.landuse_name = 'Urban'
                  AND l_from.landuse_id != l_to.landuse_id
                GROUP BY l_from.landuse_name
                ORDER BY total_acres DESC
            """,
            "scenario_comparison": """
                SELECT
                    s.scenario_name,
                    s.rcp_scenario as rcp,
                    s.ssp_scenario as ssp,
                    SUM(CASE WHEN l_to.landuse_name = 'Urban' THEN CAST(f.acres AS DOUBLE) ELSE 0 END) as urban_growth,
                    SUM(CASE WHEN l_from.landuse_name = 'Forest' AND l_from.landuse_id != l_to.landuse_id
                        THEN CAST(f.acres AS DOUBLE) ELSE 0 END) as forest_loss
                FROM fact_landuse_transitions f
                JOIN dim_scenario s ON f.scenario_id = s.scenario_id
                JOIN dim_landuse l_from ON f.from_landuse_id = l_from.landuse_id
                JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
                GROUP BY s.scenario_name, s.rcp_scenario, s.ssp_scenario
                ORDER BY urban_growth DESC
            """,
            "overview": """
                SELECT
                    (SELECT COUNT(DISTINCT geography_id) FROM dim_geography) as total_counties,
                    (SELECT COUNT(*) FROM fact_landuse_transitions) as total_transitions,
                    (SELECT COUNT(*) FROM dim_scenario) as scenarios,
                    (SELECT COUNT(*) FROM dim_time) as time_periods,
                    (SELECT COUNT(*) FROM dim_landuse) as land_use_types
            """
        }

        query = queries.get(analysis_type)
        if not query:
            raise ValueError(f"Unknown analysis type: {analysis_type}")

        _, data, _ = self.execute_query(query, limit=1000)
        return data

    def close(self):
        """Close database connection."""
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None
            logger.info("Database connection closed")
