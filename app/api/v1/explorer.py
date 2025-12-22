"""SQL Explorer endpoints for database queries."""

import logging

from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import get_database_service_singleton, require_auth
from app.models.requests import SqlQueryRequest
from app.models.responses import QueryResultResponse, SchemaResponse
from app.services.database_service import DatabaseService

router = APIRouter(prefix="/explorer", dependencies=[Depends(require_auth)])
logger = logging.getLogger(__name__)


def validate_query(query: str) -> tuple[bool, str]:
    """Validate SQL query for safety."""
    query_upper = query.strip().upper()

    # Must start with SELECT
    if not query_upper.startswith("SELECT"):
        return False, "Only SELECT queries are allowed"

    # Check for dangerous keywords
    dangerous = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE", "EXEC"]
    for keyword in dangerous:
        if keyword in query_upper:
            return False, f"Query contains forbidden keyword: {keyword}"

    return True, ""


@router.get("/schema", response_model=SchemaResponse)
async def get_schema(
    db_service: DatabaseService = Depends(get_database_service_singleton),
):
    """Get database schema information."""
    try:
        if not db_service.is_available:
            raise HTTPException(
                status_code=503,
                detail="Database not available",
            )

        schema = db_service.get_schema()
        return SchemaResponse(
            tables=schema.get("tables", []),
            views=schema.get("views", []),
            total_rows=schema.get("total_rows", 0),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query", response_model=QueryResultResponse)
async def execute_query(
    request: SqlQueryRequest,
    db_service: DatabaseService = Depends(get_database_service_singleton),
):
    """Execute a SQL query against the database."""
    # Validate query
    is_valid, error_msg = validate_query(request.query)
    if not is_valid:
        return QueryResultResponse(
            success=False,
            columns=[],
            data=[],
            row_count=0,
            execution_time=0.0,
            error=error_msg,
            suggestion="Use SELECT statements to query the database",
        )

    try:
        if not db_service.is_available:
            return QueryResultResponse(
                success=False,
                columns=[],
                data=[],
                row_count=0,
                execution_time=0.0,
                error="Database not available",
                suggestion="Check that the database file exists",
            )

        columns, data, execution_time = db_service.execute_query(
            request.query,
            limit=request.limit or 1000,
        )

        return QueryResultResponse(
            success=True,
            columns=columns,
            data=data,
            row_count=len(data),
            execution_time=execution_time,
        )

    except Exception as e:
        logger.exception("Error executing query")
        return QueryResultResponse(
            success=False,
            columns=[],
            data=[],
            row_count=0,
            execution_time=0.0,
            error=str(e),
            suggestion="Check your SQL syntax and table names",
        )


@router.get("/templates")
async def get_query_templates():
    """Get example query templates."""
    return {
        "templates": [
            {
                "id": "basic_select",
                "name": "Basic Select",
                "category": "Basic",
                "description": "Simple query to view land use transitions",
                "query": "SELECT * FROM fact_landuse_transitions LIMIT 10",
            },
            {
                "id": "forest_loss",
                "name": "Forest Loss by State",
                "category": "Forest",
                "description": "Analyze forest loss by state",
                "query": """SELECT
    g.state_name,
    SUM(f.acres) as total_forest_loss
FROM fact_landuse_transitions f
JOIN dim_geography g ON f.geography_id = g.geography_id
JOIN dim_landuse l_from ON f.from_landuse_id = l_from.landuse_id
WHERE l_from.landuse_name = 'Forest'
  AND f.transition_type = 'change'
GROUP BY g.state_name
ORDER BY total_forest_loss DESC
LIMIT 10""",
            },
            {
                "id": "scenario_comparison",
                "name": "Scenario Comparison",
                "category": "Climate",
                "description": "Compare urbanization across scenarios",
                "query": """SELECT
    s.scenario_name,
    SUM(f.acres) as urban_growth
FROM fact_landuse_transitions f
JOIN dim_scenario s ON f.scenario_id = s.scenario_id
JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
WHERE l_to.landuse_name = 'Urban'
  AND f.transition_type = 'change'
GROUP BY s.scenario_name
ORDER BY urban_growth DESC""",
            },
            {
                "id": "county_urbanization",
                "name": "Top Urbanizing Counties",
                "category": "Geographic",
                "description": "Find counties with most urban growth",
                "query": """SELECT
    g.county_name,
    g.state_name,
    SUM(f.acres) as urban_growth
FROM fact_landuse_transitions f
JOIN dim_geography g ON f.geography_id = g.geography_id
JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
WHERE l_to.landuse_name = 'Urban'
  AND f.transition_type = 'change'
GROUP BY g.county_name, g.state_name
ORDER BY urban_growth DESC
LIMIT 20""",
            },
            {
                "id": "time_trends",
                "name": "Land Use Over Time",
                "category": "Temporal",
                "description": "See how land use changes over time",
                "query": """SELECT
    t.year_range,
    l_to.landuse_name as to_landuse,
    SUM(f.acres) as total_acres
FROM fact_landuse_transitions f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
WHERE f.transition_type = 'change'
GROUP BY t.year_range, l_to.landuse_name
ORDER BY t.year_range, total_acres DESC""",
            },
        ]
    }


@router.get("/stats")
async def get_database_stats(
    db_service: DatabaseService = Depends(get_database_service_singleton),
):
    """Get database statistics."""
    return {
        "available": db_service.is_available,
        "file_size_mb": db_service.file_size_mb,
        "path": str(db_service.database_path),
    }
