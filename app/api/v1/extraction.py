"""Data extraction endpoints for exporting data."""

import csv
import io
import json
import logging
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from app.dependencies import get_database_service_singleton, require_auth
from app.models.requests import ExtractionRequest
from app.models.responses import ExtractionResponse
from app.services.database_service import DatabaseService

router = APIRouter(prefix="/extraction", dependencies=[Depends(require_auth)])
logger = logging.getLogger(__name__)


# Predefined extraction queries
EXTRACTION_QUERIES = {
    "agricultural_transitions": """
        SELECT
            g.fips_code,
            g.county_name,
            g.state_name,
            s.scenario_name,
            t.start_year as year,
            l_from.landuse_name as from_landuse,
            l_to.landuse_name as to_landuse,
            CAST(f.acres AS DOUBLE) as acres
        FROM fact_landuse_transitions f
        JOIN dim_geography g ON f.geography_id = g.geography_id
        JOIN dim_scenario s ON f.scenario_id = s.scenario_id
        JOIN dim_time t ON f.time_id = t.time_id
        JOIN dim_landuse l_from ON f.from_landuse_id = l_from.landuse_id
        JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
        WHERE l_from.landuse_name IN ('Crop', 'Pasture')
           OR l_to.landuse_name IN ('Crop', 'Pasture')
    """,
    "urbanization_data": """
        SELECT
            g.fips_code,
            g.county_name,
            g.state_name,
            s.scenario_name,
            t.start_year as year,
            l_from.landuse_name as source_landuse,
            CAST(f.acres AS DOUBLE) as acres
        FROM fact_landuse_transitions f
        JOIN dim_geography g ON f.geography_id = g.geography_id
        JOIN dim_scenario s ON f.scenario_id = s.scenario_id
        JOIN dim_time t ON f.time_id = t.time_id
        JOIN dim_landuse l_from ON f.from_landuse_id = l_from.landuse_id
        JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
        WHERE l_to.landuse_name = 'Urban'
    """,
    "forest_changes": """
        SELECT
            g.fips_code,
            g.county_name,
            g.state_name,
            s.scenario_name,
            t.start_year as year,
            CASE
                WHEN l_from.landuse_name = 'Forest' THEN 'loss'
                ELSE 'gain'
            END as change_type,
            CASE
                WHEN l_from.landuse_name = 'Forest' THEN l_to.landuse_name
                ELSE l_from.landuse_name
            END as other_landuse,
            CAST(f.acres AS DOUBLE) as acres
        FROM fact_landuse_transitions f
        JOIN dim_geography g ON f.geography_id = g.geography_id
        JOIN dim_scenario s ON f.scenario_id = s.scenario_id
        JOIN dim_time t ON f.time_id = t.time_id
        JOIN dim_landuse l_from ON f.from_landuse_id = l_from.landuse_id
        JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
        WHERE l_from.landuse_name = 'Forest' OR l_to.landuse_name = 'Forest'
    """,
    "state_summaries": """
        SELECT
            g.state_name,
            s.scenario_name,
            l_from.landuse_name as from_landuse,
            l_to.landuse_name as to_landuse,
            SUM(CAST(f.acres AS DOUBLE)) as total_acres
        FROM fact_landuse_transitions f
        JOIN dim_geography g ON f.geography_id = g.geography_id
        JOIN dim_scenario s ON f.scenario_id = s.scenario_id
        JOIN dim_landuse l_from ON f.from_landuse_id = l_from.landuse_id
        JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
        GROUP BY g.state_name, s.scenario_name, l_from.landuse_name, l_to.landuse_name
    """,
    "scenario_comparison": """
        SELECT
            s.scenario_name,
            s.rcp_scenario as rcp,
            s.ssp_scenario as ssp,
            l_from.landuse_name as from_landuse,
            l_to.landuse_name as to_landuse,
            SUM(CAST(f.acres AS DOUBLE)) as total_acres
        FROM fact_landuse_transitions f
        JOIN dim_scenario s ON f.scenario_id = s.scenario_id
        JOIN dim_landuse l_from ON f.from_landuse_id = l_from.landuse_id
        JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
        GROUP BY s.scenario_name, s.rcp_scenario, s.ssp_scenario, l_from.landuse_name, l_to.landuse_name
    """,
    "time_series": """
        SELECT
            t.start_year as year,
            l_from.landuse_name as from_landuse,
            l_to.landuse_name as to_landuse,
            SUM(CAST(f.acres AS DOUBLE)) as total_acres
        FROM fact_landuse_transitions f
        JOIN dim_time t ON f.time_id = t.time_id
        JOIN dim_landuse l_from ON f.from_landuse_id = l_from.landuse_id
        JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
        GROUP BY t.start_year, l_from.landuse_name, l_to.landuse_name
        ORDER BY t.start_year
    """,
}


@router.get("/templates")
async def get_extraction_templates():
    """Get predefined extraction templates."""
    return {
        "templates": [
            {
                "id": "agricultural_transitions",
                "name": "Agricultural Transitions",
                "description": "All transitions involving crop and pasture land",
                "estimated_rows": 1500000,
            },
            {
                "id": "urbanization_data",
                "name": "Urbanization Data",
                "description": "All land converting to urban use",
                "estimated_rows": 800000,
            },
            {
                "id": "forest_changes",
                "name": "Forest Changes",
                "description": "All forest gains and losses",
                "estimated_rows": 1200000,
            },
            {
                "id": "state_summaries",
                "name": "State Summaries",
                "description": "Aggregated data by state",
                "estimated_rows": 3000,
            },
            {
                "id": "scenario_comparison",
                "name": "Climate Scenario Comparison",
                "description": "Summary by climate scenario",
                "estimated_rows": 2000,
            },
            {
                "id": "time_series",
                "name": "Time Series Data",
                "description": "National trends over time",
                "estimated_rows": 600,
            },
        ]
    }


@router.get("/filters")
async def get_filter_options(
    db_service: DatabaseService = Depends(get_database_service_singleton),
):
    """Get available filter options for extraction."""
    try:
        # Get scenarios from database
        _, scenarios_data, _ = db_service.execute_query(
            "SELECT DISTINCT scenario_name FROM dim_scenario ORDER BY scenario_name"
        )
        scenarios = [{"id": s["scenario_name"], "name": s["scenario_name"]} for s in scenarios_data]

        # Get states from database
        _, states_data, _ = db_service.execute_query(
            "SELECT DISTINCT state_abbrev, state_name FROM dim_geography ORDER BY state_name"
        )
        states = [{"id": s["state_abbrev"], "name": s["state_name"]} for s in states_data]

        # Get land use types
        _, landuse_data, _ = db_service.execute_query(
            "SELECT DISTINCT landuse_name FROM dim_landuse ORDER BY landuse_name"
        )
        land_use_types = [{"id": l["landuse_name"].lower(), "name": l["landuse_name"]} for l in landuse_data]

        # Get time periods
        _, time_data, _ = db_service.execute_query(
            "SELECT DISTINCT start_year FROM dim_time ORDER BY start_year"
        )
        time_periods = [{"id": str(t["start_year"]), "name": str(t["start_year"])} for t in time_data]

        return {
            "scenarios": scenarios,
            "land_use_types": land_use_types,
            "time_periods": time_periods,
            "states": states,
        }
    except Exception as e:
        logger.error(f"Error getting filter options: {e}")
        # Return defaults if database unavailable
        return {
            "scenarios": [],
            "land_use_types": [
                {"id": "crop", "name": "Cropland"},
                {"id": "pasture", "name": "Pasture"},
                {"id": "forest", "name": "Forest"},
                {"id": "urban", "name": "Urban"},
                {"id": "rangeland", "name": "Rangeland"},
            ],
            "time_periods": [],
            "states": [],
        }


@router.post("/preview")
async def preview_extraction(
    request: ExtractionRequest,
    db_service: DatabaseService = Depends(get_database_service_singleton),
):
    """Preview extraction results before downloading."""
    try:
        if not db_service.is_available:
            raise HTTPException(status_code=503, detail="Database not available")

        # Build query
        query = _build_extraction_query(request)
        preview_query = f"{query} LIMIT 10"

        # Execute preview
        columns, data, _ = db_service.execute_query(preview_query, limit=10)

        # Get total count
        count_query = f"SELECT COUNT(*) as cnt FROM ({query}) subq"
        _, count_data, _ = db_service.execute_query(count_query, limit=1)
        total_count = count_data[0]["cnt"] if count_data else 0

        return ExtractionResponse(
            success=True,
            row_count=total_count,
            preview=data,
            columns=columns,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error previewing extraction: {e}")
        return ExtractionResponse(
            success=False,
            error=str(e),
            row_count=0,
        )


@router.post("/export")
async def export_data(
    request: ExtractionRequest,
    db_service: DatabaseService = Depends(get_database_service_singleton),
):
    """Export data in the requested format."""
    try:
        if not db_service.is_available:
            raise HTTPException(status_code=503, detail="Database not available")

        # Build query
        query = _build_extraction_query(request)

        # Get data (with higher limit for export)
        limit = request.limit or 100000
        columns, data, _ = db_service.execute_query(query, limit=limit)

        # Generate file based on format
        format_type = request.format or "csv"

        if format_type == "csv":
            return _generate_csv_response(columns, data, request.template_id or "export")
        elif format_type == "json":
            return _generate_json_response(data, request.template_id or "export")
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported format: {format_type}")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _build_extraction_query(request: ExtractionRequest) -> str:
    """Build SQL query from extraction request."""
    # Start with template query if specified
    if request.template_id and request.template_id in EXTRACTION_QUERIES:
        base_query = EXTRACTION_QUERIES[request.template_id]
    elif request.custom_query:
        # Validate custom query is SELECT only
        if not request.custom_query.strip().upper().startswith("SELECT"):
            raise ValueError("Only SELECT queries are allowed")
        base_query = request.custom_query
    else:
        # Default query
        base_query = """
            SELECT
                g.state_name,
                g.county_name,
                s.scenario_name,
                t.start_year as year,
                l_from.landuse_name as from_landuse,
                l_to.landuse_name as to_landuse,
                CAST(f.acres AS DOUBLE) as acres
            FROM fact_landuse_transitions f
            JOIN dim_geography g ON f.geography_id = g.geography_id
            JOIN dim_scenario s ON f.scenario_id = s.scenario_id
            JOIN dim_time t ON f.time_id = t.time_id
            JOIN dim_landuse l_from ON f.from_landuse_id = l_from.landuse_id
            JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
        """

    # Add filters if provided
    conditions = []

    if request.states:
        states_list = ", ".join(f"'{s}'" for s in request.states)
        conditions.append(f"g.state_abbrev IN ({states_list})")

    if request.scenarios:
        scenarios_list = ", ".join(f"'{s}'" for s in request.scenarios)
        conditions.append(f"s.scenario_name IN ({scenarios_list})")

    if request.time_periods:
        periods_list = ", ".join(str(p) for p in request.time_periods)
        conditions.append(f"t.start_year IN ({periods_list})")

    if request.land_use_types:
        types_list = ", ".join(f"'{t.capitalize()}'" for t in request.land_use_types)
        conditions.append(
            f"(l_from.landuse_name IN ({types_list}) OR l_to.landuse_name IN ({types_list}))"
        )

    if conditions:
        # Check if base query already has WHERE
        if "WHERE" in base_query.upper():
            base_query += " AND " + " AND ".join(conditions)
        else:
            base_query += " WHERE " + " AND ".join(conditions)

    return base_query


def _generate_csv_response(columns: List[str], data: List[Dict[str, Any]], filename: str):
    """Generate CSV streaming response."""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    writer.writerows(data)
    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}.csv"},
    )


def _generate_json_response(data: List[Dict[str, Any]], filename: str):
    """Generate JSON streaming response."""
    json_data = json.dumps(data, indent=2)

    return StreamingResponse(
        iter([json_data]),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}.json"},
    )
