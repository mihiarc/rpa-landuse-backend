"""Analytics data endpoints for dashboard visualizations."""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.dependencies import get_database_service_singleton
from app.models.responses import AnalyticsResponse
from app.services.database_service import DatabaseService

router = APIRouter(prefix="/analytics")
logger = logging.getLogger(__name__)


@router.get("/overview")
async def get_overview(
    db_service: DatabaseService = Depends(get_database_service_singleton),
):
    """Get dashboard overview metrics."""
    try:
        data = db_service.get_analytics_data("overview")
        if data:
            return data[0]
        return {
            "total_counties": 0,
            "total_transitions": 0,
            "scenarios": 0,
            "time_periods": 0,
            "land_use_types": 0,
        }
    except Exception as e:
        logger.error(f"Error getting overview: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/forest-transitions", response_model=AnalyticsResponse)
async def get_forest_transitions(
    db_service: DatabaseService = Depends(get_database_service_singleton),
    state: Optional[str] = Query(None, description="Filter by state"),
    scenario: Optional[str] = Query(None, description="Filter by scenario"),
):
    """Get forest transition analysis data."""
    try:
        filters = {}
        if state:
            filters["state"] = state
        if scenario:
            filters["scenario"] = scenario

        data = db_service.get_analytics_data("forest_transitions", filters)

        # Calculate summary
        total_acres = sum(row.get("total_acres", 0) for row in data)
        primary_dest = data[0]["to_landuse"] if data else "Unknown"

        return AnalyticsResponse(
            data=data,
            summary={
                "total_forest_loss": total_acres,
                "primary_destination": primary_dest,
            },
        )
    except Exception as e:
        logger.error(f"Error getting forest transitions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/agricultural-impact", response_model=AnalyticsResponse)
async def get_agricultural_impact(
    db_service: DatabaseService = Depends(get_database_service_singleton),
    scenarios: Optional[List[str]] = Query(None, description="Filter by scenarios"),
    time_periods: Optional[List[str]] = Query(None, description="Filter by time periods"),
):
    """Get agricultural impact analysis data."""
    try:
        filters = {}
        if scenarios:
            filters["scenarios"] = scenarios
        if time_periods:
            filters["time_periods"] = time_periods

        data = db_service.get_analytics_data("agricultural_impact", filters)

        total_loss = sum(row.get("loss_acres", 0) for row in data)

        return AnalyticsResponse(
            data=data,
            summary={"total_agricultural_loss": total_loss},
        )
    except Exception as e:
        logger.error(f"Error getting agricultural impact: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scenario-comparison", response_model=AnalyticsResponse)
async def get_scenario_comparison(
    db_service: DatabaseService = Depends(get_database_service_singleton),
):
    """Get climate scenario comparison data."""
    try:
        data = db_service.get_analytics_data("scenario_comparison")

        return AnalyticsResponse(
            data=data,
            summary={"scenarios_analyzed": len(data)},
        )
    except Exception as e:
        logger.error(f"Error getting scenario comparison: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/geographic/{state}")
async def get_geographic_data(
    state: str,
    db_service: DatabaseService = Depends(get_database_service_singleton),
):
    """Get geographic visualization data for choropleth maps."""
    try:
        query = f"""
            SELECT
                g.fips_code as fips,
                g.county_name as name,
                SUM(CASE WHEN l_to.landuse_name = 'Urban' THEN CAST(f.acres AS DOUBLE) ELSE 0 END) as urban_growth
            FROM fact_landuse_transitions f
            JOIN dim_geography g ON f.geography_id = g.geography_id
            JOIN dim_landuse l_to ON f.to_landuse_id = l_to.landuse_id
            WHERE g.state_name = '{state}'
            GROUP BY g.fips_code, g.county_name
            ORDER BY urban_growth DESC
            LIMIT 100
        """
        _, data, _ = db_service.execute_query(query)

        avg_change = sum(row.get("urban_growth", 0) for row in data) / len(data) if data else 0

        return {
            "state": state,
            "counties": data,
            "summary": {"average_urban_growth": avg_change, "county_count": len(data)},
        }
    except Exception as e:
        logger.error(f"Error getting geographic data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/urbanization-sources", response_model=AnalyticsResponse)
async def get_urbanization_sources(
    db_service: DatabaseService = Depends(get_database_service_singleton),
):
    """Get data about sources of new urban land."""
    try:
        data = db_service.get_analytics_data("urbanization_sources")

        total = sum(row.get("total_acres", 0) for row in data)

        return AnalyticsResponse(
            data=data,
            summary={"total_urbanization": total},
        )
    except Exception as e:
        logger.error(f"Error getting urbanization sources: {e}")
        raise HTTPException(status_code=500, detail=str(e))
