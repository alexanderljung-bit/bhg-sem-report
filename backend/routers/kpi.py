"""KPI & Deep-Dive endpoints."""
from datetime import date
from fastapi import APIRouter, Query
from .. import bq_data

router = APIRouter(tags=["kpi"])


@router.get("/deep-dive")
def get_deep_dive(
    start: date = Query(...),
    end: date = Query(...),
    company: str = Query(...),
    site: str = Query(...),
):
    """Full Site Deep-Dive data bundle."""
    kpi, seg_df, weekly_df, daily_df, cumulative_df = bq_data.get_site_deep_dive_data(
        start, end, company, site
    )
    return {
        "kpi": kpi,
        "segments": seg_df.to_dict(orient="records") if not seg_df.empty else [],
        "weekly": weekly_df.to_dict(orient="records") if not weekly_df.empty else [],
        "daily": daily_df.to_dict(orient="records") if not daily_df.empty else [],
        "cumulative": cumulative_df.to_dict(orient="records") if not cumulative_df.empty else [],
    }


@router.get("/hierarchy")
def get_hierarchy():
    """Company/site hierarchy for filter dropdowns."""
    return {
        "hierarchy": bq_data.get_hierarchy(),
        "companies": bq_data.get_companies(),
    }
