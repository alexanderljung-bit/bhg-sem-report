"""Portfolio / Group Overview endpoint."""
from datetime import date
from fastapi import APIRouter, Query
from .. import bq_data

router = APIRouter(tags=["portfolio"])


@router.get("/portfolio")
def get_portfolio(
    start: date = Query(...),
    end: date = Query(...),
):
    """BHG Group Overview grid data."""
    df = bq_data.get_portfolio_grid(start, end)
    return {
        "rows": df.to_dict(orient="records") if not df.empty else [],
    }
