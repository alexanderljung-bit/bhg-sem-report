"""Date preset endpoints."""
from datetime import date
from fastapi import APIRouter, Query
from ..date_engine import DateEngine

router = APIRouter(tags=["dates"])


@router.get("/dates/presets")
def get_preset_dates(
    preset: str = Query("MTD"),
    reference_date: date = Query(None),
):
    """Get date range for a named preset."""
    start, end = DateEngine.get_preset_dates(preset, reference_date)
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "preset": preset,
    }


@router.get("/dates/presets/list")
def list_presets():
    """Available date presets."""
    return {
        "presets": [
            "MTD", "Last 7 Days", "Last 30 Days", "Last Month",
            "QTD", "YTD", "Last 3 Months", "Last 12 Months",
        ]
    }
