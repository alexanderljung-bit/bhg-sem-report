"""
ETL Router – Trigger staging table refresh via API
----------------------------------------------------
Endpoints:
  POST /api/etl/run          – Run incremental ETL for yesterday (default)
  POST /api/etl/run?date=2026-03-14  – Run for a specific date

Designed to be called by Cloud Scheduler daily.
"""

import logging
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Query, BackgroundTasks

router = APIRouter(tags=["etl"])
log = logging.getLogger(__name__)


def _run_etl_job(run_date: date):
    """Execute the ETL in a background thread so the API responds quickly."""
    from ..sql.run_fact_sem import get_client, load_sources, ensure_dataset, run_incremental

    try:
        client = get_client()
        sources = load_sources()
        if not sources:
            log.error("ETL: No sources found, skipping.")
            return
        ensure_dataset(client)
        run_incremental(client, sources, run_date)
        log.info("ETL completed for %s (%d sources)", run_date, len(sources))
    except Exception as e:
        log.error("ETL failed for %s: %s", run_date, e)


@router.post("/api/etl/run")
def trigger_etl(
    background_tasks: BackgroundTasks,
    run_date: Optional[str] = Query(None, alias="date", description="Date to process (YYYY-MM-DD). Default: yesterday."),
):
    """Trigger an incremental ETL run. Returns immediately; work runs in background."""
    target_date = date.fromisoformat(run_date) if run_date else date.today() - timedelta(days=1)
    background_tasks.add_task(_run_etl_job, target_date)
    return {
        "status": "started",
        "date": target_date.isoformat(),
        "message": f"ETL job queued for {target_date}. Check logs for progress.",
    }
