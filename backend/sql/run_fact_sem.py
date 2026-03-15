"""
Runner: Create / refresh bhg_sem_report.fact_sem_sessions_daily
---------------------------------------------------------------
Dynamically reads ALL connected GA4 datasets from data_sources.json
and generates UNION ALL across them. When you add a new site via the
Settings UI, the next run automatically includes it.

Usage:
    # Full rebuild (CREATE OR REPLACE — all history for yesterday)
    python -m backend.sql.run_fact_sem --full

    # Incremental: append a specific date
    python -m backend.sql.run_fact_sem --date 2026-03-14

    # Backfill a date range
    python -m backend.sql.run_fact_sem --from 2026-01-01 --to 2026-03-14

    # Default (no flags): incremental for yesterday
    python -m backend.sql.run_fact_sem
"""

import argparse
import json
import os
import sys
import logging
from datetime import date, timedelta
from pathlib import Path

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

PROJECT  = "bygghemma-bigdata"
DATASET  = "bhg_sem_report"
TABLE    = "fact_sem_sessions_daily"
FULL_TABLE = f"{PROJECT}.{DATASET}.{TABLE}"

# Google Ads MCC tables
GADS_DATASET        = "gad_bygghemma_mcc_1502879059_new_api"
GADS_CAMPAIGN_STATS = "ads_CampaignBasicStats_1502879059"
GADS_CAMPAIGN_TABLE = "ads_Campaign_1502879059"

# Path to local config (fallback if BQ config unavailable)
CONFIG_PATH = Path(__file__).resolve().parent.parent / "data_sources.json"


# ── Helpers ──────────────────────────────────────────────────────────────

def get_client() -> bigquery.Client:
    creds_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if creds_json:
        from google.oauth2.service_account import Credentials
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        return bigquery.Client(credentials=creds, project=PROJECT)
    return bigquery.Client(project=PROJECT)


def load_sources() -> list[dict]:
    """Load connected GA4 sources. Tries BQ config first, then local file."""
    sources = []

    # Try BQ config table first (works on Cloud Run)
    try:
        client = get_client()
        sql = f"SELECT config_json FROM `{PROJECT}.app_config.data_sources` ORDER BY updated_at DESC LIMIT 1"
        rows = list(client.query(sql, location="EU").result())
        if rows:
            config = json.loads(rows[0]["config_json"])
            sources = [s for s in config.get("sources", []) if s.get("status") == "connected"]
            if sources:
                log.info("Loaded %d source(s) from BQ config: %s",
                         len(sources), ", ".join(s["label"] for s in sources))
                return sources
    except Exception as e:
        log.warning("Could not read BQ config: %s", e)

    # Fall back to local file
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)
        sources = [s for s in config.get("sources", []) if s.get("status") == "connected"]
        if sources:
            log.info("Loaded %d source(s) from local config: %s",
                     len(sources), ", ".join(s["label"] for s in sources))
            return sources

    log.error("No connected sources found in any config.")
    return []


def _vat_divisor(source: dict) -> float:
    return 1.25 if source.get("vat_status") == "inc_vat" else 1.0


# ── SQL Generation ──────────────────────────────────────────────────────

def _source_cte(source: dict, suffix_start: str, suffix_end: str, idx: int) -> str:
    """Generate the CTE block for one GA4 dataset."""
    ds = source["dataset_id"]
    label = source.get("label", ds)
    company = source.get("company", "Unknown")
    ba = source.get("business_area", "Uncategorized")
    customer_id = source.get("gads_customer_id", "")
    vat_status = source.get("vat_status", "ex_vat")
    vat_rate = 1.25 if vat_status == "inc_vat" else 1.0

    return f"""
    -- ── {label} ({ds}) ──
    cpc_{idx} AS (
      SELECT
        PARSE_DATE('%Y%m%d', event_date)                                       AS date,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        collected_traffic_source.manual_campaign_name                           AS campaign_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS landing_page
      FROM `{PROJECT}.{ds}.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{suffix_start}' AND '{suffix_end}'
        AND event_name = 'session_start'
        AND collected_traffic_source.manual_source = 'google'
        AND collected_traffic_source.manual_medium = 'cpc'
    ),
    sessions_{idx} AS (
      SELECT date, user_pseudo_id, ga_session_id,
             ANY_VALUE(campaign_name) AS campaign_name,
             ANY_VALUE(landing_page)  AS landing_page
      FROM cpc_{idx}
      GROUP BY date, user_pseudo_id, ga_session_id
    ),
    purch_{idx} AS (
      SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        ecommerce.transaction_id,
        ecommerce.purchase_revenue AS revenue_gross
      FROM `{PROJECT}.{ds}.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '{suffix_start}' AND '{suffix_end}'
        AND event_name = 'purchase'
    ),
    conv_{idx} AS (
      SELECT
        s.date, s.user_pseudo_id, s.ga_session_id,
        COUNT(DISTINCT p.transaction_id)    AS transactions,
        COALESCE(SUM(p.revenue_gross), 0)   AS revenue_gross
      FROM sessions_{idx} s
      LEFT JOIN purch_{idx} p
        ON s.user_pseudo_id = p.user_pseudo_id AND s.ga_session_id = p.ga_session_id
      GROUP BY 1, 2, 3
    ),
    final_{idx} AS (
      SELECT
        c.date,
        '{label}'       AS site,
        '{company}'     AS company,
        '{ba}'          AS business_area,
        c.user_pseudo_id,
        c.ga_session_id,
        s.campaign_name,
        s.landing_page,
        CASE
          -- TODO: Override with campaign_id list for higher precision:
          -- WHEN campaign_id IN (123, 456) THEN 'Brand'
          WHEN LOWER(COALESCE(s.campaign_name, '')) LIKE '%brand%' THEN 'Brand'
          ELSE 'Non-Brand'
        END AS campaign_segment,
        c.transactions,
        -- Revenue: keep raw + calculated net
        ROUND(c.revenue_gross, 2)                AS revenue_gross,
        '{vat_status}'                           AS vat_status,
        {vat_rate}                               AS vat_rate,
        ROUND(c.revenue_gross / {vat_rate}, 2)   AS revenue_net,
        -- Ads cost
        ROUND(COALESCE(ac.cost, 0), 2) AS cost,
        ROUND(SAFE_DIVIDE(COALESCE(ac.cost, 0), NULLIF(c.revenue_gross / {vat_rate}, 0)) * 100, 2) AS cos_pct
      FROM conv_{idx} c
      JOIN sessions_{idx} s
        ON c.date = s.date AND c.user_pseudo_id = s.user_pseudo_id AND c.ga_session_id = s.ga_session_id
      LEFT JOIN ads_cost ac
        ON c.date = ac.date
        AND s.campaign_name = ac.ads_campaign_name
        AND ac.customer_id = {customer_id if customer_id else 0}
    )"""


def build_sql(sources: list[dict], suffix_start: str, suffix_end: str,
              start_date_str: str, end_date_str: str) -> str:
    """Build the full SQL with UNION ALL across all sources."""

    # Ads CTEs (shared across all sources)
    ads_ctes = f"""
    ads_campaigns AS (
      SELECT campaign_id, customer_id, campaign_name
      FROM `{PROJECT}.{GADS_DATASET}.{GADS_CAMPAIGN_TABLE}`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id, customer_id ORDER BY _DATA_DATE DESC) = 1
    ),
    ads_cost AS (
      SELECT
        s.segments_date        AS date,
        c.campaign_name        AS ads_campaign_name,
        s.customer_id,
        SUM(s.metrics_cost_micros) / 1000000 AS cost
      FROM `{PROJECT}.{GADS_DATASET}.{GADS_CAMPAIGN_STATS}` s
      JOIN ads_campaigns c ON s.campaign_id = c.campaign_id AND s.customer_id = c.customer_id
      WHERE s.segments_date BETWEEN '{start_date_str}' AND '{end_date_str}'
      GROUP BY 1, 2, 3
    )"""

    # Per-source CTEs
    source_ctes = []
    union_parts = []
    for idx, src in enumerate(sources):
        source_ctes.append(_source_cte(src, suffix_start, suffix_end, idx))
        union_parts.append(f"SELECT * FROM final_{idx}")

    all_ctes = ",\n".join([ads_ctes] + source_ctes)
    union_sql = "\nUNION ALL\n".join(union_parts)

    return f"WITH\n{all_ctes}\n\n{union_sql}"


# ── Execution modes ─────────────────────────────────────────────────────

def ensure_dataset(client: bigquery.Client):
    """Create the target dataset if it doesn't exist."""
    ds = bigquery.Dataset(f"{PROJECT}.{DATASET}")
    ds.location = "EU"  # Match GA4 export location
    client.create_dataset(ds, exists_ok=True)
    log.info("Dataset %s.%s ready.", PROJECT, DATASET)


def run_full_rebuild(client: bigquery.Client, sources: list[dict]):
    """Full CREATE OR REPLACE TABLE with yesterday's data."""
    run_date = date.today() - timedelta(days=1)
    suffix = run_date.strftime("%Y%m%d")
    date_str = run_date.isoformat()

    inner = build_sql(sources, suffix, suffix, date_str, date_str)

    sql = f"""
    CREATE OR REPLACE TABLE `{FULL_TABLE}`
    PARTITION BY date
    CLUSTER BY site, campaign_segment
    AS
    {inner}
    """

    log.info("Full rebuild for %s (%d sources)...", date_str, len(sources))
    job = client.query(sql)
    job.result()
    log.info("Done! Table created: %s", FULL_TABLE)


def run_incremental(client: bigquery.Client, sources: list[dict], run_date: date):
    """Delete + insert for a specific date (idempotent)."""
    suffix = run_date.strftime("%Y%m%d")
    date_str = run_date.isoformat()

    inner = build_sql(sources, suffix, suffix, date_str, date_str)

    sql = f"""
    DELETE FROM `{FULL_TABLE}` WHERE date = '{date_str}';

    INSERT INTO `{FULL_TABLE}`
    {inner};
    """

    log.info("Incremental load for %s ...", date_str)
    job = client.query(sql)
    job.result()
    log.info("Done for %s.", date_str)


# ── CLI ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Run fact_sem_sessions_daily")
    parser.add_argument("--full", action="store_true", help="Full rebuild (CREATE OR REPLACE)")
    parser.add_argument("--date", type=str, help="Run for a specific date (YYYY-MM-DD)")
    parser.add_argument("--from", dest="from_date", type=str, help="Backfill start date")
    parser.add_argument("--to", dest="to_date", type=str, help="Backfill end date")
    args = parser.parse_args()

    client = get_client()
    sources = load_sources()
    ensure_dataset(client)

    if args.full:
        run_full_rebuild(client, sources)
    elif args.from_date and args.to_date:
        d = date.fromisoformat(args.from_date)
        end = date.fromisoformat(args.to_date)
        total = (end - d).days + 1
        done = 0
        while d <= end:
            run_incremental(client, sources, d)
            done += 1
            log.info("Progress: %d/%d", done, total)
            d += timedelta(days=1)
    elif args.date:
        run_incremental(client, sources, date.fromisoformat(args.date))
    else:
        run_incremental(client, sources, date.today() - timedelta(days=1))


if __name__ == "__main__":
    main()
