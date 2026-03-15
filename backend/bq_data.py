"""
BHG SEM Report – BigQuery Data Layer (FastAPI)
----------------------------------------------
Queries the pre-built staging table `fact_sem_sessions_daily` for all
SEM performance metrics.  This replaced direct GA4 event queries and is
significantly faster and cheaper.
"""

import os
import logging
import pandas as pd
import numpy as np
from datetime import date, timedelta
from google.cloud import bigquery
from cachetools import TTLCache

from . import ga4_connector
from .date_engine import DateEngine

logger = logging.getLogger(__name__)

# Staging table
PROJECT = "bygghemma-bigdata"
STAGING_TABLE = f"`{PROJECT}.bhg_sem_report.fact_sem_sessions_daily`"

# Google Ads tables (for clicks and cost)
GADS_DATASET = "gad_bygghemma_mcc_1502879059_new_api"
GADS_STATS = f"`{PROJECT}.{GADS_DATASET}.ads_CampaignBasicStats_1502879059`"
GADS_CAMPAIGNS = f"`{PROJECT}.{GADS_DATASET}.ads_Campaign_1502879059`"


def _get_customer_ids(company: str = None, site: str = None) -> list[str]:
    """Get Google Ads customer IDs for the given site/company filter."""
    sources = ga4_connector.get_connected_sources()
    if site and site != "All Sites":
        sources = [s for s in sources if s.get("label") == site]
    elif company and company != "All Companies":
        sources = [s for s in sources if s.get("company") == company]
    return [s["gads_customer_id"] for s in sources if s.get("gads_customer_id")]


def _ads_clicks_sql(start: str, end: str, customer_ids: list[str]) -> str:
    """Build a scalar subquery for Google Ads clicks."""
    if not customer_ids:
        return "0"
    ids = ", ".join(customer_ids)
    return f"""(SELECT COALESCE(SUM(metrics_clicks), 0)
     FROM {GADS_STATS}
     WHERE segments_date BETWEEN '{start}' AND '{end}'
       AND customer_id IN ({ids}))"""


def _ads_cost_sql(start: str, end: str, customer_ids: list[str]) -> str:
    """Build a scalar subquery for Google Ads cost (direct from Ads tables)."""
    if not customer_ids:
        return "0"
    ids = ", ".join(customer_ids)
    return f"""(SELECT COALESCE(SUM(metrics_cost_micros), 0) / 1000000
     FROM {GADS_STATS}
     WHERE segments_date BETWEEN '{start}' AND '{end}'
       AND customer_id IN ({ids}))"""

# Module-level client
_BQ_CLIENT: bigquery.Client | None = None

# TTL cache for queries (max 200 entries, 1 hour TTL)
_query_cache: TTLCache = TTLCache(maxsize=200, ttl=3600)


def init_client() -> bigquery.Client:
    """Initialize BigQuery client. Called on app startup."""
    global _BQ_CLIENT
    if _BQ_CLIENT is not None:
        return _BQ_CLIENT

    project = ga4_connector.get_project_id()
    creds_json = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if creds_json:
        import json
        from google.oauth2.service_account import Credentials
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        _BQ_CLIENT = bigquery.Client(credentials=creds, project=project)
    else:
        _BQ_CLIENT = bigquery.Client(project=project)
    logger.info(f"BigQuery client initialized for project {project}")
    return _BQ_CLIENT


def _get_client() -> bigquery.Client:
    global _BQ_CLIENT
    if _BQ_CLIENT is not None:
        return _BQ_CLIENT
    return init_client()


# =====================================================================
# CACHED QUERY RUNNER
# =====================================================================
def _run_query(sql: str) -> pd.DataFrame:
    """Run a BigQuery query with TTL caching."""
    cache_key = hash(sql)
    if cache_key in _query_cache:
        return _query_cache[cache_key]
    try:
        client = _get_client()
        df = client.query(sql).to_dataframe()
        _query_cache[cache_key] = df
        return df
    except Exception as e:
        logger.error(f"BigQuery error: {e}")
        return pd.DataFrame()


# =====================================================================
# HIERARCHY & FILTER HELPERS
# =====================================================================
def get_hierarchy() -> dict:
    sources = ga4_connector.get_connected_sources()
    hierarchy = {}
    for s in sources:
        ba = s.get("business_area", "Uncategorized")
        co = s.get("company", "Unknown")
        label = s.get("label", s["dataset_id"])
        hierarchy.setdefault(ba, {}).setdefault(co, []).append(label)
    return hierarchy


def get_companies() -> list[str]:
    sources = ga4_connector.get_connected_sources()
    return list({s.get("company", "Unknown") for s in sources})


def get_sites_for_company(company: str) -> list[str]:
    sources = ga4_connector.get_connected_sources()
    return [s.get("label", s["dataset_id"]) for s in sources if s.get("company") == company]


def _site_filter(company: str = None, site: str = None) -> str:
    """Build a WHERE clause fragment for site/company filtering."""
    if site and site != "All Sites":
        safe_site = site.replace("'", "\\'")
        return f"AND site = '{safe_site}'"
    if company and company != "All Companies":
        safe_company = company.replace("'", "\\'")
        return f"AND company = '{safe_company}'"
    return ""


# =====================================================================
# KPI SUMMARY
# =====================================================================
def get_kpi_summary(
    start_date: date, end_date: date, company: str = None, site: str = None
) -> dict:
    yoy_start, yoy_end = DateEngine.get_yoy_dates(start_date, end_date)
    site_filter = _site_filter(company, site)
    customer_ids = _get_customer_ids(company, site)

    cost_sub = _ads_cost_sql(start_date, end_date, customer_ids)
    clicks_sub = _ads_clicks_sql(start_date, end_date, customer_ids)
    sql = f"""
    SELECT
      {clicks_sub}         AS clicks,
      SUM(transactions)  AS transactions,
      SUM(revenue_net)   AS revenue,
      {cost_sub}         AS cost
    FROM {STAGING_TABLE}
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
      {site_filter}
    """
    df = _run_query(sql)

    yoy_cost_sub = _ads_cost_sql(yoy_start, yoy_end, customer_ids)
    yoy_clicks_sub = _ads_clicks_sql(yoy_start, yoy_end, customer_ids)
    yoy_sql = f"""
    SELECT
      SUM(revenue_net) AS revenue,
      {yoy_cost_sub}   AS cost,
      {yoy_clicks_sub} AS clicks,
      SUM(transactions) AS transactions
    FROM {STAGING_TABLE}
    WHERE date BETWEEN '{yoy_start}' AND '{yoy_end}'
      {site_filter}
    """
    yoy_df = _run_query(yoy_sql)

    if df.empty:
        return _empty_kpi()

    def _safe(val, as_type=float):
        """Handle pd.NA / None / NaN safely."""
        try:
            if pd.isna(val):
                return as_type(0)
        except (TypeError, ValueError):
            pass
        return as_type(val or 0)

    r = df.iloc[0]
    rev = round(_safe(r["revenue"]), 2)
    cost = round(_safe(r["cost"]), 2)
    cos = round((cost / rev * 100), 2) if rev else 0

    yr = yoy_df.iloc[0] if not yoy_df.empty else {}
    yoy_rev = round(_safe(yr.get("revenue", 0)), 2)
    yoy_cost = round(_safe(yr.get("cost", 0)), 2)
    yoy_cos = round((yoy_cost / yoy_rev * 100), 2) if yoy_rev else 0

    return {
        "clicks": _safe(r["clicks"], int),
        "revenue": rev,
        "cost": cost,
        "cos": cos,
        "transactions": _safe(r["transactions"], int),
        "yoy_clicks": _safe(yr.get("clicks", 0), int),
        "yoy_revenue": yoy_rev,
        "yoy_cost": yoy_cost,
        "yoy_cos": yoy_cos,
        "yoy_transactions": _safe(yr.get("transactions", 0), int),
    }


def _empty_kpi() -> dict:
    return {"clicks": 0, "revenue": 0, "cost": 0, "cos": 0, "transactions": 0,
            "yoy_clicks": 0, "yoy_revenue": 0, "yoy_cost": 0, "yoy_cos": 0,
            "yoy_transactions": 0}


# =====================================================================
# SEGMENTED PERFORMANCE (Brand / Non-Brand)
# =====================================================================
def get_segmented_performance(
    start_date: date, end_date: date, company: str = None, site: str = None
) -> pd.DataFrame:
    yoy_start, yoy_end = DateEngine.get_yoy_dates(start_date, end_date)
    site_filter = _site_filter(company, site)
    customer_ids = _get_customer_ids(company, site)
    ids_list = ", ".join(customer_ids) if customer_ids else "0"

    sql = f"""
    WITH
    ads_clicks_seg AS (
      SELECT
        CASE WHEN LOWER(COALESCE(c.campaign_name, '')) LIKE '%brand%' THEN 'Brand' ELSE 'Non-Brand' END AS segment,
        SUM(s.metrics_clicks) AS clicks,
        SUM(s.metrics_cost_micros) / 1000000 AS cost
      FROM {GADS_STATS} s
      JOIN (SELECT campaign_id, customer_id, campaign_name FROM {GADS_CAMPAIGNS}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id, customer_id ORDER BY _DATA_DATE DESC) = 1) c
        ON s.campaign_id = c.campaign_id AND s.customer_id = c.customer_id
      WHERE s.segments_date BETWEEN '{start_date}' AND '{end_date}'
        AND s.customer_id IN ({ids_list})
      GROUP BY 1
    ),
    cur AS (
      SELECT
        campaign_segment AS segment,
        SUM(transactions) AS transactions,
        SUM(revenue_net)  AS revenue
      FROM {STAGING_TABLE}
      WHERE date BETWEEN '{start_date}' AND '{end_date}'
        {site_filter}
      GROUP BY 1
    ),
    yoy AS (
      SELECT campaign_segment AS segment, SUM(revenue_net) AS revenue
      FROM {STAGING_TABLE}
      WHERE date BETWEEN '{yoy_start}' AND '{yoy_end}' {site_filter}
      GROUP BY 1
    )
    SELECT c.segment, COALESCE(ac.clicks, 0) AS clicks, c.transactions, c.revenue,
           COALESCE(ac.cost, 0) AS cost,
           COALESCE(y.revenue, 0) AS yoy_revenue
    FROM cur c
    LEFT JOIN ads_clicks_seg ac ON c.segment = ac.segment
    LEFT JOIN yoy y ON c.segment = y.segment
    ORDER BY c.segment
    """
    df = _run_query(sql)
    if df.empty:
        return pd.DataFrame(columns=["Segment", "Clicks", "Transactions", "CR %",
                                      "Revenue (SEK)", "Cost (SEK)", "CoS %", "Revenue YoY %"])

    rows = []
    for _, r in df.iterrows():
        clicks = int(r["clicks"])
        txn = int(r["transactions"])
        rev = float(r["revenue"])
        cr = (txn / clicks * 100) if clicks else 0
        yoy_rev = float(r["yoy_revenue"])
        rev_yoy = ((rev / yoy_rev - 1) * 100) if yoy_rev else 0
        seg_cost = float(r["cost"])
        cos = (seg_cost / rev * 100) if rev else 0

        rows.append({
            "Segment": r["segment"],
            "Clicks": clicks,
            "Transactions": txn,
            "CR %": round(cr, 2),
            "Revenue (SEK)": round(rev),
            "Cost (SEK)": round(seg_cost),
            "CoS %": round(cos, 2),
            "Revenue YoY %": round(rev_yoy, 1),
        })

    return pd.DataFrame(rows)


# =====================================================================
# WEEKLY PERFORMANCE
# =====================================================================
def get_weekly_performance(
    start_date: date, end_date: date, company: str = None, site: str = None
) -> pd.DataFrame:
    week_start = end_date - timedelta(weeks=12)
    yoy_offset = timedelta(days=364)
    yoy_week_start = week_start - yoy_offset
    site_filter = _site_filter(company, site)
    customer_ids = _get_customer_ids(company, site)
    ids_list = ", ".join(customer_ids) if customer_ids else "0"

    sql = f"""
    WITH
    ads_clicks_wk AS (
      SELECT FORMAT_DATE('%G-W%V', s.segments_date) AS wk,
             SUM(s.metrics_clicks) AS clicks,
             SUM(s.metrics_cost_micros) / 1000000 AS cost
      FROM {GADS_STATS} s
      WHERE s.segments_date BETWEEN '{week_start}' AND '{end_date}'
        AND s.customer_id IN ({ids_list})
      GROUP BY 1
    ),
    cur_weekly AS (
      SELECT
        FORMAT_DATE('%G-W%V', date) AS wk,
        SUM(revenue_net) AS revenue
      FROM {STAGING_TABLE}
      WHERE date BETWEEN '{week_start}' AND '{end_date}'
        {site_filter}
      GROUP BY 1
    ),
    yoy_weekly AS (
      SELECT
        FORMAT_DATE('%G-W%V', DATE_ADD(date, INTERVAL 364 DAY)) AS wk,
        SUM(revenue_net) AS revenue
      FROM {STAGING_TABLE}
      WHERE date BETWEEN '{yoy_week_start}' AND '{end_date - yoy_offset}'
        {site_filter}
      GROUP BY 1
    )
    SELECT
      c.wk AS week, COALESCE(ac.clicks, 0) AS clicks, c.revenue, COALESCE(ac.cost, 0) AS cost,
      SAFE_DIVIDE(c.revenue - COALESCE(y.revenue, 0),
                  NULLIF(COALESCE(y.revenue, 0), 0)) * 100 AS rev_yoy_pct
    FROM cur_weekly c
    LEFT JOIN ads_clicks_wk ac ON c.wk = ac.wk
    LEFT JOIN yoy_weekly y ON c.wk = y.wk
    WHERE c.wk IS NOT NULL
    ORDER BY c.wk DESC
    """
    df = _run_query(sql)
    if df.empty:
        return pd.DataFrame(columns=["Week", "Clicks", "Revenue (SEK)",
                                      "Cost (SEK)", "CoS %", "Revenue YoY %"])

    df = df.rename(columns={"week": "Week", "clicks": "Clicks", "revenue": "Revenue (SEK)",
                             "cost": "Cost (SEK)", "rev_yoy_pct": "Revenue YoY %"})
    df["CoS %"] = df.apply(lambda row: round(row["Cost (SEK)"] / row["Revenue (SEK)"] * 100, 2)
                            if row["Revenue (SEK)"] else 0, axis=1)
    df["Revenue YoY %"] = df["Revenue YoY %"].fillna(0).round(1)
    return df[["Week", "Clicks", "Revenue (SEK)", "Cost (SEK)", "CoS %", "Revenue YoY %"]]


# =====================================================================
# DAILY METRICS (for charts)
# =====================================================================
def get_daily_cos(
    start_date: date, end_date: date, company: str = None, site: str = None
) -> pd.DataFrame:
    site_filter = _site_filter(company, site)
    customer_ids = _get_customer_ids(company, site)
    ids_list = ", ".join(customer_ids) if customer_ids else "0"

    sql = f"""
    WITH
    ads_cost_daily AS (
      SELECT s.segments_date AS date, SUM(s.metrics_cost_micros) / 1000000 AS cost
      FROM {GADS_STATS} s
      WHERE s.segments_date BETWEEN '{start_date}' AND '{end_date}'
        AND s.customer_id IN ({ids_list})
      GROUP BY 1
    )
    SELECT
      s.date              AS Date,
      SUM(s.revenue_net)  AS Revenue,
      COALESCE(ac.cost, 0) AS Cost
    FROM {STAGING_TABLE} s
    LEFT JOIN ads_cost_daily ac ON s.date = ac.date
    WHERE s.date BETWEEN '{start_date}' AND '{end_date}'
      {site_filter}
    GROUP BY 1, 3
    ORDER BY 1
    """
    df = _run_query(sql)
    if df.empty:
        return pd.DataFrame(columns=["Date", "Revenue", "Cost", "CoS %"])

    df["CoS %"] = df.apply(
        lambda r: round(r["Cost"] / r["Revenue"] * 100, 2) if r["Revenue"] else 0, axis=1
    )
    return df


# =====================================================================
# CUMULATIVE METRICS (for line chart)
# =====================================================================
def get_cumulative_cos(
    start_date: date, end_date: date, company: str = None, site: str = None
) -> pd.DataFrame:
    daily = get_daily_cos(start_date, end_date, company, site)
    if daily.empty:
        return pd.DataFrame(columns=["Date", "Cumulative Revenue",
                                      "Cumulative CoS %", "Budget Target %"])
    daily = daily.sort_values("Date")
    daily["Cumulative Revenue"] = daily["Revenue"].cumsum()
    daily["Cumulative Cost"] = daily["Cost"].cumsum()
    daily["Cumulative CoS %"] = daily.apply(
        lambda r: round(r["Cumulative Cost"] / r["Cumulative Revenue"] * 100, 2)
        if r["Cumulative Revenue"] else 0, axis=1
    )
    daily["Budget Target %"] = 9.0
    return daily


# =====================================================================
# DATA LOADER – Site Deep-Dive
# =====================================================================
def get_site_deep_dive_data(
    start_date: date, end_date: date, company: str = None, site: str = None
) -> tuple:
    """Fetch KPI, segmented, weekly, daily and cumulative CoS data."""
    kpi  = get_kpi_summary(start_date, end_date, company, site)
    seg  = get_segmented_performance(start_date, end_date, company, site)
    wk   = get_weekly_performance(start_date, end_date, company, site)
    cos  = get_daily_cos(start_date, end_date, company, site)
    cum  = get_cumulative_cos(start_date, end_date, company, site)
    return kpi, seg, wk, cos, cum


# =====================================================================
# PORTFOLIO GRID
# =====================================================================
def get_portfolio_grid(start_date: date, end_date: date) -> pd.DataFrame:
    yoy_start, yoy_end = DateEngine.get_yoy_dates(start_date, end_date)

    # Build per-site ads clicks
    sources = ga4_connector.get_connected_sources()
    all_customer_ids = [s["gads_customer_id"] for s in sources if s.get("gads_customer_id")]
    all_ids = ", ".join(all_customer_ids) if all_customer_ids else "0"

    sql = f"""
    WITH
    cur AS (
      SELECT
        business_area, company, site,
        SUM(revenue_net)  AS revenue
      FROM {STAGING_TABLE}
      WHERE date BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY 1, 2, 3
    ),
    yoy AS (
      SELECT site, SUM(revenue_net) AS revenue
      FROM {STAGING_TABLE}
      WHERE date BETWEEN '{yoy_start}' AND '{yoy_end}'
      GROUP BY 1
    )
    SELECT
      c.business_area, c.company, c.site,
      c.revenue,
      COALESCE(y.revenue, 0) AS yoy_revenue
    FROM cur c
    LEFT JOIN yoy y ON c.site = y.site
    ORDER BY c.business_area, c.company, c.site
    """
    df = _run_query(sql)
    if not df.empty:
        # Map site → gads_customer_id → clicks & cost
        site_to_cid = {s.get("label"): s.get("gads_customer_id") for s in sources}
        ads_sql = f"""SELECT customer_id, SUM(metrics_clicks) AS clicks,
          SUM(metrics_cost_micros) / 1000000 AS cost FROM {GADS_STATS}
          WHERE segments_date BETWEEN '{start_date}' AND '{end_date}'
            AND customer_id IN ({all_ids}) GROUP BY 1"""
        ads_df = _run_query(ads_sql)
        cid_clicks = dict(zip(ads_df["customer_id"].astype(str), ads_df["clicks"])) if not ads_df.empty else {}
        cid_cost = dict(zip(ads_df["customer_id"].astype(str), ads_df["cost"])) if not ads_df.empty else {}

    if df.empty:
        return pd.DataFrame(columns=["Business Area", "Company", "Site",
                                      "Clicks", "Revenue (SEK)", "YoY Revenue (SEK)",
                                      "Cost (SEK)", "CoS %", "Revenue YoY %"])

    rows = []
    for _, r in df.iterrows():
        rev = float(r["revenue"] or 0)
        yoy_rev = float(r["yoy_revenue"] or 0)
        site_cid = site_to_cid.get(r["site"], "")
        site_clicks = int(cid_clicks.get(str(site_cid), 0))
        cost = float(cid_cost.get(str(site_cid), 0))
        cos = (cost / rev * 100) if rev else 0
        rev_yoy = ((rev / yoy_rev - 1) * 100) if yoy_rev else 0

        rows.append({
            "Business Area": r["business_area"],
            "Company": r["company"],
            "Site": r["site"],
            "Clicks": site_clicks,
            "Revenue (SEK)": round(rev),
            "YoY Revenue (SEK)": round(yoy_rev),
            "Cost (SEK)": round(cost),
            "CoS %": round(cos, 1),
            "Revenue YoY %": round(rev_yoy, 1),
        })

    return pd.DataFrame(rows)
