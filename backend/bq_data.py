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

# Cost lives at date+campaign+site grain, not session grain.
# This subquery deduplicates before summing.
_COST_SUBQUERY = f"""
(SELECT SUM(daily_cost) FROM (
  SELECT DISTINCT date, campaign_name, site, cost AS daily_cost
  FROM {STAGING_TABLE}
  WHERE date BETWEEN '{{start}}' AND '{{end}}'
    {{filter}}
))
"""

def _cost_sql(start: str, end: str, site_filter: str = "") -> str:
    """Build a deduplicated cost scalar subquery."""
    return f"""(SELECT SUM(daily_cost) FROM (
      SELECT DISTINCT date, campaign_name, site, cost AS daily_cost
      FROM {STAGING_TABLE}
      WHERE date BETWEEN '{start}' AND '{end}'
        {site_filter}
    ))"""

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

    cost_sub = _cost_sql(start_date, end_date, site_filter)
    sql = f"""
    SELECT
      COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(ga_session_id AS STRING))) AS clicks,
      SUM(transactions)  AS transactions,
      SUM(revenue_net)   AS revenue,
      {cost_sub}         AS cost
    FROM {STAGING_TABLE}
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
      {site_filter}
    """
    df = _run_query(sql)

    yoy_cost_sub = _cost_sql(yoy_start, yoy_end, site_filter)
    yoy_sql = f"""
    SELECT
      SUM(revenue_net) AS revenue,
      {yoy_cost_sub}   AS cost,
      COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(ga_session_id AS STRING))) AS clicks,
      SUM(transactions) AS transactions
    FROM {STAGING_TABLE}
    WHERE date BETWEEN '{yoy_start}' AND '{yoy_end}'
      {site_filter}
    """
    yoy_df = _run_query(yoy_sql)

    if df.empty:
        return _empty_kpi()

    r = df.iloc[0]
    rev = round(float(r["revenue"] or 0), 2)
    cost = round(float(r["cost"] or 0), 2)
    cos = round((cost / rev * 100), 2) if rev else 0

    yr = yoy_df.iloc[0] if not yoy_df.empty else {}
    yoy_rev = round(float(yr.get("revenue", 0) or 0), 2)
    yoy_cost = round(float(yr.get("cost", 0) or 0), 2)
    yoy_cos = round((yoy_cost / yoy_rev * 100), 2) if yoy_rev else 0

    return {
        "clicks": int(r["clicks"] or 0),
        "revenue": rev,
        "cost": cost,
        "cos": cos,
        "transactions": int(r["transactions"] or 0),
        "yoy_clicks": int(yr.get("clicks", 0) or 0),
        "yoy_revenue": yoy_rev,
        "yoy_cost": yoy_cost,
        "yoy_cos": yoy_cos,
        "yoy_transactions": int(yr.get("transactions", 0) or 0),
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

    sql = f"""
    WITH
    dedup_cost AS (
      SELECT campaign_segment, SUM(daily_cost) AS cost
      FROM (SELECT DISTINCT date, campaign_name, site, campaign_segment, cost AS daily_cost
            FROM {STAGING_TABLE}
            WHERE date BETWEEN '{start_date}' AND '{end_date}' {site_filter})
      GROUP BY 1
    ),
    cur AS (
      SELECT
        campaign_segment AS segment,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(ga_session_id AS STRING))) AS clicks,
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
    SELECT c.segment, c.clicks, c.transactions, c.revenue,
           COALESCE(dc.cost, 0) AS cost,
           COALESCE(y.revenue, 0) AS yoy_revenue
    FROM cur c
    LEFT JOIN dedup_cost dc ON c.segment = dc.campaign_segment
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

    sql = f"""
    WITH
    dedup_cost_wk AS (
      SELECT FORMAT_DATE('%G-W%V', date) AS wk, SUM(daily_cost) AS cost
      FROM (SELECT DISTINCT date, campaign_name, site, cost AS daily_cost
            FROM {STAGING_TABLE}
            WHERE date BETWEEN '{week_start}' AND '{end_date}' {site_filter})
      GROUP BY 1
    ),
    cur_weekly AS (
      SELECT
        FORMAT_DATE('%G-W%V', date) AS wk,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(ga_session_id AS STRING))) AS clicks,
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
      c.wk AS week, c.clicks, c.revenue, COALESCE(dc.cost, 0) AS cost,
      SAFE_DIVIDE(c.revenue - COALESCE(y.revenue, 0),
                  NULLIF(COALESCE(y.revenue, 0), 0)) * 100 AS rev_yoy_pct
    FROM cur_weekly c
    LEFT JOIN dedup_cost_wk dc ON c.wk = dc.wk
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

    sql = f"""
    WITH
    dedup_cost_daily AS (
      SELECT date, SUM(daily_cost) AS cost
      FROM (SELECT DISTINCT date, campaign_name, site, cost AS daily_cost
            FROM {STAGING_TABLE}
            WHERE date BETWEEN '{start_date}' AND '{end_date}' {site_filter})
      GROUP BY 1
    )
    SELECT
      s.date              AS Date,
      SUM(s.revenue_net)  AS Revenue,
      COALESCE(dc.cost, 0) AS Cost
    FROM {STAGING_TABLE} s
    LEFT JOIN dedup_cost_daily dc ON s.date = dc.date
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

    sql = f"""
    WITH
    dedup_cost_site AS (
      SELECT site, SUM(daily_cost) AS cost
      FROM (SELECT DISTINCT date, campaign_name, site, cost AS daily_cost
            FROM {STAGING_TABLE}
            WHERE date BETWEEN '{start_date}' AND '{end_date}')
      GROUP BY 1
    ),
    cur AS (
      SELECT
        business_area, company, site,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(ga_session_id AS STRING))) AS clicks,
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
      c.clicks, c.revenue, COALESCE(dc.cost, 0) AS cost,
      COALESCE(y.revenue, 0) AS yoy_revenue
    FROM cur c
    LEFT JOIN dedup_cost_site dc ON c.site = dc.site
    LEFT JOIN yoy y ON c.site = y.site
    ORDER BY c.business_area, c.company, c.site
    """
    df = _run_query(sql)
    if df.empty:
        return pd.DataFrame(columns=["Business Area", "Company", "Site",
                                      "Clicks", "Revenue (SEK)", "YoY Revenue (SEK)",
                                      "Cost (SEK)", "CoS %", "Revenue YoY %"])

    rows = []
    for _, r in df.iterrows():
        rev = float(r["revenue"] or 0)
        yoy_rev = float(r["yoy_revenue"] or 0)
        cost = float(r["cost"] or 0)
        cos = (cost / rev * 100) if rev else 0
        rev_yoy = ((rev / yoy_rev - 1) * 100) if yoy_rev else 0

        rows.append({
            "Business Area": r["business_area"],
            "Company": r["company"],
            "Site": r["site"],
            "Clicks": int(r["clicks"] or 0),
            "Revenue (SEK)": round(rev),
            "YoY Revenue (SEK)": round(yoy_rev),
            "Cost (SEK)": round(cost),
            "CoS %": round(cos, 1),
            "Revenue YoY %": round(rev_yoy, 1),
        })

    return pd.DataFrame(rows)
