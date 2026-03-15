"""
BHG SEM Report – BigQuery Data Layer (FastAPI)
----------------------------------------------
Queries GA4 BigQuery exports for SEM performance metrics.
Uses session-scoped CPC attribution (purchases joined with CPC sessions via ga_session_id).
Cost data from Google Ads MCC dataset (metrics_cost_micros).
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

# Google Ads MCC dataset (BHG Group)
GADS_MCC_DATASET = "gad_bygghemma_mcc_1502879059_new_api"
GADS_CAMPAIGN_STATS = "ads_CampaignBasicStats_1502879059"
GADS_CAMPAIGN_TABLE = "ads_Campaign_1502879059"

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
        # Local dev: GOOGLE_APPLICATION_CREDENTIALS env var
        _BQ_CLIENT = bigquery.Client(project=project)
    logger.info(f"BigQuery client initialized for project {project}")
    return _BQ_CLIENT


def _get_client() -> bigquery.Client:
    """Return the cached client."""
    global _BQ_CLIENT
    if _BQ_CLIENT is not None:
        return _BQ_CLIENT
    return init_client()



def _table_ref(dataset_id: str) -> str:
    project = ga4_connector.get_project_id()
    return f"`{project}.{dataset_id}.events_*`"


def _gads_table(table_name: str) -> str:
    project = ga4_connector.get_project_id()
    return f"`{project}.{GADS_MCC_DATASET}.{table_name}`"


def _get_default_dataset() -> str:
    sources = ga4_connector.get_connected_sources()
    return sources[0]["dataset_id"] if sources else None


def _get_gads_customer_id(company: str = None, site: str = None) -> str | None:
    """Get the Google Ads customer_id for a given source."""
    sources = ga4_connector.get_connected_sources()
    if site and site != "All Sites":
        for s in sources:
            if s.get("label") == site:
                return s.get("gads_customer_id")
    if company and company != "All Companies":
        for s in sources:
            if s.get("company") == company:
                return s.get("gads_customer_id")
    if sources:
        return sources[0].get("gads_customer_id")
    return None


# ── CPC session CTE (reused across queries) ─────────────────────────
def _cpc_session_cte(table: str, suffix_start: str, suffix_end: str, alias: str = "cpc") -> str:
    return f"""
    {alias} AS (
      SELECT DISTINCT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
        collected_traffic_source.manual_campaign_name AS campaign
      FROM {table}
      WHERE _TABLE_SUFFIX BETWEEN '{suffix_start}' AND '{suffix_end}'
        AND event_name = 'session_start'
        AND collected_traffic_source.manual_source = 'google'
        AND collected_traffic_source.manual_medium = 'cpc'
    )"""


def _purchase_cte(table: str, suffix_start: str, suffix_end: str, alias: str = "purch") -> str:
    return f"""
    {alias} AS (
      SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
        ecommerce.purchase_revenue AS revenue,
        ecommerce.transaction_id,
        PARSE_DATE('%Y%m%d', event_date) AS event_dt
      FROM {table}
      WHERE _TABLE_SUFFIX BETWEEN '{suffix_start}' AND '{suffix_end}'
        AND event_name = 'purchase'
    )"""


# =====================================================================
# HIERARCHY FROM CONNECTED SOURCES
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


def _resolve_dataset(company: str = None, site: str = None) -> str | None:
    sources = ga4_connector.get_connected_sources()
    if site and site != "All Sites":
        for s in sources:
            if s.get("label") == site:
                return s["dataset_id"]
    if company and company != "All Companies":
        for s in sources:
            if s.get("company") == company:
                return s["dataset_id"]
    return _get_default_dataset()


def _get_vat_divisor(company: str = None, site: str = None) -> float:
    """Return VAT divisor: 1.25 if source is inc_vat, else 1.0 (ex_vat)."""
    sources = ga4_connector.get_connected_sources()
    source = None
    if site and site != "All Sites":
        source = next((s for s in sources if s.get("label") == site), None)
    if not source and company and company != "All Companies":
        source = next((s for s in sources if s.get("company") == company), None)
    if not source and sources:
        source = sources[0]
    if source and source.get("vat_status") == "inc_vat":
        return 1.25
    return 1.0


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
# GOOGLE ADS COST HELPERS
# =====================================================================
def _get_total_cost(customer_id: str, start_date: str, end_date: str) -> float:
    """Get total Google Ads cost for a customer in a date range."""
    if not customer_id:
        return 0
    stats = _gads_table(GADS_CAMPAIGN_STATS)
    sql = f"""
    SELECT COALESCE(SUM(metrics_cost_micros), 0) / 1000000 AS cost
    FROM {stats}
    WHERE customer_id = {customer_id}
      AND segments_date BETWEEN '{start_date}' AND '{end_date}'
    """
    df = _run_query(sql)
    return float(df.iloc[0]["cost"]) if not df.empty else 0


def _get_daily_cost(customer_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Get daily Google Ads cost for a customer."""
    if not customer_id:
        return pd.DataFrame(columns=["Date", "Cost"])
    stats = _gads_table(GADS_CAMPAIGN_STATS)
    sql = f"""
    SELECT segments_date AS Date,
           SUM(metrics_cost_micros) / 1000000 AS Cost
    FROM {stats}
    WHERE customer_id = {customer_id}
      AND segments_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1 ORDER BY 1
    """
    return _run_query(sql)


def _get_brand_cost_split(customer_id: str, start_date: str, end_date: str) -> dict:
    """Get Brand vs Non-Brand cost split using campaign names."""
    if not customer_id:
        return {"Brand": 0, "Non-Brand": 0}
    stats = _gads_table(GADS_CAMPAIGN_STATS)
    campaigns = _gads_table(GADS_CAMPAIGN_TABLE)
    sql = f"""
    WITH campaign_latest AS (
      SELECT campaign_id, customer_id, campaign_name
      FROM {campaigns}
      WHERE customer_id = {customer_id}
      QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id, customer_id ORDER BY _DATA_DATE DESC) = 1
    )
    SELECT
      IF(LOWER(c.campaign_name) LIKE '%brand%', 'Brand', 'Non-Brand') AS segment,
      SUM(s.metrics_cost_micros) / 1000000 AS cost
    FROM {stats} s
    JOIN campaign_latest c ON s.campaign_id = c.campaign_id AND s.customer_id = c.customer_id
    WHERE s.customer_id = {customer_id}
      AND s.segments_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1
    """
    df = _run_query(sql)
    result = {"Brand": 0, "Non-Brand": 0}
    for _, r in df.iterrows():
        result[r["segment"]] = float(r["cost"])
    return result


def _get_weekly_cost(customer_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Get weekly Google Ads cost."""
    if not customer_id:
        return pd.DataFrame(columns=["week", "cost"])
    stats = _gads_table(GADS_CAMPAIGN_STATS)
    sql = f"""
    SELECT FORMAT_DATE('%G-W%V', segments_date) AS week,
           SUM(metrics_cost_micros) / 1000000 AS cost
    FROM {stats}
    WHERE customer_id = {customer_id}
      AND segments_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1
    """
    return _run_query(sql)


# =====================================================================
# KPI SUMMARY
# =====================================================================
def get_kpi_summary(
    start_date: date, end_date: date, company: str = None, site: str = None
) -> dict:
    dataset_id = _resolve_dataset(company, site)
    if not dataset_id:
        return _empty_kpi()

    customer_id = _get_gads_customer_id(company, site)
    yoy_start, yoy_end = DateEngine.get_yoy_dates(start_date, end_date)
    table = _table_ref(dataset_id)
    sfx = (f"{start_date:%Y%m%d}", f"{end_date:%Y%m%d}")
    ysfx = (f"{yoy_start:%Y%m%d}", f"{yoy_end:%Y%m%d}")

    sql = f"""
    WITH
    {_cpc_session_cte(table, sfx[0], sfx[1], 'cur_cpc')},
    {_purchase_cte(table, sfx[0], sfx[1], 'cur_purch')},
    {_cpc_session_cte(table, ysfx[0], ysfx[1], 'yoy_cpc')},
    {_purchase_cte(table, ysfx[0], ysfx[1], 'yoy_purch')},
    cur_metrics AS (
      SELECT
        (SELECT COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) FROM cur_cpc) AS clicks,
        COUNT(DISTINCT cp.transaction_id) AS transactions,
        COALESCE(SUM(cp.revenue), 0) AS revenue
      FROM cur_purch cp
      INNER JOIN cur_cpc cc ON cp.user_pseudo_id = cc.user_pseudo_id AND cp.session_id = cc.session_id
    ),
    yoy_metrics AS (
      SELECT
        (SELECT COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) FROM yoy_cpc) AS clicks,
        COUNT(DISTINCT yp.transaction_id) AS transactions,
        COALESCE(SUM(yp.revenue), 0) AS revenue
      FROM yoy_purch yp
      INNER JOIN yoy_cpc yc ON yp.user_pseudo_id = yc.user_pseudo_id AND yp.session_id = yc.session_id
    )
    SELECT
      c.clicks, c.revenue, c.transactions,
      y.clicks AS yoy_clicks, y.revenue AS yoy_revenue, y.transactions AS yoy_transactions
    FROM cur_metrics c, yoy_metrics y
    """
    df = _run_query(sql)
    if df.empty:
        return _empty_kpi()

    r = df.iloc[0]
    vat_div = _get_vat_divisor(company, site)
    rev = round(float(r["revenue"] or 0) / vat_div, 2)
    yoy_rev = round(float(r["yoy_revenue"] or 0) / vat_div, 2)

    # Get cost from Google Ads
    cost = _get_total_cost(customer_id, str(start_date), str(end_date))
    yoy_cost = _get_total_cost(customer_id, str(yoy_start), str(yoy_end))
    cos = round((cost / rev * 100), 2) if rev else 0
    yoy_cos = round((yoy_cost / yoy_rev * 100), 2) if yoy_rev else 0

    return {
        "clicks": int(r["clicks"] or 0),
        "revenue": rev,
        "cost": round(cost, 2),
        "cos": cos,
        "transactions": int(r["transactions"] or 0),
        "yoy_clicks": int(r["yoy_clicks"] or 0),
        "yoy_revenue": yoy_rev,
        "yoy_cost": round(yoy_cost, 2),
        "yoy_cos": yoy_cos,
        "yoy_transactions": int(r["yoy_transactions"] or 0),
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
    dataset_id = _resolve_dataset(company, site)
    if not dataset_id:
        return pd.DataFrame()

    customer_id = _get_gads_customer_id(company, site)
    yoy_start, yoy_end = DateEngine.get_yoy_dates(start_date, end_date)
    table = _table_ref(dataset_id)
    sfx = (f"{start_date:%Y%m%d}", f"{end_date:%Y%m%d}")
    ysfx = (f"{yoy_start:%Y%m%d}", f"{yoy_end:%Y%m%d}")

    sql = f"""
    WITH
    {_cpc_session_cte(table, sfx[0], sfx[1], 'cur_cpc')},
    {_purchase_cte(table, sfx[0], sfx[1], 'cur_purch')},
    {_cpc_session_cte(table, ysfx[0], ysfx[1], 'yoy_cpc')},
    {_purchase_cte(table, ysfx[0], ysfx[1], 'yoy_purch')},
    cur_joined AS (
      SELECT
        IF(LOWER(cc.campaign) LIKE '%brand%', 'Brand', 'Non-Brand') AS segment,
        cc.user_pseudo_id, cc.session_id,
        cp.transaction_id, cp.revenue
      FROM cur_cpc cc
      LEFT JOIN cur_purch cp ON cc.user_pseudo_id = cp.user_pseudo_id AND cc.session_id = cp.session_id
    ),
    yoy_joined AS (
      SELECT
        IF(LOWER(yc.campaign) LIKE '%brand%', 'Brand', 'Non-Brand') AS segment,
        yp.transaction_id, yp.revenue
      FROM yoy_cpc yc
      LEFT JOIN yoy_purch yp ON yc.user_pseudo_id = yp.user_pseudo_id AND yc.session_id = yp.session_id
    ),
    cur_agg AS (
      SELECT segment,
        COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) AS clicks,
        COUNT(DISTINCT transaction_id) AS transactions,
        COALESCE(SUM(revenue), 0) AS revenue
      FROM cur_joined GROUP BY 1
    ),
    yoy_agg AS (
      SELECT segment,
        COALESCE(SUM(revenue), 0) AS revenue
      FROM yoy_joined GROUP BY 1
    )
    SELECT c.segment, c.clicks, c.transactions, c.revenue,
           COALESCE(y.revenue, 0) AS yoy_revenue
    FROM cur_agg c
    LEFT JOIN yoy_agg y ON c.segment = y.segment
    ORDER BY c.segment
    """
    df = _run_query(sql)
    if df.empty:
        return pd.DataFrame(columns=["Segment", "Clicks", "Transactions", "CR %",
                                      "Revenue (SEK)", "Cost (SEK)", "CoS %", "Revenue YoY %"])

    # Get brand/non-brand cost split
    cost_split = _get_brand_cost_split(customer_id, str(start_date), str(end_date))
    vat_div = _get_vat_divisor(company, site)

    rows = []
    for _, r in df.iterrows():
        clicks = int(r["clicks"])
        txn = int(r["transactions"])
        rev = float(r["revenue"]) / vat_div
        cr = (txn / clicks * 100) if clicks else 0
        yoy_rev = float(r["yoy_revenue"]) / vat_div
        rev_yoy = ((rev / yoy_rev - 1) * 100) if yoy_rev else 0
        seg_cost = cost_split.get(r["segment"], 0)
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
    dataset_id = _resolve_dataset(company, site)
    if not dataset_id:
        return pd.DataFrame()

    customer_id = _get_gads_customer_id(company, site)
    week_start = end_date - timedelta(weeks=12)
    yoy_offset = timedelta(days=364)
    yoy_week_start = week_start - yoy_offset
    table = _table_ref(dataset_id)
    sfx = (f"{week_start:%Y%m%d}", f"{end_date:%Y%m%d}")
    ysfx = (f"{yoy_week_start:%Y%m%d}", f"{(end_date - yoy_offset):%Y%m%d}")

    sql = f"""
    WITH
    {_cpc_session_cte(table, sfx[0], sfx[1], 'cur_cpc')},
    {_purchase_cte(table, sfx[0], sfx[1], 'cur_purch')},
    {_cpc_session_cte(table, ysfx[0], ysfx[1], 'yoy_cpc')},
    {_purchase_cte(table, ysfx[0], ysfx[1], 'yoy_purch')},
    cur_weekly AS (
      SELECT
        FORMAT_DATE('%G-W%V', cp.event_dt) AS wk,
        COUNT(DISTINCT CONCAT(cc.user_pseudo_id, CAST(cc.session_id AS STRING))) AS clicks,
        COALESCE(SUM(cp.revenue), 0) AS revenue
      FROM cur_cpc cc
      LEFT JOIN cur_purch cp ON cc.user_pseudo_id = cp.user_pseudo_id AND cc.session_id = cp.session_id
      GROUP BY 1
    ),
    yoy_weekly AS (
      SELECT
        FORMAT_DATE('%G-W%V', DATE_ADD(yp.event_dt, INTERVAL 364 DAY)) AS wk,
        COALESCE(SUM(yp.revenue), 0) AS revenue
      FROM yoy_cpc yc
      LEFT JOIN yoy_purch yp ON yc.user_pseudo_id = yp.user_pseudo_id AND yc.session_id = yp.session_id
      GROUP BY 1
    )
    SELECT
      c.wk AS week,
      c.clicks,
      c.revenue,
      SAFE_DIVIDE(c.revenue - COALESCE(y.revenue, 0), NULLIF(COALESCE(y.revenue, 0), 0)) * 100 AS rev_yoy_pct
    FROM cur_weekly c
    LEFT JOIN yoy_weekly y ON c.wk = y.wk
    WHERE c.wk IS NOT NULL
    ORDER BY c.wk DESC
    """
    df = _run_query(sql)
    if df.empty:
        return pd.DataFrame(columns=["Week", "Clicks", "Revenue (SEK)",
                                      "Cost (SEK)", "CoS %", "Revenue YoY %"])

    # Get weekly cost from Google Ads
    weekly_cost = _get_weekly_cost(customer_id, str(week_start), str(end_date))
    cost_by_week = {}
    if not weekly_cost.empty:
        cost_by_week = dict(zip(weekly_cost["week"], weekly_cost["cost"]))

    df = df.rename(columns={"week": "Week", "clicks": "Clicks", "revenue": "Revenue (SEK)",
                             "rev_yoy_pct": "Revenue YoY %"})
    vat_div = _get_vat_divisor(company, site)
    df["Revenue (SEK)"] = df["Revenue (SEK)"] / vat_div
    df["Cost (SEK)"] = df["Week"].map(lambda w: round(cost_by_week.get(w, 0)))
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
    dataset_id = _resolve_dataset(company, site)
    if not dataset_id:
        return pd.DataFrame(columns=["Date", "Revenue", "Cost", "CoS %"])

    customer_id = _get_gads_customer_id(company, site)
    table = _table_ref(dataset_id)
    sfx = (f"{start_date:%Y%m%d}", f"{end_date:%Y%m%d}")

    sql = f"""
    WITH
    {_cpc_session_cte(table, sfx[0], sfx[1], 'cpc')},
    {_purchase_cte(table, sfx[0], sfx[1], 'purch')}
    SELECT
      p.event_dt AS Date,
      COALESCE(SUM(p.revenue), 0) AS Revenue
    FROM purch p
    INNER JOIN cpc c ON p.user_pseudo_id = c.user_pseudo_id AND p.session_id = c.session_id
    GROUP BY 1
    ORDER BY 1
    """
    rev_df = _run_query(sql)
    if rev_df.empty:
        return pd.DataFrame(columns=["Date", "Revenue", "Cost", "CoS %"])

    # Apply VAT adjustment
    vat_div = _get_vat_divisor(company, site)
    rev_df["Revenue"] = rev_df["Revenue"] / vat_div

    # Get daily cost
    cost_df = _get_daily_cost(customer_id, str(start_date), str(end_date))
    if not cost_df.empty:
        cost_df["Date"] = pd.to_datetime(cost_df["Date"]).dt.date
        rev_df["Date"] = pd.to_datetime(rev_df["Date"]).dt.date
        merged = rev_df.merge(cost_df, on="Date", how="left").fillna(0)
    else:
        merged = rev_df.copy()
        merged["Cost"] = 0

    merged["CoS %"] = merged.apply(
        lambda r: round(r["Cost"] / r["Revenue"] * 100, 2) if r["Revenue"] else 0, axis=1
    )
    return merged


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
    """Fetch KPI, segmented, weekly, daily and cumulative CoS data.
    Returns (kpi_dict, segmented_df, weekly_df, daily_cos_df, cumulative_cos_df).
    """
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
    sources = ga4_connector.get_connected_sources()
    if not sources:
        return pd.DataFrame()

    yoy_start, yoy_end = DateEngine.get_yoy_dates(start_date, end_date)

    def _query_source(s):
        """Query a single source sequentially."""
        dataset_id = s["dataset_id"]
        customer_id = s.get("gads_customer_id")
        table = _table_ref(dataset_id)
        sfx = (f"{start_date:%Y%m%d}", f"{end_date:%Y%m%d}")
        ysfx = (f"{yoy_start:%Y%m%d}", f"{yoy_end:%Y%m%d}")

        sql = f"""
        WITH
        {_cpc_session_cte(table, sfx[0], sfx[1], 'cur_cpc')},
        {_purchase_cte(table, sfx[0], sfx[1], 'cur_purch')},
        {_cpc_session_cte(table, ysfx[0], ysfx[1], 'yoy_cpc')},
        {_purchase_cte(table, ysfx[0], ysfx[1], 'yoy_purch')}
        SELECT
          (SELECT COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) FROM cur_cpc) AS clicks,
          (SELECT COALESCE(SUM(cp.revenue), 0) FROM cur_purch cp INNER JOIN cur_cpc cc ON cp.user_pseudo_id = cc.user_pseudo_id AND cp.session_id = cc.session_id) AS revenue,
          (SELECT COALESCE(SUM(yp.revenue), 0) FROM yoy_purch yp INNER JOIN yoy_cpc yc ON yp.user_pseudo_id = yc.user_pseudo_id AND yp.session_id = yc.session_id) AS yoy_revenue
        """
        df = _run_query(sql)
        if df.empty:
            return None

        r = df.iloc[0]
        vat_div = 1.25 if s.get("vat_status") == "inc_vat" else 1.0
        rev = float(r["revenue"] or 0) / vat_div
        yoy_rev = float(r["yoy_revenue"] or 0) / vat_div
        rev_yoy = ((rev / yoy_rev - 1) * 100) if yoy_rev else 0

        cost = _get_total_cost(customer_id, str(start_date), str(end_date))
        cos = (cost / rev * 100) if rev else 0

        return {
            "Business Area": s.get("business_area", "Uncategorized"),
            "Company": s.get("company", "Unknown"),
            "Site": s.get("label", dataset_id),
            "Clicks": int(r["clicks"] or 0),
            "Revenue (SEK)": round(rev),
            "YoY Revenue (SEK)": round(yoy_rev),
            "Cost (SEK)": round(cost),
            "CoS %": round(cos, 1),
            "Revenue YoY %": round(rev_yoy, 1),
        }

    # Run source queries sequentially
    rows = [r for r in (_query_source(s) for s in sources) if r is not None]

    if not rows:
        return pd.DataFrame(columns=["Business Area", "Company", "Site",
                                      "Clicks", "Revenue (SEK)", "YoY Revenue (SEK)",
                                      "Cost (SEK)", "CoS %", "Revenue YoY %"])
    return pd.DataFrame(rows)
