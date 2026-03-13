"""
BHG SEM Report – Dummy Data Generator
---------------------------------------
Generates realistic dummy data for immediate UI testing.
Uses BHG Group subsidiary names and realistic SEM metrics.
"""

import pandas as pd
import numpy as np
from datetime import date, timedelta
from typing import Tuple


# ── BHG Group hierarchy ─────────────────────────────────────────────
HIERARCHY = {
    "DIY": {
        "Bygghemma": ["bygghemma.se", "bygghemma.no"],
        "Trademax": ["trademax.se", "trademax.fi"],
    },
    "Home Furnishing": {
        "Furniturebox": ["furniturebox.se", "furniturebox.no"],
        "Chilli": ["chilli.se"],
    },
    "Nordic Renovation": {
        "Nordiska Fönster": ["nordiskafönster.se"],
        "Skånska Byggvaror": ["skanskabyggvaror.se"],
    },
}

# Flatten for quick lookups
ALL_COMPANIES = {}
ALL_SITES = []
for ba, companies in HIERARCHY.items():
    for company, sites in companies.items():
        ALL_COMPANIES[company] = {"business_area": ba, "sites": sites}
        ALL_SITES.extend(sites)


def _seed():
    """Consistent random seed for reproducible dummy data."""
    np.random.seed(42)


# ── KPI Summary ─────────────────────────────────────────────────────
def get_kpi_summary(
    start_date: date, end_date: date, company: str = None, site: str = None
) -> dict:
    """
    Generate KPI summary data: clicks, revenue, cost, CoS.
    Returns current period + YoY comparison values.
    """
    _seed()
    days = (end_date - start_date).days + 1
    base_clicks = days * np.random.randint(3500, 5500)
    base_revenue = base_clicks * np.random.uniform(5.5, 7.5)
    base_cost = base_revenue * np.random.uniform(0.07, 0.10)

    # Apply filter scaling
    if company and company != "All Companies":
        base_clicks = int(base_clicks * 0.3)
        base_revenue *= 0.3
        base_cost *= 0.3
    if site and site != "All Sites":
        base_clicks = int(base_clicks * 0.5)
        base_revenue *= 0.5
        base_cost *= 0.5

    cos = base_cost / base_revenue if base_revenue else 0

    # YoY values (slightly worse last year)
    yoy_clicks = int(base_clicks * np.random.uniform(0.84, 0.94))
    yoy_revenue = base_revenue * np.random.uniform(0.88, 0.96)
    yoy_cost = yoy_revenue * (cos + np.random.uniform(0.003, 0.012))
    yoy_cos = yoy_cost / yoy_revenue if yoy_revenue else 0

    return {
        "clicks": int(base_clicks),
        "revenue": round(base_revenue, 2),
        "cost": round(base_cost, 2),
        "cos": round(cos * 100, 2),  # percentage
        "transactions": int(base_clicks * np.random.uniform(0.02, 0.04)),
        "yoy_clicks": yoy_clicks,
        "yoy_revenue": round(yoy_revenue, 2),
        "yoy_cost": round(yoy_cost, 2),
        "yoy_cos": round(yoy_cos * 100, 2),
    }


# ── Segmented Performance (Brand / Non-Brand) ───────────────────────
def get_segmented_performance(
    start_date: date, end_date: date, company: str = None, site: str = None
) -> pd.DataFrame:
    """
    Generate Brand vs Non-Brand segmented performance data.
    """
    _seed()
    days = (end_date - start_date).days + 1

    rows = []
    for segment in ["Brand", "Non-Brand"]:
        multiplier = 0.35 if segment == "Brand" else 0.65
        clicks = int(days * np.random.randint(3500, 5500) * multiplier)
        transactions = int(clicks * (0.045 if segment == "Brand" else 0.025))
        cr = transactions / clicks if clicks else 0
        revenue = transactions * np.random.uniform(280, 450)
        cost = revenue * (0.04 if segment == "Brand" else 0.095)
        cos = cost / revenue if revenue else 0

        # YoY
        yoy_clicks = int(clicks * np.random.uniform(0.85, 0.95))
        yoy_transactions = int(yoy_clicks * (0.042 if segment == "Brand" else 0.022))
        yoy_revenue = yoy_transactions * np.random.uniform(260, 420)
        yoy_cost = yoy_revenue * (0.042 if segment == "Brand" else 0.10)

        rows.append({
            "Segment": segment,
            "Clicks": clicks,
            "Transactions": transactions,
            "CR %": round(cr * 100, 2),
            "Revenue (SEK)": round(revenue),
            "Cost (SEK)": round(cost),
            "CoS %": round(cos * 100, 2),
            "Clicks YoY %": round((clicks / yoy_clicks - 1) * 100, 1) if yoy_clicks else 0,
            "Revenue YoY %": round((revenue / yoy_revenue - 1) * 100, 1) if yoy_revenue else 0,
            "CoS YoY pp": round((cos - yoy_cost / yoy_revenue) * 100, 2) if yoy_revenue else 0,
        })

    return pd.DataFrame(rows)


# ── Weekly Performance ───────────────────────────────────────────────
def get_weekly_performance(
    start_date: date, end_date: date, company: str = None, site: str = None
) -> pd.DataFrame:
    """
    Generate 12 weeks of performance data with YoY deltas.
    """
    _seed()
    rows = []
    current = end_date
    for i in range(12):
        week_end = current - timedelta(days=i * 7)
        week_start = week_end - timedelta(days=6)
        if week_start < start_date - timedelta(days=60):
            break

        week_id = week_start.strftime("%Y-W%W")
        clicks = int(np.random.randint(22000, 40000))
        revenue = int(clicks * np.random.uniform(5.5, 7.8))
        cost = int(revenue * np.random.uniform(0.07, 0.11))
        cos = cost / revenue if revenue else 0

        yoy_clicks = int(clicks * np.random.uniform(0.82, 0.98))
        yoy_revenue = int(revenue * np.random.uniform(0.85, 0.97))
        yoy_cost = int(yoy_revenue * np.random.uniform(0.075, 0.115))
        yoy_cos = yoy_cost / yoy_revenue if yoy_revenue else 0

        rows.append({
            "Week": week_id,
            "Clicks": clicks,
            "Revenue (SEK)": revenue,
            "Cost (SEK)": cost,
            "CoS %": round(cos * 100, 2),
            "Clicks YoY %": round((clicks / yoy_clicks - 1) * 100, 1) if yoy_clicks else 0,
            "Revenue YoY %": round((revenue / yoy_revenue - 1) * 100, 1) if yoy_revenue else 0,
            "CoS YoY pp": round((cos - yoy_cos) * 100, 2),
        })

    return pd.DataFrame(rows)


# ── Daily CoS (for bar chart) ───────────────────────────────────────
def get_daily_cos(
    start_date: date, end_date: date, company: str = None, site: str = None
) -> pd.DataFrame:
    """
    Generate daily CoS data for the bar chart.
    """
    _seed()
    days = (end_date - start_date).days + 1
    days = min(days, 31)  # Cap at 31 for readability

    dates = [end_date - timedelta(days=i) for i in range(days)]
    dates.reverse()

    cos_values = np.random.uniform(0.06, 0.13, size=len(dates))
    # Add some trend: slightly lower CoS toward end of month
    trend = np.linspace(0.01, -0.01, len(dates))
    cos_values = cos_values + trend

    return pd.DataFrame({
        "Date": dates,
        "CoS %": np.round(cos_values * 100, 2),
    })


# ── Cumulative CoS (for line chart) ─────────────────────────────────
def get_cumulative_cos(
    start_date: date, end_date: date, company: str = None, site: str = None
) -> pd.DataFrame:
    """
    Generate cumulative CoS data for the line chart, with a budget target.
    """
    _seed()
    days = (end_date - start_date).days + 1
    days = min(days, 31)

    dates = [start_date + timedelta(days=i) for i in range(days)]

    # Simulate cumulative CoS that starts high and stabilizes
    cos_values = []
    running_cost = 0
    running_revenue = 0
    for i, d in enumerate(dates):
        daily_revenue = np.random.uniform(35000, 65000)
        daily_cost = daily_revenue * np.random.uniform(0.07, 0.11)
        running_cost += daily_cost
        running_revenue += daily_revenue
        cos_values.append(round(running_cost / running_revenue * 100, 2))

    # YoY cumulative (slightly higher CoS last year)
    yoy_cos_values = [v + np.random.uniform(0.2, 0.8) for v in cos_values]

    return pd.DataFrame({
        "Date": dates,
        "Cumulative CoS %": cos_values,
        "YoY Cumulative CoS %": [round(v, 2) for v in yoy_cos_values],
        "Budget Target %": [9.0] * len(dates),
    })


# ── Portfolio Grid ───────────────────────────────────────────────────
def get_portfolio_grid(
    start_date: date, end_date: date
) -> pd.DataFrame:
    """
    Generate hierarchical portfolio data: Business Area > Company > Site.
    Includes YoY performance metrics.
    """
    _seed()
    rows = []

    for ba, companies in HIERARCHY.items():
        for company, sites in companies.items():
            for site in sites:
                clicks = int(np.random.randint(5000, 25000))
                revenue = int(clicks * np.random.uniform(5.0, 8.5))
                cost = int(revenue * np.random.uniform(0.06, 0.12))
                cos = cost / revenue if revenue else 0

                yoy_clicks = int(clicks * np.random.uniform(0.80, 1.05))
                yoy_revenue = int(revenue * np.random.uniform(0.82, 1.08))

                clicks_yoy = round((clicks / yoy_clicks - 1) * 100, 1) if yoy_clicks else 0
                revenue_yoy = round((revenue / yoy_revenue - 1) * 100, 1) if yoy_revenue else 0

                rows.append({
                    "Business Area": ba,
                    "Company": company,
                    "Site": site,
                    "Clicks": clicks,
                    "Revenue (SEK)": revenue,
                    "Cost (SEK)": cost,
                    "CoS %": round(cos * 100, 2),
                    "Clicks YoY %": clicks_yoy,
                    "Revenue YoY %": revenue_yoy,
                })

    return pd.DataFrame(rows)


# ── Hierarchy helpers ────────────────────────────────────────────────
def get_companies() -> list:
    """Return list of all company names."""
    return list(ALL_COMPANIES.keys())


def get_sites_for_company(company: str) -> list:
    """Return sites for a given company, or all sites if 'All Companies'."""
    if company == "All Companies":
        return ALL_SITES
    return ALL_COMPANIES.get(company, {}).get("sites", [])
