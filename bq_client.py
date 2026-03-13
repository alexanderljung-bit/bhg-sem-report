"""
BHG SEM Report – BigQuery Client
----------------------------------
Wrapper around google-cloud-bigquery for parameterized queries.
Features a USE_DUMMY_DATA flag to toggle between dummy data and live BigQuery.
"""

from datetime import date
from typing import Optional
import pandas as pd

# ── Feature Flag ─────────────────────────────────────────────────────
# Set to False and configure GCP credentials to use real BigQuery data
USE_DUMMY_DATA = True

# Only import BQ if needed
if not USE_DUMMY_DATA:
    from google.cloud import bigquery


# ── SQL Templates ────────────────────────────────────────────────────
class BQQueries:
    """SQL templates with parameterized filters."""

    KPI_SUMMARY = """
    SELECT 
        SUM(clicks) as total_clicks,
        SUM(revenue) as total_revenue,
        SUM(cost) as total_cost,
        SUM(transactions) as total_transactions,
        SAFE_DIVIDE(SUM(cost), SUM(revenue)) as cos
    FROM `your_project.your_dataset.sem_performance`
    WHERE date BETWEEN @start_date AND @end_date
    {company_filter}
    {site_filter}
    """

    SEGMENTED_PERFORMANCE = """
    SELECT 
        segment,
        SUM(clicks) as clicks,
        SUM(transactions) as transactions,
        SAFE_DIVIDE(SUM(transactions), SUM(clicks)) as cr,
        SUM(revenue) as revenue,
        SUM(cost) as cost,
        SAFE_DIVIDE(SUM(cost), SUM(revenue)) as cos
    FROM `your_project.your_dataset.sem_performance`
    WHERE date BETWEEN @start_date AND @end_date
    {company_filter}
    {site_filter}
    GROUP BY segment
    """

    WEEKLY_PERFORMANCE = """
    SELECT 
        FORMAT_DATE('%Y-W%W', date) as week_id,
        SUM(clicks) as clicks,
        SUM(revenue) as revenue,
        SUM(cost) as cost,
        SAFE_DIVIDE(SUM(cost), SUM(revenue)) as cos
    FROM `your_project.your_dataset.sem_performance`
    WHERE date BETWEEN @start_date AND @end_date
    {company_filter}
    {site_filter}
    GROUP BY week_id
    ORDER BY week_id DESC
    """

    PORTFOLIO_GRID = """
    SELECT 
        business_area,
        company,
        site,
        SUM(clicks) as clicks,
        SUM(revenue) as revenue,
        SUM(cost) as cost,
        SAFE_DIVIDE(SUM(cost), SUM(revenue)) as cos
    FROM `your_project.your_dataset.sem_performance`
    WHERE date BETWEEN @start_date AND @end_date
    GROUP BY business_area, company, site
    """


# ── Client ───────────────────────────────────────────────────────────
class BQClient:
    """
    BigQuery client wrapper. When USE_DUMMY_DATA is True, this class
    is not instantiated — the app uses dummy_data.py instead.
    """

    def __init__(self, project_id: str = None):
        self.client = bigquery.Client(project=project_id)

    def _build_filters(
        self, company: Optional[str] = None, site: Optional[str] = None
    ) -> dict:
        """Build SQL filter clauses."""
        filters = {
            "company_filter": "",
            "site_filter": "",
        }
        if company and company != "All Companies":
            filters["company_filter"] = f"AND company = '{company}'"
        if site and site != "All Sites":
            filters["site_filter"] = f"AND site = '{site}'"
        return filters

    def query(
        self,
        sql_template: str,
        start_date: date,
        end_date: date,
        company: Optional[str] = None,
        site: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Execute a parameterized BigQuery query.

        Args:
            sql_template: SQL template with {company_filter} and {site_filter} placeholders
            start_date: Period start
            end_date: Period end
            company: Optional company filter
            site: Optional site filter

        Returns:
            Query results as a pandas DataFrame
        """
        filters = self._build_filters(company, site)
        sql = sql_template.format(**filters)

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            ]
        )

        return self.client.query(sql, job_config=job_config).to_dataframe()

    def get_kpi_summary(
        self, start_date: date, end_date: date, company=None, site=None
    ) -> pd.DataFrame:
        return self.query(BQQueries.KPI_SUMMARY, start_date, end_date, company, site)

    def get_segmented(
        self, start_date: date, end_date: date, company=None, site=None
    ) -> pd.DataFrame:
        return self.query(
            BQQueries.SEGMENTED_PERFORMANCE, start_date, end_date, company, site
        )

    def get_weekly(
        self, start_date: date, end_date: date, company=None, site=None
    ) -> pd.DataFrame:
        return self.query(
            BQQueries.WEEKLY_PERFORMANCE, start_date, end_date, company, site
        )

    def get_portfolio(self, start_date: date, end_date: date) -> pd.DataFrame:
        return self.query(BQQueries.PORTFOLIO_GRID, start_date, end_date)
