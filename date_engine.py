"""
BHG SEM Report – Date Engine
-----------------------------
Handles all date logic: YoY comparisons, MTD, and preset date ranges.
Uses 364-day weekday-aligned shifts for non-full-month periods,
and calendar-year shifts for full-month periods.
"""

from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from typing import Tuple


class DateEngine:
    """
    Manages date logic for the SEM report.
    Primary rule: Weekday-aligned YoY (364-day shift) to compare same weekdays.
    Exception: Full-month comparisons (e.g. full Feb this year vs full Feb last year).
    """

    @staticmethod
    def is_full_month(start_date: date, end_date: date) -> bool:
        """Check if a date range covers one or more exact calendar months."""
        if start_date.day != 1:
            return False
        # End date should be the last day of its month
        next_day = end_date + timedelta(days=1)
        return next_day.day == 1

    @staticmethod
    def get_yoy_dates(
        start_date: date, end_date: date, is_full_month: bool = False
    ) -> Tuple[date, date]:
        """
        Calculate YoY comparison dates.

        Args:
            start_date: Start of current period
            end_date: End of current period
            is_full_month: True if the period covers exact calendar months

        Returns:
            Tuple of (yoy_start_date, yoy_end_date)
        """
        if is_full_month:
            # Exact calendar year shift
            # e.g. 2026-03-01 to 2026-03-31 -> 2025-03-01 to 2025-03-31
            yoy_start = start_date - relativedelta(years=1)
            yoy_end = end_date - relativedelta(years=1)
            return yoy_start, yoy_end
        else:
            # Weekday-aligned YoY (364 days = 52 exact weeks)
            # Ensures Monday compares to Monday the year before
            yoy_start = start_date - timedelta(days=364)
            yoy_end = end_date - timedelta(days=364)
            return yoy_start, yoy_end

    @staticmethod
    def get_mtd_dates(reference_date: date = None) -> Tuple[date, date]:
        """
        Return Month-to-Date dates: 1st of current month to yesterday.

        Args:
            reference_date: The reference date (defaults to today)

        Returns:
            Tuple of (mtd_start, mtd_end) where mtd_end is yesterday
        """
        if reference_date is None:
            reference_date = date.today()
        mtd_start = reference_date.replace(day=1)
        mtd_end = reference_date - timedelta(days=1)
        # Edge case: if it's the 1st, end = start (no "yesterday" in this month)
        if mtd_end < mtd_start:
            mtd_end = mtd_start
        return mtd_start, mtd_end

    @staticmethod
    def get_preset_dates(preset: str, reference_date: date = None) -> Tuple[date, date]:
        """
        Get start/end dates for common presets.

        Args:
            preset: One of 'MTD', 'Last 7 Days', 'Last 30 Days', 'Last Month'
            reference_date: Reference date (defaults to today)

        Returns:
            Tuple of (start_date, end_date)
        """
        if reference_date is None:
            reference_date = date.today()

        yesterday = reference_date - timedelta(days=1)

        if preset == "MTD":
            return DateEngine.get_mtd_dates(reference_date)

        elif preset == "Last 7 Days":
            return yesterday - timedelta(days=6), yesterday

        elif preset == "Last 30 Days":
            return yesterday - timedelta(days=29), yesterday

        elif preset == "Last Month":
            # Full previous calendar month
            first_of_current = reference_date.replace(day=1)
            end_of_last = first_of_current - timedelta(days=1)
            start_of_last = end_of_last.replace(day=1)
            return start_of_last, end_of_last

        elif preset == "QTD":
            # Quarter to Date
            q_month = ((reference_date.month - 1) // 3) * 3 + 1
            q_start = reference_date.replace(month=q_month, day=1)
            return q_start, yesterday

        elif preset == "YTD":
            return reference_date.replace(month=1, day=1), yesterday

        elif preset == "Last 3 Months":
            return yesterday - relativedelta(months=3), yesterday

        elif preset == "Last 12 Months":
            return yesterday - relativedelta(months=12), yesterday

        else:
            # Default to MTD
            return DateEngine.get_mtd_dates(reference_date)
