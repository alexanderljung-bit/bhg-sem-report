"""
BHG SEM Report – Main Application
-----------------------------------
Streamlit dashboard matching the AI Studio React reference design.
Top navbar layout, inline filters, clean card-based UI.
Uses st.html() for complex HTML components (tables, grids).
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import date, timedelta
from pathlib import Path

from date_engine import DateEngine
from bq_data import (
    get_kpi_summary,
    get_segmented_performance,
    get_weekly_performance,
    get_daily_cos,
    get_cumulative_cos,
    get_portfolio_grid,
    get_site_deep_dive_data,
    get_companies,
    get_sites_for_company,
    get_hierarchy,
)
import ga4_connector

# =====================================================================
# PAGE CONFIG
# =====================================================================
st.set_page_config(
    page_title="BHG SEM Report",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# =====================================================================
# CSS – Matching AI Studio reference design
# =====================================================================
CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');

:root {
    --bhg-blue: #009FE3;
    --bhg-blue-hover: #008AC5;
    --bhg-blue-light: #E5F5FC;
    --surface: #ffffff;
    --bg: #f8fafc;
    --text-primary: #0f172a;
    --text-secondary: #475569;
    --text-muted: #64748b;
    --text-light: #94a3b8;
    --border: #e2e8f0;
    --border-light: #f1f5f9;
    --emerald-50: #ecfdf5;
    --emerald-100: #d1fae5;
    --emerald-500: #10b981;
    --emerald-600: #059669;
    --emerald-700: #047857;
    --rose-50: #fff1f2;
    --rose-500: #f43f5e;
    --rose-600: #e11d48;
    --shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
    --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    --radius-lg: 12px;
    --radius-xl: 16px;
}

html, body, [class*="main"], .stMarkdown, .stText, p, span, div {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
}

#MainMenu, footer, header,
[data-testid="stHeader"],
[data-testid="stToolbar"],
[data-testid="stDecoration"],
[data-testid="collapsedControl"] { display: none !important; visibility: hidden !important; height: 0 !important; min-height: 0 !important; border: none !important; box-shadow: none !important; }

[data-testid="stSidebar"] { display: none !important; }
[data-testid="stAppViewContainer"] { border-top: none !important; box-shadow: none !important; }
[data-testid="stApp"] { border-top: none !important; box-shadow: none !important; }

.block-container { padding: 0 2rem 2rem 2rem !important; max-width: 1600px !important; margin: 0 auto !important; border-top: none !important; }

/* Unified navbar row */
.navbar-row {
    position: sticky; top: 0; z-index: 999;
    background: rgba(255,255,255,0.92);
    backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px);
    border: none !important;
    margin: 0 -2rem;
    padding: 0 2rem;
}
.navbar-row [data-testid="stHorizontalBlock"] {
    align-items: center !important;
    min-height: 64px;
    gap: 8px !important;
}
/* Remove default spacing for widgets inside navbar */
.navbar-row [data-testid="stVerticalBlockBorderWrapper"],
.navbar-row [data-testid="stColumn"] {
    margin: 0 !important;
    padding: 0 !important;
}
.navbar-row [data-testid="stColumn"] > div {
    padding: 0 !important;
}
.navbar-row .stSelectbox,
.navbar-row [data-testid="stSelectbox"] {
    margin-bottom: 0 !important;
    padding: 0 !important;
}
.navbar-row [data-testid="stSelectbox"] > div { margin-bottom: 0 !important; }
.navbar-row [data-testid="stPopover"] { margin-bottom: 0 !important; }

.content-area { padding: 24px 0; max-width: 1600px; margin: 0 auto; }

[data-testid="stSelectbox"] { min-width: 140px; }
[data-testid="stSelectbox"] > div > div { border-radius: 8px !important; border-color: var(--border) !important; font-size: 0.85rem !important; box-shadow: var(--shadow-sm) !important; }

.stPlotlyChart { border: none !important; box-shadow: none !important; }

/* Popover styling */
[data-testid="stPopover"] button {
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    font-size: 0.82rem !important;
    font-weight: 500 !important;
    padding: 6px 12px !important;
    box-shadow: var(--shadow-sm) !important;
    background: var(--surface) !important;
    color: var(--text-secondary) !important;
    overflow: hidden !important;
    white-space: nowrap !important;
}
[data-testid="stPopover"] button:hover {
    background: var(--bg) !important;
    border-color: var(--bhg-blue) !important;
    color: var(--text-primary) !important;
}
[data-testid="stPopover"] button > div > div:last-child {
    display: none !important;
}
/* Popover panel: compact, left-aligned */
[data-testid="stPopoverBody"] {
    min-width: 180px !important;
    max-width: 220px !important;
    padding: 4px 0 !important;
}
[data-testid="stPopoverBody"] button {
    border: none !important;
    box-shadow: none !important;
    background: transparent !important;
    padding: 6px 16px !important;
    font-size: 0.88rem !important;
    font-weight: 500 !important;
    color: var(--text-secondary) !important;
    border-radius: 0 !important;
    width: 100% !important;
}
[data-testid="stPopoverBody"] button > div {
    justify-content: flex-start !important;
    text-align: left !important;
    width: 100% !important;
}
[data-testid="stPopoverBody"] button > div > span {
    text-align: left !important;
    width: auto !important;
}
[data-testid="stPopoverBody"] button > div > span p {
    text-align: left !important;
}
[data-testid="stPopoverBody"] button:hover {
    background: var(--bg) !important;
    color: var(--text-primary) !important;
    border: none !important;
}

::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 3px; }

@media (max-width: 768px) {
    .navbar-row { margin: 0 -1rem; padding: 0 1rem; }
    .content-area { padding: 16px 0; }
}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# Inject JS to remove the pesky top decoration line that CSS alone can't kill
import streamlit.components.v1 as components
components.html("""
<script>
(function() {
    function killTopLine() {
        var doc = window.parent.document;
        // Remove stDecoration and stHeader from DOM entirely
        var dec = doc.querySelector('[data-testid="stDecoration"]');
        if (dec) dec.remove();
        var hdr = doc.querySelector('[data-testid="stHeader"]');
        if (hdr) hdr.remove();
        // Nuke borders/shadows on all top-level containers
        var selectors = ['.stApp', '[data-testid="stAppViewContainer"]',
                         '[data-testid="stMain"]', '.block-container',
                         '[data-testid="stAppViewBlockContainer"]',
                         '[data-testid="stVerticalBlock"]'];
        selectors.forEach(function(sel) {
            var els = doc.querySelectorAll(sel);
            els.forEach(function(el) {
                el.style.setProperty('border', 'none', 'important');
                el.style.setProperty('box-shadow', 'none', 'important');
                el.style.setProperty('outline', 'none', 'important');
            });
        });
    }
    killTopLine();
    // Re-run after Streamlit finishes rendering
    setTimeout(killTopLine, 500);
    setTimeout(killTopLine, 1500);
})();
</script>
""", height=0)


# =====================================================================
# Shared CSS for st.html() components (injected into each iframe)
# =====================================================================
COMPONENT_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');
:root {
    --bhg-blue: #009FE3; --bhg-blue-light: #E5F5FC;
    --surface: #ffffff; --bg: #f8fafc;
    --text-primary: #0f172a; --text-secondary: #475569;
    --text-muted: #64748b; --text-light: #94a3b8;
    --border: #e2e8f0; --border-light: #f1f5f9;
    --emerald-50: #ecfdf5; --emerald-100: #d1fae5;
    --emerald-500: #10b981; --emerald-600: #059669; --emerald-700: #047857;
    --rose-50: #fff1f2; --rose-500: #f43f5e; --rose-600: #e11d48;
    --shadow-sm: 0 1px 2px 0 rgba(0,0,0,0.05);
    --shadow-md: 0 4px 6px -1px rgba(0,0,0,0.1);
    --radius-lg: 12px; --radius-xl: 16px;
}
* { margin: 0; padding: 0; box-sizing: border-box; font-family: 'Inter', -apple-system, sans-serif; }
body { background: transparent; }

/* KPI Cards */
.kpi-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 24px; }
.kpi-card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius-xl); padding: 24px; box-shadow: var(--shadow-sm); position: relative; overflow: hidden; transition: box-shadow 0.2s ease; }
.kpi-card:hover { box-shadow: var(--shadow-md); }
.kpi-card-watermark { position: absolute; top: 16px; right: 16px; font-size: 3.5rem; opacity: 0.04; transition: opacity 0.2s ease; }
.kpi-card:hover .kpi-card-watermark { opacity: 0.08; }
.kpi-title { font-size: 0.875rem; font-weight: 500; color: var(--text-muted); margin-bottom: 8px; }
.kpi-value-row { display: flex; align-items: baseline; gap: 12px; }
.kpi-value { font-size: 1.875rem; font-weight: 600; color: var(--text-primary); letter-spacing: -0.02em; }
.kpi-badge { display: inline-flex; align-items: center; gap: 4px; font-size: 0.75rem; font-weight: 500; padding: 4px 10px; border-radius: 9999px; }
.kpi-badge-positive { background: var(--emerald-50); color: var(--emerald-600); }
.kpi-badge-negative { background: var(--rose-50); color: var(--rose-600); }

/* Chart Header */
.chart-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 20px; }
.chart-title { font-size: 0.95rem; font-weight: 600; color: var(--text-primary); }
.chart-badge { font-size: 0.75rem; font-weight: 500; background: var(--border-light); color: var(--text-secondary); padding: 4px 10px; border-radius: 6px; }

/* Data Tables */
.data-table-card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius-xl); overflow: hidden; box-shadow: var(--shadow-sm); }
.data-table-header { padding: 20px 24px; border-bottom: 1px solid var(--border); display: flex; align-items: center; justify-content: space-between; }
.data-table-title { font-size: 0.95rem; font-weight: 600; color: var(--text-primary); }
.data-table-subtitle { font-size: 0.75rem; color: var(--text-muted); }
.data-table { width: 100%; border-collapse: collapse; font-size: 0.875rem; }
.data-table thead { background: var(--bg); border-bottom: 1px solid var(--border); }
.data-table th { padding: 14px 24px; text-align: right; font-size: 0.7rem; font-weight: 500; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.05em; }
.data-table th:first-child { text-align: left; }
.data-table td { padding: 14px 24px; text-align: right; color: var(--text-secondary); }
.data-table td:first-child { text-align: left; font-weight: 500; color: var(--text-primary); }
.data-table tbody tr { border-bottom: 1px solid var(--border); transition: background 0.15s ease; }
.data-table tbody tr:hover { background: var(--bg); }
.data-table tbody tr:last-child { border-bottom: none; }
.td-bold { font-weight: 500 !important; color: var(--text-primary) !important; }
.td-mono { font-family: 'JetBrains Mono', monospace !important; font-size: 0.8rem; color: var(--text-secondary) !important; }

/* YoY Heatmap */
.yoy-strong-positive { background: var(--emerald-100); color: var(--emerald-700); font-weight: 500; }
.yoy-positive { background: var(--emerald-50); color: var(--emerald-600); font-weight: 500; }
.yoy-neutral { color: var(--emerald-600); font-weight: 500; }
.yoy-negative { color: var(--rose-600); font-weight: 500; }

/* Info Banner */
.info-banner { display: flex; align-items: flex-start; gap: 12px; background: var(--bhg-blue-light); border: 1px solid rgba(0,159,227,0.2); border-radius: var(--radius-lg); padding: 16px 20px; margin-bottom: 24px; }
.info-banner-icon { color: var(--bhg-blue); font-size: 1.1rem; margin-top: 2px; }
.info-banner-title { font-size: 0.875rem; font-weight: 500; color: var(--text-primary); }
.info-banner-text { font-size: 0.75rem; color: var(--text-secondary); margin-top: 2px; }

/* Portfolio */
.portfolio-row-ba { background: var(--bg) !important; }
.portfolio-row-ba:hover { background: var(--border-light) !important; }
.portfolio-name { display: flex; align-items: center; gap: 4px; }
.area-badge { display: inline-block; margin-left: 8px; padding: 2px 6px; border-radius: 4px; font-size: 0.6rem; font-weight: 500; background: var(--bhg-blue-light); color: var(--bhg-blue); text-transform: uppercase; letter-spacing: 0.05em; }

/* Performance Bar */
.perf-bar-wrap { display: flex; align-items: center; gap: 8px; width: 128px; justify-content: flex-end; }
.perf-label { font-size: 0.75rem; font-weight: 500; min-width: 40px; text-align: right; }
.perf-label-pos { color: var(--emerald-600); }
.perf-label-neg { color: var(--rose-600); }
.perf-track { flex: 1; height: 6px; background: var(--border); border-radius: 9999px; overflow: hidden; position: relative; }
.perf-track-center { position: absolute; left: 50%; top: 0; bottom: 0; width: 1px; background: #94a3b8; z-index: 1; }
.perf-fill-pos { position: absolute; left: 50%; top: 0; bottom: 0; background: var(--emerald-500); border-radius: 0 9999px 9999px 0; }
.perf-fill-neg { position: absolute; top: 0; bottom: 0; background: var(--rose-500); border-radius: 9999px 0 0 9999px; }

@media (max-width: 768px) { .kpi-grid { grid-template-columns: 1fr; } }
</style>
"""


# =====================================================================
# HELPER FUNCTIONS
# =====================================================================
def fmt_num(n: float, prefix: str = "", suffix: str = "") -> str:
    if abs(n) >= 1_000_000:
        return f"{prefix}{n / 1_000_000:.1f}M{suffix}"
    elif abs(n) >= 1_000:
        return f"{prefix}{n / 1_000:.1f}K{suffix}"
    else:
        return f"{prefix}{n:,.0f}{suffix}"


def yoy_pct(current: float, previous: float) -> float:
    if previous == 0:
        return 0
    return (current / previous - 1) * 100


def yoy_badge_html(pct: float) -> str:
    is_pos = pct >= 0
    cls = "kpi-badge-positive" if is_pos else "kpi-badge-negative"
    sign = "+" if is_pos else ""
    arrow = "↗" if is_pos else "↘"
    return f'<span class="kpi-badge {cls}">{arrow} {sign}{pct:.1f}% YoY</span>'


def perf_bar_html(value: float, max_abs: float = 30) -> str:
    is_pos = value >= 0
    clamped = min(abs(value), max_abs)
    pct = clamped / max_abs * 50
    label_cls = "perf-label-pos" if is_pos else "perf-label-neg"
    sign = "+" if is_pos else ""
    if is_pos:
        fill = f'<div class="perf-fill-pos" style="width:{pct}%;"></div>'
    else:
        fill = f'<div class="perf-fill-neg" style="right:50%; width:{pct}%;"></div>'
    return f'<div class="perf-bar-wrap"><span class="perf-label {label_cls}">{sign}{value:.1f}%</span><div class="perf-track"><div class="perf-track-center"></div>{fill}</div></div>'


def yoy_heatmap_class(value: float) -> str:
    if value > 10: return "yoy-strong-positive"
    elif value > 5: return "yoy-positive"
    elif value > 0: return "yoy-neutral"
    else: return "yoy-negative"


# =====================================================================
# UNIFIED TOP NAVIGATION BAR
# =====================================================================
def render_top_bar():
    """
    Render the unified navbar containing:
    Brand | Date selector (center) | Menu
    Returns (start_date, end_date) from the date selector.
    """
    date_presets = ["MTD", "QTD", "YTD", "Last 7 Days", "Last 30 Days",
                    "Last Month", "Last 3 Months", "Last 12 Months", "Custom"]

    c_brand, c_sp1, c_date, c_sp2, c_menu = st.columns([3, 1, 4, 2, 1])

    with c_brand:
        st.html(f"""
        {COMPONENT_CSS}
        <div style="display:flex; align-items:center; gap:12px; height:48px;">
            <img src="app/static/bhg-logo.png" style="height:36px;" onerror="this.style.display='none'; this.nextElementSibling.style.display='inline';">
            <span style="font-size:2.2rem; font-weight:700; letter-spacing:-0.06em; color:#009FE3; line-height:1; display:none; font-family:system-ui,sans-serif;">bhg.</span>
            <div style="width:1px; height:28px; background:#cbd5e1; margin-left:4px;"></div>
            <span style="font-size:1.1rem; font-weight:600; color:#0f172a; padding-left:8px; white-space:nowrap;">SEM Report</span>
        </div>
        """)

    with c_date:
        selected_preset = st.selectbox(
            "Date Range", date_presets, key="f_date", label_visibility="collapsed"
        )

    with c_menu:
        with st.popover("☰ Menu"):
            if st.button("🔗  Data Sources", key="nav_ds", use_container_width=True, type="tertiary"):
                st.session_state["active_view"] = "🔗 Data Sources"
                st.rerun()
            if st.button("⚙️  Settings", key="nav_set", use_container_width=True, type="tertiary"):
                st.session_state["active_view"] = "⚙️ Settings"
                st.rerun()

    # Separator line below navbar
    st.html('<div style="border-bottom:1px solid #e2e8f0; margin:0 -2rem;"></div>')

    # Calculate dates from preset or custom
    if selected_preset == "Custom":
        custom_cols = st.columns([2, 2, 1, 2, 2, 3])
        with custom_cols[0]:
            start_date = st.date_input("Från", value=date.today().replace(day=1), key="custom_start")
        with custom_cols[1]:
            end_date = st.date_input("Till", value=date.today() - timedelta(days=1), key="custom_end")
        with custom_cols[2]:
            st.markdown("<div style='text-align:center;padding-top:28px;color:#64748b;'>vs</div>", unsafe_allow_html=True)
        with custom_cols[3]:
            yoy_start_default = start_date - timedelta(days=364)
            cmp_start = st.date_input("Jämför från", value=yoy_start_default, key="cmp_start")
        with custom_cols[4]:
            yoy_end_default = end_date - timedelta(days=364)
            cmp_end = st.date_input("Jämför till", value=yoy_end_default, key="cmp_end")
        # Store custom comparison dates in session_state
        st.session_state["custom_yoy_start"] = cmp_start
        st.session_state["custom_yoy_end"] = cmp_end
    else:
        start_date, end_date = DateEngine.get_preset_dates(selected_preset)
        # Clear custom comparison if not in Custom mode
        if "custom_yoy_start" in st.session_state:
            del st.session_state["custom_yoy_start"]
        if "custom_yoy_end" in st.session_state:
            del st.session_state["custom_yoy_end"]

    # Show date range display
    yoy_start, yoy_end = DateEngine.get_yoy_dates(start_date, end_date)
    if selected_preset == "Custom" and "custom_yoy_start" in st.session_state:
        yoy_start = st.session_state["custom_yoy_start"]
        yoy_end = st.session_state["custom_yoy_end"]

    st.html(f"""
    <div style="display:flex;justify-content:center;gap:8px;padding:4px 0;font-size:0.82rem;color:#64748b;font-family:Inter,sans-serif;">
        <span style="font-weight:500;color:#0f172a;">{start_date.strftime('%b %d')} – {end_date.strftime('%b %d, %Y')}</span>
        <span>vs</span>
        <span>{yoy_start.strftime('%b %d')} – {yoy_end.strftime('%b %d, %Y')}</span>
    </div>
    """)

    return start_date, end_date


# =====================================================================
# KPI SCORECARDS
# =====================================================================
def render_kpi_scorecards(kpi: dict):
    watermarks = ["📈", "💰", "🛒", "📉", "💳", "🎯"]
    titles = ["Total Clicks", "Total Revenue", "Transactions", "Cost of Sale", "AVG Sales", "CR %"]

    clicks = kpi["clicks"]
    rev = kpi["revenue"]
    txn = kpi["transactions"]
    cos = kpi["cos"]
    avg_sales = rev / txn if txn else 0
    cr = (txn / clicks * 100) if clicks else 0

    yoy_clicks = kpi["yoy_clicks"]
    yoy_rev = kpi["yoy_revenue"]
    yoy_txn = kpi.get("yoy_transactions", 0)
    yoy_cos = kpi["yoy_cos"]
    yoy_avg_sales = yoy_rev / yoy_txn if yoy_txn else 0
    yoy_cr = (yoy_txn / yoy_clicks * 100) if yoy_clicks else 0

    values = [
        fmt_num(clicks),
        fmt_num(rev, suffix=" SEK"),
        fmt_num(txn),
        f"{cos:.1f}%",
        fmt_num(avg_sales, suffix=" SEK"),
        f"{cr:.2f}%",
    ]
    deltas = [
        yoy_pct(clicks, yoy_clicks),
        yoy_pct(rev, yoy_rev),
        yoy_pct(txn, yoy_txn) if yoy_txn else 0,
        -(cos - yoy_cos),
        yoy_pct(avg_sales, yoy_avg_sales) if yoy_avg_sales else 0,
        -(cr - yoy_cr),  # positive if CR improved
    ]

    cards = ""
    for i in range(6):
        badge = yoy_badge_html(deltas[i])
        cards += f"""
        <div class="kpi-card">
            <div class="kpi-card-watermark">{watermarks[i]}</div>
            <div class="kpi-title">{titles[i]}</div>
            <div class="kpi-value-row">
                <span class="kpi-value">{values[i]}</span>
                {badge}
            </div>
        </div>"""

    st.html(f'{COMPONENT_CSS}<div class="kpi-grid">{cards}</div>')


# =====================================================================
# CHARTS
# =====================================================================
def render_charts(start_date: date, end_date: date, company: str, site: str):
    daily_df = get_daily_cos(start_date, end_date, company, site)
    render_charts_from_data(daily_df, start_date, end_date, company, site)


def render_charts_from_data(daily_df, start_date: date, end_date: date, company: str, site: str):
    col1, col2 = st.columns(2)

    with col1:
        st.html(f'{COMPONENT_CSS}<div class="chart-header"><span class="chart-title">Cumulative CoS</span><span class="chart-badge">MTD</span></div>')
        # Derive cumulative from the daily data directly
        if not daily_df.empty:
            cum_df = daily_df.sort_values("Date").copy()
            cum_df["Cumulative Revenue"] = cum_df["Revenue"].cumsum()
            cum_df["Cumulative Cost"] = cum_df["Cost"].cumsum()
            cum_df["Cumulative CoS %"] = cum_df.apply(
                lambda r: round(r["Cumulative Cost"] / r["Cumulative Revenue"] * 100, 2)
                if r["Cumulative Revenue"] else 0, axis=1
            )
            cum_df["Budget Target %"] = 9.0
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=cum_df["Date"], y=cum_df["Cumulative CoS %"],
                name="Actual CoS",
                line=dict(color="#009FE3", width=3),
                mode="lines+markers",
                marker=dict(size=6, color="#009FE3", line=dict(width=2, color="white")),
                hovertemplate="%{x|%b %d}: %{y:.2f}%<extra></extra>",
            ))
            fig.add_trace(go.Scatter(
                x=cum_df["Date"], y=cum_df["Budget Target %"],
                name="Target 9.0%",
                line=dict(color="#ef4444", width=1.5, dash="dash"),
                hovertemplate="Target: %{y:.1f}%<extra></extra>",
            ))
            fig.update_layout(
                template="plotly_white", height=300,
                margin=dict(l=0, r=20, t=10, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=11)),
                yaxis=dict(ticksuffix="%", gridcolor="#f1f5f9", zeroline=False, tickfont=dict(color="#64748b", size=12)),
                xaxis=dict(gridcolor="#f1f5f9", showgrid=False, tickfont=dict(color="#64748b", size=12)),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                hovermode="x unified",
            )
            st.plotly_chart(fig, use_container_width=True, key="cum_cos")
        else:
            st.info("No data for selected period.")

    with col2:
        st.html(f'{COMPONENT_CSS}<div class="chart-header"><span class="chart-title">Day-by-Day CoS</span><span class="chart-badge">MTD</span></div>')
        if not daily_df.empty:
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=daily_df["Date"], y=daily_df["CoS %"],
                marker=dict(color="#009FE3", cornerradius=4),
                hovertemplate="%{x|%b %d}: %{y:.2f}%<extra></extra>",
            ))
            fig2.add_hline(
                y=9.0, line_dash="dash", line_color="#ef4444", line_width=1.5,
                annotation_text="Target 9.0%", annotation_position="top right",
                annotation_font=dict(size=11, color="#64748b"),
            )
            fig2.update_layout(
                template="plotly_white", height=300,
                margin=dict(l=0, r=20, t=10, b=10),
                yaxis=dict(ticksuffix="%", gridcolor="#f1f5f9", zeroline=False, tickfont=dict(color="#64748b", size=12)),
                xaxis=dict(gridcolor="#f1f5f9", showgrid=False, tickfont=dict(color="#64748b", size=12)),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                showlegend=False, bargap=0.3,
            )
            st.plotly_chart(fig2, use_container_width=True, key="daily_cos")
        else:
            st.info("No data for selected period.")


# =====================================================================
# SEGMENTED TABLE
# =====================================================================
def render_segmented_table(start_date: date, end_date: date, company: str, site: str):
    df = get_segmented_performance(start_date, end_date, company, site)
    render_segmented_table_from_data(df)


def render_segmented_table_from_data(df):
    rows = ""
    for _, row in df.iterrows():
        rows += f'<tr><td style="font-weight:500;color:#0f172a;">{row["Segment"]}</td><td>{row["Clicks"]:,.0f}</td><td>{row["Transactions"]:,.0f}</td><td>{row["CR %"]:.2f}%</td><td>{row["Revenue (SEK)"]:,.0f}</td><td>{row["Cost (SEK)"]:,.0f}</td><td class="td-bold">{row["CoS %"]:.2f}%</td></tr>'

    st.html(f"""{COMPONENT_CSS}
    <div class="data-table-card">
        <div class="data-table-header"><span class="data-table-title">Brand vs Non-Brand Performance</span></div>
        <table class="data-table">
            <thead><tr><th style="text-align:left;">Segment</th><th>Clicks</th><th>Transactions</th><th>CR</th><th>Revenue</th><th>Cost</th><th>CoS</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>
    </div>
    """)


# =====================================================================
# WEEKLY PERFORMANCE TABLE
# =====================================================================
def render_weekly_table(start_date: date, end_date: date, company: str, site: str):
    df = get_weekly_performance(start_date, end_date, company, site)
    render_weekly_table_from_data(df)


def render_weekly_table_from_data(df):
    rows = ""
    for _, row in df.iterrows():
        yoy_cls = yoy_heatmap_class(row['Revenue YoY %'])
        sign = '+' if row['Revenue YoY %'] >= 0 else ''
        rows += f'<tr><td class="td-mono">{row["Week"]}</td><td>{row["Clicks"]:,.0f}</td><td>{row["Revenue (SEK)"]:,.0f}</td><td>{row["Cost (SEK)"]:,.0f}</td><td class="td-bold">{row["CoS %"]:.2f}%</td><td class="{yoy_cls}">{sign}{row["Revenue YoY %"]:.1f}%</td></tr>'

    st.html(f"""{COMPONENT_CSS}
    <div class="data-table-card">
        <div class="data-table-header">
            <span class="data-table-title">Weekly Performance</span>
            <span class="data-table-subtitle">Heatmap applied to YoY +/-</span>
        </div>
        <table class="data-table">
            <thead><tr><th style="text-align:left;">Week</th><th>Clicks</th><th>Revenue</th><th>Cost</th><th>CoS</th><th>YoY +/-</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>
    </div>
    """)


# =====================================================================
# PORTFOLIO OVERVIEW
# =====================================================================
def render_portfolio_grid(start_date: date, end_date: date):
    yoy_start, yoy_end = DateEngine.get_yoy_dates(start_date, end_date)

    st.html(f"""{COMPONENT_CSS}
    <div class="info-banner">
        <span class="info-banner-icon">ℹ️</span>
        <div>
            <div class="info-banner-title">BHG Group Overview (MTD)</div>
            <div class="info-banner-text">Comparing {start_date.strftime('%b %d')} – {end_date.strftime('%b %d, %Y')} vs {yoy_start.strftime('%b %d')} – {yoy_end.strftime('%b %d, %Y')} (Weekday Aligned)</div>
        </div>
    </div>
    """)

    df = get_portfolio_grid(start_date, end_date)

    if df.empty:
        st.info("Ingen data hittades för vald period. Kontrollera att datakällor är konfigurerade och att credentials är inlagda.")
        return

    rows = ""

    for ba in df["Business Area"].unique():
        ba_df = df[df["Business Area"] == ba]
        ba_clicks = ba_df["Clicks"].sum()
        ba_revenue = ba_df["Revenue (SEK)"].sum()
        ba_cost = ba_df["Cost (SEK)"].sum()
        ba_cos = ba_cost / ba_revenue * 100 if ba_revenue else 0
        ba_yoy = ba_df["Revenue YoY %"].mean()

        rows += f'<tr class="portfolio-row-ba"><td><div class="portfolio-name"><span style="font-weight:600;color:#0f172a;">{ba}</span><span class="area-badge">Area</span></div></td><td>{fmt_num(ba_clicks)}</td><td style="font-weight:500;color:#0f172a;">{fmt_num(ba_revenue)} SEK</td><td>{fmt_num(ba_cost)} SEK</td><td class="td-bold">{ba_cos:.1f}%</td><td>9.0%</td><td>{perf_bar_html(ba_yoy)}</td></tr>'

        for company in ba_df["Company"].unique():
            co_df = ba_df[ba_df["Company"] == company]
            co_clicks = co_df["Clicks"].sum()
            co_revenue = co_df["Revenue (SEK)"].sum()
            co_cost = co_df["Cost (SEK)"].sum()
            co_cos = co_cost / co_revenue * 100 if co_revenue else 0
            co_yoy = co_df["Revenue YoY %"].mean()

            rows += f'<tr><td><div class="portfolio-name" style="padding-left:24px;"><span style="font-weight:500;color:#0f172a;">{company}</span></div></td><td>{fmt_num(co_clicks)}</td><td style="font-weight:500;color:#0f172a;">{fmt_num(co_revenue)} SEK</td><td>{fmt_num(co_cost)} SEK</td><td class="td-bold">{co_cos:.1f}%</td><td>9.0%</td><td>{perf_bar_html(co_yoy)}</td></tr>'

            for _, site_row in co_df.iterrows():
                rows += f'<tr><td><div class="portfolio-name" style="padding-left:56px;"><span style="color:#475569;">{site_row["Site"]}</span></div></td><td>{site_row["Clicks"]:,.0f}</td><td style="font-weight:500;color:#0f172a;">{site_row["Revenue (SEK)"]:,.0f} SEK</td><td>{site_row["Cost (SEK)"]:,.0f} SEK</td><td class="td-bold">{site_row["CoS %"]:.1f}%</td><td>9.0%</td><td>{perf_bar_html(site_row["Revenue YoY %"])}</td></tr>'

    st.html(f"""{COMPONENT_CSS}
    <div class="data-table-card">
        <table class="data-table">
            <thead><tr>
                <th style="text-align:left;width:25%;">Business Area / Company / Site</th>
                <th>Clicks</th><th>Revenue</th><th>Cost</th><th>CoS</th><th>Target</th><th>YoY Growth</th>
            </tr></thead>
            <tbody>{rows}</tbody>
        </table>
    </div>
    """)


# =====================================================================
# DATA SOURCES VIEW
# =====================================================================
def render_data_sources():
    """Render the Data Sources management view."""
    # ── Connected Sources ──
    sources = ga4_connector.get_connected_sources()

    st.html(f"""{COMPONENT_CSS}
    <div class="info-banner">
        <span class="info-banner-icon">🔗</span>
        <div>
            <div class="info-banner-title">BigQuery Data Sources</div>
            <div class="info-banner-text">Connect GA4 datasets from project <strong>{ga4_connector.get_project_id()}</strong>. Each dataset corresponds to a GA4 property (analytics_[property_id]).</div>
        </div>
    </div>
    """)

    # ── Dialog for editing a source ──
    @st.dialog("Redigera källa")
    def _edit_source(ds_id):
        s = next((x for x in sources if x["dataset_id"] == ds_id), None)
        if not s:
            st.error("Källa hittades inte.")
            return
        co = s.get("company", "—")
        ba = s.get("business_area", "—")
        vat = s.get("vat_status", "ex_vat")

        new_label = st.text_input("Label", value=s.get("label", ""))
        new_gads_id = st.text_input("Google Ads Customer ID", value=s.get("gads_customer_id", ""),
                                    placeholder="1234567890")
        new_company = st.selectbox(
            "Bolag", ga4_connector.COMPANIES,
            index=ga4_connector.COMPANIES.index(co) if co in ga4_connector.COMPANIES else 0,
        )
        new_ba = st.selectbox(
            "Affärsområde", ga4_connector.BUSINESS_AREAS,
            index=ga4_connector.BUSINESS_AREAS.index(ba) if ba in ga4_connector.BUSINESS_AREAS else 0,
        )
        vat_options = ["Ex VAT", "Inc VAT"]
        new_vat = st.selectbox("Moms", vat_options, index=0 if vat == "ex_vat" else 1)

        bc1, bc2, _ = st.columns([1, 1, 2])
        with bc1:
            if st.button("💾 Spara", type="primary"):
                new_vat_status = "ex_vat" if new_vat == "Ex VAT" else "inc_vat"
                ga4_connector.update_source(
                    ds_id, label=new_label, company=new_company,
                    business_area=new_ba, vat_status=new_vat_status,
                    gads_customer_id=new_gads_id,
                )
                st.rerun()
        with bc2:
            if st.button("🗑️ Ta bort", type="secondary"):
                ga4_connector.remove_source(ds_id)
                st.rerun()

    if sources:
        st.markdown(f"**Connected Sources ({len(sources)})**")

        for s in sources:
            ds_id = s["dataset_id"]
            co = s.get("company", "—")
            currency = s.get("currency", "SEK")
            vat = s.get("vat_status", "ex_vat")
            vat_label = "Ex VAT" if vat == "ex_vat" else "Inc VAT"
            ba = s.get("business_area", "—")

            rc1, rc2, rc3, rc4, rc5 = st.columns([3, 2, 1, 1, 1])
            with rc1:
                st.markdown(f"**{s['label']}** `{ds_id}`")
            with rc2:
                st.caption(f"{co} · {ba}")
            with rc3:
                st.caption(currency)
            with rc4:
                st.caption(vat_label)
            with rc5:
                if st.button("✏️", key=f"edit_{ds_id}"):
                    _edit_source(ds_id)
    else:
        st.info("No data sources connected yet. Add one below.")

    st.markdown("---")

    # ── Add New Source ──
    st.subheader("Add New Source")

    add_mode = st.radio("Method", ["Manual", "Auto-Discover"], horizontal=True, key="add_mode")

    if add_mode == "Manual":
        row1_col1, row1_col2 = st.columns(2)
        with row1_col1:
            property_id = st.text_input("GA4 Property ID (9 digits)", key="manual_prop_id", placeholder="123456789")
        with row1_col2:
            label = st.text_input("Label (e.g. site name)", key="manual_label", placeholder="bygghemma.se")

        row2_col1, row2_col2 = st.columns(2)
        with row2_col1:
            business_area = st.selectbox("Affärsområde", ga4_connector.BUSINESS_AREAS, key="manual_ba")
        with row2_col2:
            company = st.selectbox("Bolag", ga4_connector.COMPANIES, key="manual_company")

        row3_col1, row3_col2, row3_col3 = st.columns(3)
        with row3_col1:
            gads_id = st.text_input("Google Ads Customer ID", key="manual_gads", placeholder="1234567890")
        with row3_col2:
            vat_choice = st.selectbox("Moms", ["Ex VAT", "Inc VAT"], key="manual_vat")
        with row3_col3:
            st.markdown("<br>", unsafe_allow_html=True)

        if st.button("➕ Connect", key="btn_manual_add", type="primary"):
            if not property_id or not property_id.isdigit() or len(property_id) != 9:
                st.error("Property ID must be exactly 9 digits.")
            elif not label:
                st.error("Please enter a label.")
            else:
                dataset_id = f"analytics_{property_id}"
                vat_status = "ex_vat" if vat_choice == "Ex VAT" else "inc_vat"
                result = ga4_connector.add_source(
                    dataset_id, label, business_area, company,
                    vat_status=vat_status, gads_customer_id=gads_id
                )
                if "error" in result:
                    st.error(result["error"])
                else:
                    st.success(f"Connected **{label}** ({dataset_id})")
                    st.rerun()

        # Test connection button
        if property_id and property_id.isdigit() and len(property_id) == 9:
            if st.button("🔍 Test Connection", key="btn_test"):
                with st.spinner("Testing connection..."):
                    result = ga4_connector.test_connection(f"analytics_{property_id}")
                if "error" in result:
                    st.error(f"Connection failed: {result['error']}")
                else:
                    st.success(
                        f"✅ Connection successful! Found **{result['table_count']}** event tables. "
                        f"Date range: **{result['first_date']}** → **{result['last_date']}**"
                    )

    else:  # Auto-Discover
        if not ga4_connector.has_credentials():
            st.warning("⚠️ GCP credentials not configured. Go to **⚙️ Settings** to set up credentials first.")
        else:
            if st.button("🔍 Discover Datasets", key="btn_discover", type="primary"):
                with st.spinner(f"Scanning project {ga4_connector.get_project_id()}..."):
                    datasets = ga4_connector.discover_datasets()

                if not datasets:
                    st.warning("No analytics_* datasets found.")
                elif "error" in datasets[0]:
                    st.error(f"Discovery failed: {datasets[0]['error']}")
                else:
                    st.session_state["discovered_datasets"] = datasets

            # Show discovered datasets
            if "discovered_datasets" in st.session_state:
                discovered = st.session_state["discovered_datasets"]
                connected_ids = {s["dataset_id"] for s in sources}

                for ds in discovered:
                    dcol1, dcol2, dcol3 = st.columns([3, 2, 1])
                    with dcol1:
                        st.code(ds["dataset_id"])
                    with dcol2:
                        already = ds["dataset_id"] in connected_ids
                        if already:
                            st.success("Already connected")
                        else:
                            dl = st.text_input("Label", key=f"lbl_{ds['dataset_id']}", placeholder="site name")
                    with dcol3:
                        if not already:
                            if st.button("Connect", key=f"conn_{ds['dataset_id']}"):
                                lbl = st.session_state.get(f"lbl_{ds['dataset_id']}", ds['dataset_id'])
                                ga4_connector.add_source(ds["dataset_id"], lbl or ds["dataset_id"])
                                st.rerun()


# =====================================================================
# SETTINGS VIEW
# =====================================================================
def render_settings():
    """Render the Settings view for GCP credentials and project config."""
    st.html(f"""{COMPONENT_CSS}
    <div class="info-banner">
        <span class="info-banner-icon">⚙️</span>
        <div>
            <div class="info-banner-title">Settings</div>
            <div class="info-banner-text">Configure GCP credentials and project settings for BigQuery access.</div>
        </div>
    </div>
    """)

    # ── Credentials Status ──
    cred_info = ga4_connector.get_credentials_info()

    st.subheader("GCP Credentials")

    if cred_info and "error" not in cred_info:
        st.html(f"""{COMPONENT_CSS}
        <div class="data-table-card">
            <div class="data-table-header"><span class="data-table-title">Active Credentials</span></div>
            <table class="data-table">
                <thead><tr><th style="text-align:left;">Field</th><th style="text-align:left;">Value</th></tr></thead>
                <tbody>
                    <tr><td>Type</td><td>{cred_info.get('type', 'N/A')}</td></tr>
                    <tr><td>Project ID</td><td><strong>{cred_info.get('project_id', 'N/A')}</strong></td></tr>
                    <tr><td>Service Account</td><td>{cred_info.get('client_email', 'N/A')}</td></tr>
                    <tr><td>File Path</td><td class="td-mono" style="font-size:0.75rem;">{cred_info.get('path', 'N/A')}</td></tr>
                </tbody>
            </table>
        </div>
        """)
    else:
        st.warning("⚠️ No GCP credentials configured. Upload or specify a service account JSON file below.")

    st.markdown("---")

    # ── Upload or specify credentials ──
    st.markdown("**Option 1: Upload Service Account JSON**")
    uploaded = st.file_uploader(
        "Upload credentials JSON",
        type=["json"],
        key="cred_upload",
        label_visibility="collapsed",
    )
    if uploaded:
        import tempfile
        # Save uploaded file
        cred_dir = Path(__file__).parent / ".credentials"
        cred_dir.mkdir(exist_ok=True)
        cred_path = cred_dir / "service_account.json"
        cred_path.write_bytes(uploaded.getvalue())
        result = ga4_connector.set_credentials_path(str(cred_path))
        if "error" in result:
            st.error(result["error"])
        else:
            st.success(f"✅ Credentials saved! Project: **{result['project_id']}**, Account: **{result['client_email']}**")
            st.rerun()

    st.markdown("**Option 2: Specify file path**")
    pcol1, pcol2 = st.columns([4, 1])
    with pcol1:
        cred_path_input = st.text_input(
            "Path to service account JSON",
            key="cred_path",
            placeholder="C:/path/to/service_account.json",
            label_visibility="collapsed",
        )
    with pcol2:
        if st.button("💾 Save", key="btn_save_cred", type="primary"):
            if cred_path_input:
                result = ga4_connector.set_credentials_path(cred_path_input)
                if "error" in result:
                    st.error(result["error"])
                else:
                    st.success(f"✅ Credentials set! Project: **{result['project_id']}**")
                    st.rerun()
            else:
                st.error("Please enter a file path.")

    st.markdown("---")

    # ── Project Configuration ──
    st.subheader("Project Configuration")
    proj_col1, proj_col2 = st.columns([4, 1])
    with proj_col1:
        project_id = st.text_input(
            "GCP Project ID",
            value=ga4_connector.get_project_id(),
            key="project_id_input",
            label_visibility="collapsed",
        )
    with proj_col2:
        if st.button("💾 Save", key="btn_save_proj", type="primary"):
            ga4_connector.set_project_id(project_id)
            st.success(f"Project ID updated to **{project_id}**")


# =====================================================================
# MAIN APPLICATION
# =====================================================================
def main():
    start_date, end_date = render_top_bar()

    # Check if navigating to a menu page
    active = st.session_state.get("active_view", "Site Deep-Dive")

    if active in ("🔗 Data Sources", "⚙️ Settings"):
        # Back button to return to report
        if st.button("← Back to Report", key="btn_back"):
            st.session_state["active_view"] = "Site Deep-Dive"
            st.rerun()

        st.markdown('<div class="content-area">', unsafe_allow_html=True)
        if active == "🔗 Data Sources":
            render_data_sources()
        else:
            render_settings()
        st.markdown('</div>', unsafe_allow_html=True)
        return

    # ── View selector (styled tabs) ──
    tab_deep_dive, tab_group = st.tabs(["📊 Site Deep-Dive", "🏢 BHG Group Overview"])

    with tab_deep_dive:
        # Filter bar – company & site are REQUIRED
        companies = get_companies()
        all_sites = [s.get("label", s["dataset_id"]) for s in ga4_connector.get_connected_sources()]

        # Restore saved selections (survives tab switches)
        saved_co = st.session_state.get("saved_company")
        saved_site = st.session_state.get("saved_site")
        co_idx = companies.index(saved_co) if saved_co in companies else None
        
        filter_cols = st.columns([2, 2, 8])
        with filter_cols[0]:
            selected_company = st.selectbox(
                "Company", companies, key="f_company",
                index=co_idx, placeholder="— Välj bolag —",
                label_visibility="collapsed",
            )
        with filter_cols[1]:
            if selected_company:
                available_sites = get_sites_for_company(selected_company)
            else:
                available_sites = all_sites
            site_idx = available_sites.index(saved_site) if saved_site in available_sites else None
            selected_site = st.selectbox(
                "Site", available_sites, key="f_site",
                index=site_idx, placeholder="— Välj sajt —",
                label_visibility="collapsed",
            )

        # Persist selections
        if selected_company:
            st.session_state["saved_company"] = selected_company
        if selected_site:
            st.session_state["saved_site"] = selected_site

        # Require both selections
        if not selected_company or not selected_site:
            st.html(f"""{COMPONENT_CSS}
            <div style="text-align:center;padding:60px 20px;">
                <div style="font-size:2.5rem;margin-bottom:12px;">📊</div>
                <div style="font-size:1.2rem;font-weight:600;color:#0f172a;margin-bottom:8px;">Välj bolag och sajt</div>
                <div style="color:#64748b;font-size:0.95rem;">Välj ett bolag och en sajt ovan för att visa rapporten.<br>
                För en samlad översikt, använd <strong>BHG Group Overview</strong>.</div>
            </div>
            """)
        else:
            with st.spinner("Laddar rapportdata..."):
                kpi, seg_df, weekly_df, daily_df = get_site_deep_dive_data(
                    start_date, end_date, selected_company, selected_site
                )
            render_kpi_scorecards(kpi)
            render_charts_from_data(daily_df, start_date, end_date, selected_company, selected_site)
            render_segmented_table_from_data(seg_df)
            render_weekly_table_from_data(weekly_df)

    with tab_group:
        with st.spinner("Laddar BHG Group Overview..."):
            render_portfolio_grid(start_date, end_date)


if __name__ == "__main__":
    main()
