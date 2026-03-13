"""
BHG SEM Report - Streamlit Architecture Blueprint
-------------------------------------------------
Detta är ett arkitektoniskt skelett för BHG SEM Report byggt i Streamlit.
Koden är strukturerad för att en annan AI (t.ex. Claude) enkelt ska kunna
fylla i implementationen för databasanrop, databearbetning och exakt UI-rendering.

Designkrav (för .streamlit/config.toml eller custom CSS):
- Ljust tema (Background: #f8fafc)
- Accentfärg: BHG Blue (#009FE3)
- Typsnitt: Inter / sans-serif
"""

import streamlit as st
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

# ==========================================
# 1. DATE ENGINE
# ==========================================
class DateEngine:
    """
    Hanterar datumlogik för rapporten, specifikt för att jämföra perioder.
    Huvudregel: Weekday-aligned YoY (364-dagars skift) för att jämföra samma veckodagar.
    Undantag: Full-month comparisons (t.ex. hela februari i år vs hela februari förra året).
    """
    
    @staticmethod
    def get_yoy_dates(start_date: datetime, end_date: datetime, is_full_month: bool = False) -> Tuple[datetime, datetime]:
        """
        Beräknar YoY-datum baserat på om det är en hel månad eller inte.
        
        Args:
            start_date: Startdatum för aktuell period
            end_date: Slutdatum för aktuell period
            is_full_month: True om perioden exakt täcker en eller flera hela kalendermånader
            
        Returns:
            Tuple med (yoy_start_date, yoy_end_date)
        """
        if is_full_month:
            # TODO för Claude: Implementera exakt 1 års tillbakagång (t.ex. med dateutil.relativedelta(years=1))
            # Exempel: 2026-03-01 till 2026-03-31 -> 2025-03-01 till 2025-03-31
            pass
        else:
            # Weekday-aligned YoY (364 dagar)
            # Detta säkerställer att en måndag jämförs med en måndag året innan.
            yoy_start_date = start_date - timedelta(days=364)
            yoy_end_date = end_date - timedelta(days=364)
            return yoy_start_date, yoy_end_date

    @staticmethod
    def get_mtd_dates(current_date: datetime) -> Tuple[datetime, datetime]:
        """
        Returnerar start- och slutdatum för Month-to-Date (MTD).
        Logik: 1:a i nuvarande månad fram till 'yesterday'.
        """
        # TODO för Claude: Implementera MTD-logik
        pass


# ==========================================
# 2. SQL TEMPLATES (BigQuery)
# ==========================================
class BQQueries:
    """
    Innehåller SQL-templates för BigQuery. 
    Använder placeholders ({company}, {site}) som fylls i dynamiskt.
    """
    
    # Template för KPI Scorecards (Clicks, Revenue, CoS)
    KPI_SUMMARY = """
    SELECT 
        SUM(clicks) as total_clicks,
        SUM(revenue) as total_revenue,
        SAFE_DIVIDE(SUM(cost), SUM(revenue)) as cos
    FROM `your_project.your_dataset.sem_performance`
    WHERE date BETWEEN @start_date AND @end_date
    -- Dynamiska filter appliceras här via Python/Jinja
    {company_filter}
    {site_filter}
    """

    # Template för Brand vs Non-Brand segmentering
    SEGMENTED_PERFORMANCE = """
    SELECT 
        segment, -- 'Brand' eller 'Non-Brand'
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

    # Template för Veckovis data (YYYYWW format)
    WEEKLY_PERFORMANCE = """
    SELECT 
        FORMAT_DATE('%Y-W%W', date) as week_id,
        SUM(clicks) as clicks,
        SUM(revenue) as revenue,
        SAFE_DIVIDE(SUM(cost), SUM(revenue)) as cos
        -- TODO för Claude: Lägg till fönsterfunktioner eller JOINs för att hämta YoY-data per vecka
    FROM `your_project.your_dataset.sem_performance`
    WHERE date BETWEEN @start_date AND @end_date
    {company_filter}
    {site_filter}
    GROUP BY week_id
    ORDER BY week_id DESC
    """

    # Template för Hierarkisk Portfolio Grid (Business Area > Company > Site)
    PORTFOLIO_GRID = """
    SELECT 
        business_area,
        company,
        site,
        SUM(clicks) as clicks,
        SUM(revenue) as revenue,
        SAFE_DIVIDE(SUM(cost), SUM(revenue)) as cos
        -- TODO för Claude: Inkludera YoY-beräkningar för Performance Bar
    FROM `your_project.your_dataset.sem_performance`
    WHERE date BETWEEN @start_date AND @end_date
    GROUP BY business_area, company, site
    """


# ==========================================
# 3. UI COMPONENTS & PLACEHOLDERS
# ==========================================
def render_kpi_scorecards(data: dict, yoy_data: dict):
    """
    Renderar 3 stora KPI-kort: Clicks, Revenue, CoS.
    Inkluderar YoY +/- badge inuti kortet.
    """
    st.markdown("### KPI Overview")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # TODO för Claude: Använd st.metric eller custom HTML/CSS för att matcha BHG-designen
        st.metric(label="Total Clicks", value="124.5K", delta="+12.4% YoY")
        
    with col2:
        st.metric(label="Total Revenue", value="$842.2K", delta="+8.2% YoY")
        
    with col3:
        # Notera: För CoS är ett negativt delta (lägre kostnad) oftast bra (inverterad färglogik)
        st.metric(label="Cost of Sale (CoS)", value="8.4%", delta="-0.6% YoY", delta_color="inverse")

def render_charts():
    """
    Renderar Cumulative CoS Line Chart och Day-by-Day CoS Bar Chart.
    """
    st.markdown("### CoS Trends")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Cumulative CoS (Non-Brand)**")
        # TODO för Claude: Använd plotly.express eller st.line_chart. 
        # Lägg till en horisontell linje för Budget Target (t.ex. 9.0%).
        st.info("[Placeholder: Line Chart - Cumulative CoS with Target Line]")
        
    with col2:
        st.markdown("**Day-by-Day CoS**")
        # TODO för Claude: Använd plotly.express eller st.bar_chart.
        st.info("[Placeholder: Bar Chart - Daily CoS]")

def render_segmented_table():
    """ Renderar Brand vs Non-Brand tabell """
    st.markdown("### Brand vs Non-Brand Performance")
    # TODO för Claude: Använd st.dataframe med pandas styler för snygg formatering
    st.info("[Placeholder: Segmented Table]")

def render_weekly_table():
    """ Renderar veckotabell med heatmap på YoY-kolumnen """
    st.markdown("### Weekly Performance")
    # TODO för Claude: Använd st.dataframe. Applicera pandas background_gradient på YoY-kolumnen.
    st.info("[Placeholder: Weekly Table with Heatmap on YoY +/-]")

def render_portfolio_grid():
    """ 
    Renderar den hierarkiska tabellen (Business Area > Company > Site).
    Inkluderar 'Performance Bar' (sparklines/bullet charts) för YoY tillväxt.
    """
    st.markdown("### Portfolio Overview (MTD)")
    st.caption("Comparing Mar 1 - Mar 11, 2026 vs Mar 2 - Mar 12, 2025 (Weekday Aligned)")
    
    # TODO för Claude: 
    # Alternativ 1: Använd st.dataframe med MultiIndex i Pandas för hierarki.
    # Alternativ 2: Använd AgGrid (streamlit-aggrid) för expanderbara/kollapsbara rader och custom cell renderers för Performance Bar.
    st.info("[Placeholder: Hierarchical Grid with Expandable Rows and YoY Performance Bars]")


# ==========================================
# 4. MAIN APP & HIERARCHY LOGIC
# ==========================================
def main():
    # Page Config
    st.set_page_config(
        page_title="BHG SEM Report", 
        page_icon="ð", 
        layout="wide"
    )
    
    # Custom CSS för att injicera BHG Blue och ljus bakgrund om Streamlit-temat inte räcker
    st.markdown("""
        <style>
        /* TODO för Claude: Lägg till specifik CSS för att dölja Streamlit-menyer, 
           styla metrics-kort med skuggor och rundade hörn (12px+), 
           och sätta accentfärgen till #009FE3 */
        </style>
    """, unsafe_allow_html=True)

    # --- SIDEBAR & HIERARCHY LOGIC ---
    st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/a/a0/BHG_Group_logo.svg/1200px-BHG_Group_logo.svg.png", width=150) # Placeholder för BHG logga
    st.sidebar.title("Filters")
    
    # Mock data för hierarki
    hierarchy_data = {
        "Acme Corp": ["US Main Store", "CA Outlet"],
        "Globex": ["US Tech", "MX Tech"],
        "Initech": ["UK Store", "DE Store"]
    }
    
    # Dropdown 1: Company
    companies = ["All Companies"] + list(hierarchy_data.keys())
    selected_company = st.sidebar.selectbox("Company", companies)
    
    # Hierarki-logik: Filtrera Dropdown 2 (Site) baserat på Dropdown 1 (Company)
    if selected_company == "All Companies":
        # Samla alla sites från alla bolag
        available_sites = ["All Sites"] + [site for sites in hierarchy_data.values() for site in sites]
    else:
        available_sites = ["All Sites"] + hierarchy_data[selected_company]
        
    # Dropdown 2: Site
    selected_site = st.sidebar.selectbox("Site", available_sites)
    
    # Date Picker (Preset + Custom)
    date_presets = ["MTD", "Last 7 Days", "Last 30 Days", "Last Month", "Custom Range"]
    selected_date_preset = st.sidebar.selectbox("Date Range", date_presets)
    
    if selected_date_preset == "Custom Range":
        # TODO för Claude: Lägg till st.date_input för custom range
        pass

    # --- TABS (VIEWS) ---
    tab1, tab2 = st.tabs(["Site Deep-Dive", "Portfolio Overview"])
    
    with tab1:
        # VIEW 1: SITE DEEP-DIVE
        st.header(f"Site Deep-Dive: {selected_company} - {selected_site}")
        
        # Rendera UI-komponenter
        render_kpi_scorecards(data={}, yoy_data={})
        st.divider()
        render_charts()
        st.divider()
        col_table1, col_table2 = st.columns([1, 2])
        with col_table1:
            render_segmented_table()
        with col_table2:
            render_weekly_table()

    with tab2:
        # VIEW 2: PORTFOLIO OVERVIEW
        # Denna vy visar ofta hela portföljen, så vi kan ignorera site-filtret här
        # men respektera datumfiltret.
        render_portfolio_grid()

if __name__ == "__main__":
    main()
