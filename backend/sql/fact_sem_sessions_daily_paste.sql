-- =============================================================================
-- fact_sem_sessions_daily — READY TO PASTE INTO BIGQUERY CONSOLE
-- =============================================================================
-- BACKFILL: 13 months (2025-01-01 → 2026-03-14)
-- WARNING: This query scans a lot of data. It may take a few minutes.
-- After this, switch back to the daily version for incremental runs.
-- =============================================================================

CREATE OR REPLACE TABLE `bygghemma-bigdata.bhg_sem_report.fact_sem_sessions_daily`
PARTITION BY date
CLUSTER BY site, campaign_segment
AS
WITH
    -- ── Google Ads: latest campaign names ──
    ads_campaigns AS (
      SELECT campaign_id, customer_id, campaign_name
      FROM `bygghemma-bigdata.gad_bygghemma_mcc_1502879059_new_api.ads_Campaign_1502879059`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id, customer_id ORDER BY _DATA_DATE DESC) = 1
    ),
    ads_cost AS (
      SELECT
        s.segments_date        AS date,
        c.campaign_name        AS ads_campaign_name,
        s.customer_id,
        SUM(s.metrics_cost_micros) / 1000000 AS cost
      FROM `bygghemma-bigdata.gad_bygghemma_mcc_1502879059_new_api.ads_CampaignBasicStats_1502879059` s
      JOIN ads_campaigns c ON s.campaign_id = c.campaign_id AND s.customer_id = c.customer_id
      WHERE s.segments_date BETWEEN '2025-01-01' AND '2026-03-14'
      GROUP BY 1, 2, 3
    ),

    -- ════════════════════════════════════════════════════════════════════
    -- Site 0: Bygghemma.se  (ex_vat → vat_rate = 1.0)
    -- GA4: analytics_269613104  |  Ads: 9855672833
    -- ════════════════════════════════════════════════════════════════════
    cpc_0 AS (
      SELECT PARSE_DATE('%Y%m%d', event_date) AS date, user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        collected_traffic_source.manual_campaign_name AS campaign_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS landing_page
      FROM `bygghemma-bigdata.analytics_269613104.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260314'
        AND event_name = 'session_start'
        AND collected_traffic_source.manual_source = 'google'
        AND collected_traffic_source.manual_medium = 'cpc'
    ),
    sessions_0 AS (
      SELECT date, user_pseudo_id, ga_session_id,
             ANY_VALUE(campaign_name) AS campaign_name, ANY_VALUE(landing_page) AS landing_page
      FROM cpc_0 GROUP BY date, user_pseudo_id, ga_session_id
    ),
    purch_0 AS (
      SELECT user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        ecommerce.transaction_id, ecommerce.purchase_revenue AS revenue_gross
      FROM `bygghemma-bigdata.analytics_269613104.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260314' AND event_name = 'purchase'
    ),
    conv_0 AS (
      SELECT s.date, s.user_pseudo_id, s.ga_session_id,
        COUNT(DISTINCT p.transaction_id) AS transactions,
        COALESCE(SUM(p.revenue_gross), 0) AS revenue_gross
      FROM sessions_0 s LEFT JOIN purch_0 p ON s.user_pseudo_id = p.user_pseudo_id AND s.ga_session_id = p.ga_session_id
      GROUP BY 1, 2, 3
    ),
    final_0 AS (
      SELECT c.date, 'Bygghemma.se' AS site, 'Bygghemma Nordic' AS company, 'Home Improvement' AS business_area,
        c.user_pseudo_id, c.ga_session_id, s.campaign_name, s.landing_page,
        CASE WHEN LOWER(COALESCE(s.campaign_name, '')) LIKE '%brand%' THEN 'Brand' ELSE 'Non-Brand' END AS campaign_segment,
        c.transactions,
        ROUND(c.revenue_gross, 2)              AS revenue_gross,
        'ex_vat'                               AS vat_status,
        1.0                                    AS vat_rate,
        ROUND(c.revenue_gross / 1.0, 2)        AS revenue_net,
        ROUND(COALESCE(ac.cost, 0), 2)         AS cost,
        ROUND(SAFE_DIVIDE(COALESCE(ac.cost, 0), NULLIF(c.revenue_gross / 1.0, 0)) * 100, 2) AS cos_pct
      FROM conv_0 c
      JOIN sessions_0 s ON c.date = s.date AND c.user_pseudo_id = s.user_pseudo_id AND c.ga_session_id = s.ga_session_id
      LEFT JOIN ads_cost ac ON c.date = ac.date AND s.campaign_name = ac.ads_campaign_name AND ac.customer_id = 9855672833
    ),

    -- ════════════════════════════════════════════════════════════════════
    -- Site 1: Badshop.se  (ex_vat → vat_rate = 1.0)
    -- GA4: analytics_269651337  |  Ads: 3824376925
    -- ════════════════════════════════════════════════════════════════════
    cpc_1 AS (
      SELECT PARSE_DATE('%Y%m%d', event_date) AS date, user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        collected_traffic_source.manual_campaign_name AS campaign_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS landing_page
      FROM `bygghemma-bigdata.analytics_269651337.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260314'
        AND event_name = 'session_start'
        AND collected_traffic_source.manual_source = 'google'
        AND collected_traffic_source.manual_medium = 'cpc'
    ),
    sessions_1 AS (
      SELECT date, user_pseudo_id, ga_session_id,
             ANY_VALUE(campaign_name) AS campaign_name, ANY_VALUE(landing_page) AS landing_page
      FROM cpc_1 GROUP BY date, user_pseudo_id, ga_session_id
    ),
    purch_1 AS (
      SELECT user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        ecommerce.transaction_id, ecommerce.purchase_revenue AS revenue_gross
      FROM `bygghemma-bigdata.analytics_269651337.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260314' AND event_name = 'purchase'
    ),
    conv_1 AS (
      SELECT s.date, s.user_pseudo_id, s.ga_session_id,
        COUNT(DISTINCT p.transaction_id) AS transactions,
        COALESCE(SUM(p.revenue_gross), 0) AS revenue_gross
      FROM sessions_1 s LEFT JOIN purch_1 p ON s.user_pseudo_id = p.user_pseudo_id AND s.ga_session_id = p.ga_session_id
      GROUP BY 1, 2, 3
    ),
    final_1 AS (
      SELECT c.date, 'Badshop.se' AS site, 'Bygghemma Nordic' AS company, 'Home Improvement' AS business_area,
        c.user_pseudo_id, c.ga_session_id, s.campaign_name, s.landing_page,
        CASE WHEN LOWER(COALESCE(s.campaign_name, '')) LIKE '%brand%' THEN 'Brand' ELSE 'Non-Brand' END AS campaign_segment,
        c.transactions,
        ROUND(c.revenue_gross, 2)              AS revenue_gross,
        'ex_vat'                               AS vat_status,
        1.0                                    AS vat_rate,
        ROUND(c.revenue_gross / 1.0, 2)        AS revenue_net,
        ROUND(COALESCE(ac.cost, 0), 2)         AS cost,
        ROUND(SAFE_DIVIDE(COALESCE(ac.cost, 0), NULLIF(c.revenue_gross / 1.0, 0)) * 100, 2) AS cos_pct
      FROM conv_1 c
      JOIN sessions_1 s ON c.date = s.date AND c.user_pseudo_id = s.user_pseudo_id AND c.ga_session_id = s.ga_session_id
      LEFT JOIN ads_cost ac ON c.date = ac.date AND s.campaign_name = ac.ads_campaign_name AND ac.customer_id = 3824376925
    ),

    -- ════════════════════════════════════════════════════════════════════
    -- Site 2: Byggshop.se  (ex_vat → vat_rate = 1.0)
    -- GA4: analytics_269631374  |  Ads: 5486757260
    -- ════════════════════════════════════════════════════════════════════
    cpc_2 AS (
      SELECT PARSE_DATE('%Y%m%d', event_date) AS date, user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        collected_traffic_source.manual_campaign_name AS campaign_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS landing_page
      FROM `bygghemma-bigdata.analytics_269631374.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260314'
        AND event_name = 'session_start'
        AND collected_traffic_source.manual_source = 'google'
        AND collected_traffic_source.manual_medium = 'cpc'
    ),
    sessions_2 AS (
      SELECT date, user_pseudo_id, ga_session_id,
             ANY_VALUE(campaign_name) AS campaign_name, ANY_VALUE(landing_page) AS landing_page
      FROM cpc_2 GROUP BY date, user_pseudo_id, ga_session_id
    ),
    purch_2 AS (
      SELECT user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        ecommerce.transaction_id, ecommerce.purchase_revenue AS revenue_gross
      FROM `bygghemma-bigdata.analytics_269631374.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260314' AND event_name = 'purchase'
    ),
    conv_2 AS (
      SELECT s.date, s.user_pseudo_id, s.ga_session_id,
        COUNT(DISTINCT p.transaction_id) AS transactions,
        COALESCE(SUM(p.revenue_gross), 0) AS revenue_gross
      FROM sessions_2 s LEFT JOIN purch_2 p ON s.user_pseudo_id = p.user_pseudo_id AND s.ga_session_id = p.ga_session_id
      GROUP BY 1, 2, 3
    ),
    final_2 AS (
      SELECT c.date, 'Byggshop.se' AS site, 'Bygghemma Nordic' AS company, 'Home Improvement' AS business_area,
        c.user_pseudo_id, c.ga_session_id, s.campaign_name, s.landing_page,
        CASE WHEN LOWER(COALESCE(s.campaign_name, '')) LIKE '%brand%' THEN 'Brand' ELSE 'Non-Brand' END AS campaign_segment,
        c.transactions,
        ROUND(c.revenue_gross, 2)              AS revenue_gross,
        'ex_vat'                               AS vat_status,
        1.0                                    AS vat_rate,
        ROUND(c.revenue_gross / 1.0, 2)        AS revenue_net,
        ROUND(COALESCE(ac.cost, 0), 2)         AS cost,
        ROUND(SAFE_DIVIDE(COALESCE(ac.cost, 0), NULLIF(c.revenue_gross / 1.0, 0)) * 100, 2) AS cos_pct
      FROM conv_2 c
      JOIN sessions_2 s ON c.date = s.date AND c.user_pseudo_id = s.user_pseudo_id AND c.ga_session_id = s.ga_session_id
      LEFT JOIN ads_cost ac ON c.date = ac.date AND s.campaign_name = ac.ads_campaign_name AND ac.customer_id = 5486757260
    ),

    -- ════════════════════════════════════════════════════════════════════
    -- Site 3: Outl1.se  (ex_vat → vat_rate = 1.0)
    -- GA4: analytics_261685181  |  Ads: 1691624653
    -- ════════════════════════════════════════════════════════════════════
    cpc_3 AS (
      SELECT PARSE_DATE('%Y%m%d', event_date) AS date, user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        collected_traffic_source.manual_campaign_name AS campaign_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS landing_page
      FROM `bygghemma-bigdata.analytics_261685181.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260314'
        AND event_name = 'session_start'
        AND collected_traffic_source.manual_source = 'google'
        AND collected_traffic_source.manual_medium = 'cpc'
    ),
    sessions_3 AS (
      SELECT date, user_pseudo_id, ga_session_id,
             ANY_VALUE(campaign_name) AS campaign_name, ANY_VALUE(landing_page) AS landing_page
      FROM cpc_3 GROUP BY date, user_pseudo_id, ga_session_id
    ),
    purch_3 AS (
      SELECT user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        ecommerce.transaction_id, ecommerce.purchase_revenue AS revenue_gross
      FROM `bygghemma-bigdata.analytics_261685181.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260314' AND event_name = 'purchase'
    ),
    conv_3 AS (
      SELECT s.date, s.user_pseudo_id, s.ga_session_id,
        COUNT(DISTINCT p.transaction_id) AS transactions,
        COALESCE(SUM(p.revenue_gross), 0) AS revenue_gross
      FROM sessions_3 s LEFT JOIN purch_3 p ON s.user_pseudo_id = p.user_pseudo_id AND s.ga_session_id = p.ga_session_id
      GROUP BY 1, 2, 3
    ),
    final_3 AS (
      SELECT c.date, 'Outl1.se' AS site, 'Hemfint Group' AS company, 'Home Improvement' AS business_area,
        c.user_pseudo_id, c.ga_session_id, s.campaign_name, s.landing_page,
        CASE WHEN LOWER(COALESCE(s.campaign_name, '')) LIKE '%brand%' THEN 'Brand' ELSE 'Non-Brand' END AS campaign_segment,
        c.transactions,
        ROUND(c.revenue_gross, 2)              AS revenue_gross,
        'ex_vat'                               AS vat_status,
        1.0                                    AS vat_rate,
        ROUND(c.revenue_gross / 1.0, 2)        AS revenue_net,
        ROUND(COALESCE(ac.cost, 0), 2)         AS cost,
        ROUND(SAFE_DIVIDE(COALESCE(ac.cost, 0), NULLIF(c.revenue_gross / 1.0, 0)) * 100, 2) AS cos_pct
      FROM conv_3 c
      JOIN sessions_3 s ON c.date = s.date AND c.user_pseudo_id = s.user_pseudo_id AND c.ga_session_id = s.ga_session_id
      LEFT JOIN ads_cost ac ON c.date = ac.date AND s.campaign_name = ac.ads_campaign_name AND ac.customer_id = 1691624653
    )

-- ═══════════════════════════════════════════════════════════════════════
-- UNION ALL
-- ═══════════════════════════════════════════════════════════════════════
SELECT * FROM final_0
UNION ALL
SELECT * FROM final_1
UNION ALL
SELECT * FROM final_2
UNION ALL
SELECT * FROM final_3;
