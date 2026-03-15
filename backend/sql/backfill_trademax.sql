-- =============================================================================
-- BACKFILL: Trademax.se (analytics_315024045, Ads: 2653921083)
-- =============================================================================
-- Inserts into the existing fact_sem_sessions_daily table.
-- Date range: 2025-01-01 → 2026-03-14
-- =============================================================================

INSERT INTO `bygghemma-bigdata.bhg_sem_report.fact_sem_sessions_daily`
WITH
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
    cpc AS (
      SELECT PARSE_DATE('%Y%m%d', event_date) AS date, user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        collected_traffic_source.manual_campaign_name AS campaign_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS landing_page
      FROM `bygghemma-bigdata.analytics_315024045.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260314'
        AND event_name = 'session_start'
        AND collected_traffic_source.manual_source = 'google'
        AND collected_traffic_source.manual_medium = 'cpc'
    ),
    sessions AS (
      SELECT date, user_pseudo_id, ga_session_id,
             ANY_VALUE(campaign_name) AS campaign_name, ANY_VALUE(landing_page) AS landing_page
      FROM cpc GROUP BY date, user_pseudo_id, ga_session_id
    ),
    purch AS (
      SELECT user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        ecommerce.transaction_id, ecommerce.purchase_revenue AS revenue_gross
      FROM `bygghemma-bigdata.analytics_315024045.events_*`
      WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260314' AND event_name = 'purchase'
    ),
    conv AS (
      SELECT s.date, s.user_pseudo_id, s.ga_session_id,
        COUNT(DISTINCT p.transaction_id) AS transactions,
        COALESCE(SUM(p.revenue_gross), 0) AS revenue_gross
      FROM sessions s LEFT JOIN purch p ON s.user_pseudo_id = p.user_pseudo_id AND s.ga_session_id = p.ga_session_id
      GROUP BY 1, 2, 3
    )
SELECT c.date, 'Trademax.se' AS site, 'Home Furnishing Nordic' AS company, 'Value Home' AS business_area,
  c.user_pseudo_id, c.ga_session_id, s.campaign_name, s.landing_page,
  CASE WHEN LOWER(COALESCE(s.campaign_name, '')) LIKE '%brand%' THEN 'Brand' ELSE 'Non-Brand' END AS campaign_segment,
  c.transactions,
  ROUND(c.revenue_gross, 2)              AS revenue_gross,
  'ex_vat'                               AS vat_status,
  1.0                                    AS vat_rate,
  ROUND(c.revenue_gross / 1.0, 2)        AS revenue_net,
  ROUND(COALESCE(ac.cost, 0), 2)         AS cost,
  ROUND(SAFE_DIVIDE(COALESCE(ac.cost, 0), NULLIF(c.revenue_gross / 1.0, 0)) * 100, 2) AS cos_pct
FROM conv c
JOIN sessions s ON c.date = s.date AND c.user_pseudo_id = s.user_pseudo_id AND c.ga_session_id = s.ga_session_id
LEFT JOIN ads_cost ac ON c.date = ac.date AND s.campaign_name = ac.ads_campaign_name AND ac.customer_id = 2653921083;
