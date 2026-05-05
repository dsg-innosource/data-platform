-- ============================================================================
-- silver.v_sales_pipeline_summary
-- ----------------------------------------------------------------------------
-- Purpose : Pre-aggregated pipeline by stage + forecast category
-- Grain   : One row per (stage_name, forecast_category)
-- Notes   : Powers the "PIPELINE BREAKDOWN" panel in the CRO weekly view.
--           Only includes OPEN opps (IsClosed = false).
-- Depends : silver.v_sales_opportunities
-- ============================================================================

DROP VIEW IF EXISTS silver.v_sales_pipeline_summary;

CREATE VIEW silver.v_sales_pipeline_summary AS
SELECT
    stage_name,
    forecast_category,
    COUNT(*)                                              AS opportunity_count,
    COUNT(*) FILTER (WHERE data_quality_flag = 'OK')      AS clean_opportunity_count,
    COUNT(*) FILTER (WHERE data_quality_flag <> 'OK')     AS dirty_opportunity_count,
    SUM(amount)                                           AS total_amount,
    SUM(weighted_amount)                                  AS total_weighted_amount,
    AVG(probability_pct)                                  AS avg_probability_pct,
    AVG(age_days)                                         AS avg_age_days,
    AVG(days_since_stage_change)                          AS avg_days_since_stage_change,
    MIN(close_date)                                       AS earliest_close_date,
    MAX(close_date)                                       AS latest_close_date
FROM silver.v_sales_opportunities
WHERE is_closed = false
GROUP BY stage_name, forecast_category;

COMMENT ON VIEW silver.v_sales_pipeline_summary IS
'Open pipeline rolled up by stage and forecast category. Use for stage-funnel charts and totals.';
