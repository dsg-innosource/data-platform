-- ============================================================================
-- silver.v_sales_activity_daily
-- ----------------------------------------------------------------------------
-- Purpose : Daily activity counts per rep + activity type
-- Grain   : One row per (activity_date, owner_user_id, activity_type)
-- Notes   : Source for the monthly Outbound Touches KPI. Filter to
--           is_outbound_touch = true upstream by joining back to the activities
--           view, OR sum activity_type IN ('Email','Call','LinkedIn').
-- Depends : silver.v_sales_activities
-- ============================================================================

DROP VIEW IF EXISTS silver.v_sales_activity_daily;

CREATE VIEW silver.v_sales_activity_daily AS
SELECT
    activity_date,
    owner_user_id,
    owner_name,
    owner_manager_user_id,
    owner_manager_name,
    activity_type,
    COUNT(*)                                              AS activity_count,
    SUM(CASE WHEN is_outbound_touch THEN 1 ELSE 0 END)    AS outbound_touch_count,
    SUM(CASE WHEN is_closed THEN 1 ELSE 0 END)            AS completed_count
FROM silver.v_sales_activities
WHERE activity_date IS NOT NULL
GROUP BY activity_date, owner_user_id, owner_name,
         owner_manager_user_id, owner_manager_name, activity_type;

COMMENT ON VIEW silver.v_sales_activity_daily IS
'Daily activity rollup by rep and type. Used for the monthly Outbound Touches KPI.';
