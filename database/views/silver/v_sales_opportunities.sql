-- ============================================================================
-- silver.v_sales_opportunities
-- ----------------------------------------------------------------------------
-- Purpose : Core pipeline view — every opportunity, fully enriched
-- Grain   : One row per non-deleted SF Opportunity
-- Notes   : data_quality_flag bubbles up hygiene problems for CRO visibility.
--           is_new_logo_win flags account's FIRST closed-won (one row per account
--           gets the flag, the rest are expansion).
--           Period flags (in_*) make it trivial to filter for KPI cards.
-- Depends : silver.v_sales_users, silver.v_sales_accounts
-- ============================================================================

DROP VIEW IF EXISTS silver.v_sales_opportunities;

CREATE VIEW silver.v_sales_opportunities AS
SELECT
    o."Id"                          AS opportunity_id,
    o."Name"                        AS opportunity_name,
    -- account context
    o."AccountId"                   AS account_id,
    a.account_name                  AS account_name,
    a.industry                      AS account_industry,
    a.account_stage                 AS account_stage,
    -- ownership
    o."OwnerId"                     AS owner_user_id,
    u.user_name                     AS owner_name,
    u.manager_user_id               AS owner_manager_user_id,
    u.manager_name                  AS owner_manager_name,
    -- stage / forecast
    o."StageName"                   AS stage_name,
    o."ForecastCategoryName"        AS forecast_category,
    o."Probability"                 AS probability_pct,
    o."IsClosed"                    AS is_closed,
    o."IsWon"                       AS is_won,
    -- amounts
    o."Amount"                      AS amount,
    -- weighted by probability for forecasting
    CASE WHEN o."Amount" IS NOT NULL AND o."Probability" IS NOT NULL
         THEN ROUND(o."Amount" * o."Probability" / 100.0, 2)
         ELSE NULL
    END                             AS weighted_amount,
    -- dates
    o."CloseDate"::date             AS close_date,
    o."CreatedDate"::date           AS created_date,
    o."CreatedDate"                 AS created_at,
    o."LastStageChangeDate"::date   AS last_stage_change_date,
    o."LastStageChangeInDays"       AS days_since_stage_change,
    o."LastActivityDate"::date      AS last_activity_date,
    o."LastActivityInDays"          AS days_since_last_activity,
    o."AgeInDays"                   AS age_days,
    -- attributes
    o."Type"                        AS opportunity_type,
    o."LeadSource"                  AS lead_source,
    o."NextStep"                    AS next_step,
    o."Description"                 AS description,
    o."Loss_Reason__c"              AS loss_reason,
    o."Discovery_Completed__c"      AS is_discovery_completed,
    o."Budget_Confirmed__c"         AS is_budget_confirmed,
    o."ROI_Analysis_Completed__c"   AS is_roi_analysis_completed,
    o."HasOpenActivity"             AS has_open_activity,
    o."HasOverdueTask"              AS has_overdue_task,
    o."PushCount"                   AS close_date_push_count,
    -- ============= DERIVED FLAGS =============
    -- new logo: this opp is the FIRST closed-won for the account
    CASE
        WHEN o."IsWon" = true
         AND o."CloseDate"::date = a.first_won_date
        THEN true ELSE false
    END                             AS is_new_logo_win,
    -- period flags (relative to today)
    CASE WHEN o."IsWon" = true
              AND o."CloseDate" >= DATE_TRUNC('year', CURRENT_DATE)::date
              AND o."CloseDate" <  (DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year')::date
         THEN true ELSE false END   AS is_in_ytd_won,
    CASE WHEN o."IsClosed" = false
              AND o."CloseDate" >= CURRENT_DATE
              AND o."CloseDate" <  CURRENT_DATE + INTERVAL '90 days'
         THEN true ELSE false END   AS is_in_90day_pipeline,
    CASE WHEN o."IsClosed" = false
              AND o."CloseDate" >= CURRENT_DATE
              AND o."CloseDate" <= MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, 12, 31)
         THEN true ELSE false END   AS is_in_year_end_pipeline,
    CASE WHEN o."IsClosed" = false
              AND o."CloseDate" >= CURRENT_DATE
              AND o."CloseDate" <= MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, 12, 31)
              AND o."Probability" >= 50
         THEN true ELSE false END   AS is_in_year_end_pipeline_50pct,
    -- data quality flag — only meaningful for OPEN opps
    CASE
        WHEN o."IsClosed" = true                                                THEN 'OK'
        WHEN o."Amount" IS NULL AND o."CloseDate" < CURRENT_DATE                THEN 'MISSING_AMOUNT_AND_PAST_DUE'
        WHEN o."Amount" IS NULL                                                  THEN 'MISSING_AMOUNT'
        WHEN o."CloseDate" < CURRENT_DATE                                        THEN 'PAST_DUE_CLOSE_DATE'
        WHEN o."LastStageChangeInDays" > 90                                      THEN 'STALE_STAGE'
        ELSE 'OK'
    END                             AS data_quality_flag
FROM bronze."sf_Opportunity"     o
LEFT JOIN silver.v_sales_accounts a ON a.account_id = o."AccountId"
LEFT JOIN silver.v_sales_users    u ON u.user_id    = o."OwnerId"
WHERE o."IsDeleted" = false;

COMMENT ON VIEW silver.v_sales_opportunities IS
'Pipeline opportunities with full enrichment. Use is_in_* flags for period filters; data_quality_flag <> ''OK'' surfaces hygiene issues.';
