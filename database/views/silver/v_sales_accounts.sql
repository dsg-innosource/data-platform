-- ============================================================================
-- silver.v_sales_accounts
-- ----------------------------------------------------------------------------
-- Purpose : Account master with pipeline + lifetime metrics rolled up
-- Grain   : One row per non-deleted SF Account
-- Notes   : first_won_date drives the new-logo derivation.
--           lifetime / open metrics are pre-aggregated for fast filtering.
--           portal_client_id is a placeholder (Phase 2 work item).
-- Depends : silver.v_sales_users
-- ============================================================================

DROP VIEW IF EXISTS silver.v_sales_accounts;

CREATE VIEW silver.v_sales_accounts AS
WITH opp_rollup AS (
    SELECT
        o."AccountId"                                                AS account_id,
        MIN(CASE WHEN o."IsWon"    THEN o."CloseDate" END)::date     AS first_won_date,
        MAX(CASE WHEN o."IsWon"    THEN o."CloseDate" END)::date     AS most_recent_won_date,
        COUNT(*) FILTER (WHERE o."IsWon")                            AS lifetime_won_count,
        COALESCE(SUM(CASE WHEN o."IsWon"    THEN o."Amount" END), 0) AS lifetime_won_amount,
        COUNT(*) FILTER (WHERE NOT o."IsClosed")                     AS open_opp_count,
        COALESCE(SUM(CASE WHEN NOT o."IsClosed" THEN o."Amount" END), 0) AS open_pipeline_amount,
        COUNT(*) FILTER (WHERE o."IsClosed" AND NOT o."IsWon")       AS lifetime_lost_count
    FROM bronze."sf_Opportunity" o
    WHERE o."IsDeleted" = false
    GROUP BY o."AccountId"
)
SELECT
    a."Id"                          AS account_id,
    a."Name"                        AS account_name,
    a."Type"                        AS account_type,
    a."Industry"                    AS industry,
    a."Website"                     AS website,
    a."Phone"                       AS phone,
    a."NumberOfEmployees"           AS employee_count,
    a."BillingCity"                 AS billing_city,
    a."BillingState"                AS billing_state,
    a."BillingCountry"              AS billing_country,
    a."BillingPostalCode"           AS billing_postal_code,
    a."OwnerId"                     AS owner_user_id,
    u.user_name                     AS owner_name,
    a."ParentId"                    AS parent_account_id,
    a."AccountSource"               AS account_source,
    a."Description"                 AS description,
    a."CreatedDate"                 AS created_at,
    a."LastActivityDate"            AS last_activity_date,
    a."LastModifiedDate"            AS last_modified_at,
    -- pipeline metrics
    COALESCE(r.first_won_date,        NULL)              AS first_won_date,
    COALESCE(r.most_recent_won_date,  NULL)              AS most_recent_won_date,
    COALESCE(r.lifetime_won_count,    0)                 AS lifetime_won_count,
    COALESCE(r.lifetime_won_amount,   0)                 AS lifetime_won_amount,
    COALESCE(r.lifetime_lost_count,   0)                 AS lifetime_lost_count,
    COALESCE(r.open_opp_count,        0)                 AS open_opp_count,
    COALESCE(r.open_pipeline_amount,  0)                 AS open_pipeline_amount,
    -- account stage classification
    CASE
        WHEN COALESCE(r.lifetime_won_count, 0) > 0       THEN 'CUSTOMER'
        WHEN COALESCE(r.open_opp_count,     0) > 0       THEN 'ACTIVE_PROSPECT'
        WHEN COALESCE(r.lifetime_lost_count,0) > 0       THEN 'LOST_PROSPECT'
        ELSE 'COLD'
    END                                                  AS account_stage,
    -- placeholder for Phase 2: bridge to bronze.portal_clients
    NULL::bigint                    AS portal_client_id
FROM bronze."sf_Account" a
LEFT JOIN opp_rollup     r ON r.account_id = a."Id"
LEFT JOIN silver.v_sales_users u ON u.user_id = a."OwnerId"
WHERE a."IsDeleted" = false;

COMMENT ON VIEW silver.v_sales_accounts IS
'Account master with rolled-up opportunity metrics. account_stage classifies accounts as CUSTOMER / ACTIVE_PROSPECT / LOST_PROSPECT / COLD.';
