-- ============================================================================
-- silver.v_sales_campaigns
-- ----------------------------------------------------------------------------
-- Purpose : Campaign master with live member + opportunity rollups, for
--           campaign/cadence attribution and effectiveness reporting.
-- Grain   : One row per non-deleted SF Campaign.
-- Notes   : * Member rollups come LIVE from silver.v_sales_campaign_members
--             (the M:N bridge); opportunity rollups from sf_Opportunity. We do
--             NOT trust Salesforce's denormalized NumberOf* fields (they lag) —
--             same approach as v_sales_accounts.
--             DEPLOY v_sales_campaign_members BEFORE this view.
--           * Opportunity link is via Opportunity.CampaignId = the opp's
--             PRIMARY campaign source only (one campaign per opp). True
--             multi-touch influence would need the CampaignInfluence object,
--             which isn't synced. influenced_* reads 0 today because
--             Opportunity.CampaignId isn't being populated yet — forward-ready.
--           * Relates to the other SF views:
--               owner_user_id    -> v_sales_users.user_id
--               (members)        -> silver.v_sales_campaign_members (M:N bridge),
--                                   then lead_id -> v_sales_leads.lead_id
--               (opportunities)  -> v_sales_opportunities.campaign_id (now a
--                                   column there) / sf_Opportunity.CampaignId
--           * Filter is_test_campaign = false for real reporting.
-- Depends : silver.v_sales_users, silver.v_sales_campaign_members,
--           bronze.sf_Campaign, bronze.sf_Opportunity
-- ============================================================================

DROP VIEW IF EXISTS silver.v_sales_campaigns;

CREATE VIEW silver.v_sales_campaigns AS
WITH member_rollup AS (
    SELECT
        campaign_id,
        COUNT(*)                                        AS member_count,
        COUNT(*) FILTER (WHERE member_type = 'Lead')    AS lead_member_count,
        COUNT(*) FILTER (WHERE member_type = 'Contact') AS contact_member_count,
        COUNT(*) FILTER (WHERE has_responded)           AS responded_count,
        MIN(added_at)::date                             AS first_member_added,
        MAX(added_at)::date                             AS last_member_added
    FROM silver.v_sales_campaign_members
    GROUP BY campaign_id
),
opp_rollup AS (
    SELECT
        o."CampaignId"                                                       AS campaign_id,
        COUNT(*)                                                             AS influenced_opp_count,
        COUNT(*) FILTER (WHERE NOT o."IsClosed")                             AS influenced_open_count,
        COUNT(*) FILTER (WHERE o."IsWon")                                    AS influenced_won_count,
        COALESCE(SUM(CASE WHEN NOT o."IsClosed" THEN o."Amount" END), 0)     AS influenced_open_amount,
        COALESCE(SUM(CASE WHEN o."IsWon"        THEN o."Amount" END), 0)     AS influenced_won_amount
    FROM bronze."sf_Opportunity" o
    WHERE o."IsDeleted" = false AND o."CampaignId" IS NOT NULL
    GROUP BY o."CampaignId"
)
SELECT
    c."Id"                          AS campaign_id,
    c."Name"                        AS campaign_name,
    c."Type"                        AS campaign_type,
    c."Status"                      AS status,
    c."IsActive"                    AS is_active,
    -- hierarchy
    c."ParentId"                    AS parent_campaign_id,
    p."Name"                        AS parent_campaign_name,
    -- ownership
    c."OwnerId"                     AS owner_user_id,
    u.user_name                     AS owner_name,
    u.manager_name                  AS owner_manager_name,
    -- dates
    c."StartDate"                   AS start_date,
    c."EndDate"                     AS end_date,
    c."CreatedDate"                 AS created_at,
    c."CreatedDate"::date           AS created_date,
    c."LastActivityDate"            AS last_activity_date,
    -- cost / financials (as entered in SF)
    c."BudgetedCost"                AS budgeted_cost,
    c."ActualCost"                  AS actual_cost,
    c."ExpectedRevenue"             AS expected_revenue,
    -- ============= LIVE MEMBER ROLLUP =============
    COALESCE(mr.member_count, 0)            AS member_count,
    COALESCE(mr.lead_member_count, 0)       AS lead_member_count,
    COALESCE(mr.contact_member_count, 0)    AS contact_member_count,
    COALESCE(mr.responded_count, 0)         AS responded_count,
    ROUND(100.0 * mr.responded_count / NULLIF(mr.member_count, 0), 1) AS response_rate_pct,
    mr.first_member_added,
    mr.last_member_added,
    -- ============= LIVE OPPORTUNITY ROLLUP (primary campaign source) =============
    COALESCE(orr.influenced_opp_count, 0)   AS influenced_opp_count,
    COALESCE(orr.influenced_open_count, 0)  AS influenced_open_count,
    COALESCE(orr.influenced_won_count, 0)   AS influenced_won_count,
    COALESCE(orr.influenced_open_amount, 0) AS influenced_open_pipeline,
    COALESCE(orr.influenced_won_amount, 0)  AS influenced_won_amount,
    -- ============= DERIVED FLAGS =============
    CASE WHEN c."Name" ILIKE '%test%' THEN true ELSE false END AS is_test_campaign,
    CASE
        WHEN c."Name" ILIKE '%test%'                                THEN 'TEST'
        WHEN COALESCE(mr.member_count, 0) = 0                       THEN 'NO_MEMBERS'
        WHEN u.user_name IS NULL                                    THEN 'NO_OWNER'
        ELSE 'OK'
    END                             AS data_quality_flag
FROM bronze."sf_Campaign"          c
LEFT JOIN bronze."sf_Campaign"     p   ON p."Id"        = c."ParentId"
LEFT JOIN silver.v_sales_users     u   ON u.user_id     = c."OwnerId"
LEFT JOIN member_rollup            mr  ON mr.campaign_id = c."Id"
LEFT JOIN opp_rollup               orr ON orr.campaign_id = c."Id"
WHERE c."IsDeleted" = false;

COMMENT ON VIEW silver.v_sales_campaigns IS
'Campaigns with live member + opportunity rollups. Member/opp counts are computed from sf_CampaignMember and sf_Opportunity (not SF NumberOf* fields). influenced_* uses Opportunity.CampaignId (primary campaign only) and reads 0 until that field is populated. Filter is_test_campaign = false; relate to leads/contacts via sf_CampaignMember, to opps via Opportunity.CampaignId, to owners via v_sales_users.';
