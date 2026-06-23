-- ============================================================================
-- silver.v_sales_campaign_members
-- ----------------------------------------------------------------------------
-- Purpose : Bridge between campaigns and the people in them. Resolves the
--           campaign↔lead / campaign↔contact many-to-many in the silver layer
--           so reports don't have to reach into bronze.
-- Grain   : One row per non-deleted SF CampaignMember (one person's membership
--           in one campaign).
-- Notes   : * A lead/contact can belong to many campaigns and a campaign has
--             many members — this view is the M:N link. Join lead_id ->
--             v_sales_leads.lead_id (member_type = 'Lead') to relate leads to
--             campaigns; contact members carry contact_id (no contact view yet).
--           * Pulls campaign name/type/status straight from bronze.sf_Campaign
--             (NOT v_sales_campaigns) on purpose — v_sales_campaigns rolls up
--             FROM this view, so depending on it here would be circular.
--           * v_sales_campaigns derives its member_count / response_rate from
--             this view.
-- Depends : silver.v_sales_users, bronze.sf_CampaignMember, bronze.sf_Campaign
-- ============================================================================

DROP VIEW IF EXISTS silver.v_sales_campaign_members;

CREATE VIEW silver.v_sales_campaign_members AS
SELECT
    cm."Id"                         AS campaign_member_id,
    -- campaign context
    cm."CampaignId"                 AS campaign_id,
    c."Name"                        AS campaign_name,
    c."Type"                        AS campaign_type,
    c."Status"                      AS campaign_status,
    -- who the member is
    CASE
        WHEN cm."LeadId"    IS NOT NULL THEN 'Lead'
        WHEN cm."ContactId" IS NOT NULL THEN 'Contact'
        ELSE 'Unknown'
    END                             AS member_type,
    cm."LeadId"                     AS lead_id,
    cm."ContactId"                  AS contact_id,
    cm."AccountId"                  AS account_id,
    cm."LeadOrContactId"            AS lead_or_contact_id,
    cm."Name"                       AS member_name,
    cm."Title"                      AS member_title,
    cm."Email"                      AS member_email,
    cm."CompanyOrAccount"           AS company_or_account,
    -- member-level engagement
    cm."Status"                     AS member_status,
    cm."HasResponded"               AS has_responded,
    cm."FirstRespondedDate"         AS first_responded_date,
    -- ownership
    cm."LeadOrContactOwnerId"       AS member_owner_user_id,
    u.user_name                     AS member_owner_name,
    -- timing
    cm."CreatedDate"                AS added_at,
    cm."CreatedDate"::date          AS added_date
FROM bronze."sf_CampaignMember"  cm
LEFT JOIN bronze."sf_Campaign"   c ON c."Id"     = cm."CampaignId"
LEFT JOIN silver.v_sales_users   u ON u.user_id  = cm."LeadOrContactOwnerId"
WHERE cm."IsDeleted" = false;

COMMENT ON VIEW silver.v_sales_campaign_members IS
'Campaign↔member bridge (M:N). One row per CampaignMember. Join lead_id -> v_sales_leads for lead memberships; campaign_id -> v_sales_campaigns. v_sales_campaigns rolls up its member metrics from this view.';
