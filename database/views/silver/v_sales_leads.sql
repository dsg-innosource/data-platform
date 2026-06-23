-- ============================================================================
-- silver.v_sales_leads
-- ----------------------------------------------------------------------------
-- Purpose : Lead funnel view — every Lead with lifecycle timing & attribution.
--           Powers the BDR-stage of the two-stage funnel and the
--           Accepted -> Qualified control metric (BDR Lead-to-Opp SOP).
-- Grain   : One row per non-deleted SF Lead.
-- Notes   : * Lifecycle timestamps (working_at .. disqualified_at) are derived
--             from bronze.sf_LeadHistory (field-history tracking on Lead Status).
--             SF does NOT backfill history, so these are NULL until a lead's
--             status actually changes going forward — expect them sparse until
--             the BDRs work the 676 leads currently at New.
--           * Forward-ready: the Engaged/Meeting Set/Accepted/Held/Disqualified
--             statuses don't exist in the picklist yet (pending the Lead-to-Opp
--             build). Their columns resolve to NULL until those values appear.
--           * meeting_accepted_at is a typed NULL placeholder for now. It will
--             be sourced from the custom field Accepted_Date__c (stamped by the
--             acceptance Flow) once the Lead-to-Opp build adds it — NOT derived
--             from history. Swap the NULL for l."Accepted_Date__c" at that point.
--           * bdr_owner_name = the owner BEFORE the first ownership change
--             (the originating BDR); falls back to current owner when no
--             handoff has occurred. The BDR->SE handoff is an Owner change at
--             Meeting Accepted, so this reconstructs origin even after transfer.
--           * Test/junk leads are FLAGGED (is_test_lead), not filtered, matching
--             the v_sales_opportunities pattern. Filter is_test_lead = false.
-- Depends : silver.v_sales_users, silver.v_sales_opportunities,
--           bronze.sf_Lead, bronze.sf_LeadHistory
-- ============================================================================

DROP VIEW IF EXISTS silver.v_sales_leads;

CREATE VIEW silver.v_sales_leads AS
WITH status_hist AS (
    -- first time each status was set, per lead (Status field history)
    SELECT
        "LeadId",
        MIN("CreatedDate") FILTER (WHERE "NewValue" = 'Working')         AS working_at,
        MIN("CreatedDate") FILTER (WHERE "NewValue" LIKE 'Engaged%')      AS engaged_at,
        MIN("CreatedDate") FILTER (WHERE "NewValue" = 'Meeting Set')      AS meeting_set_at,
        MIN("CreatedDate") FILTER (WHERE "NewValue" = 'Meeting Held')     AS meeting_held_at,
        MIN("CreatedDate") FILTER (WHERE "NewValue" = 'Qualified')        AS qualified_at,
        MIN("CreatedDate") FILTER (WHERE "NewValue" = 'Disqualified')     AS disqualified_at,
        MAX("CreatedDate")                                                AS last_status_change_at
    FROM bronze."sf_LeadHistory"
    WHERE "Field" = 'Status'
    GROUP BY "LeadId"
),
owner_hist AS (
    -- originating owner = OldValue of the FIRST ownership change (BDR before handoff)
    SELECT DISTINCT ON ("LeadId")
        "LeadId",
        "OldValue" AS original_owner_name
    FROM bronze."sf_LeadHistory"
    WHERE "Field" = 'Owner'
    ORDER BY "LeadId", "CreatedDate" ASC
)
SELECT
    l."Id"                              AS lead_id,
    l."Name"                            AS lead_name,
    l."FirstName"                       AS first_name,
    l."LastName"                        AS last_name,
    l."Title"                           AS title,
    l."Company"                         AS company,
    l."Email"                           AS email,
    l."Phone"                           AS phone,
    l."Industry"                        AS industry,
    -- ============= STATUS / FUNNEL =============
    l."Status"                          AS status,
    CASE
        WHEN l."Status" = 'New'              THEN 'New'
        WHEN l."Status" = 'Working'          THEN 'Working'
        WHEN l."Status" LIKE 'Engaged%'      THEN 'Engaged'
        WHEN l."Status" = 'Meeting Set'      THEN 'Meeting Set'
        WHEN l."Status" = 'Meeting Accepted' THEN 'Meeting Accepted'
        WHEN l."Status" = 'Meeting Held'     THEN 'Meeting Held'
        WHEN l."Status" = 'Qualified'        THEN 'Qualified'
        WHEN l."Status" = 'Disqualified'     THEN 'Disqualified'
        ELSE l."Status"
    END                                 AS funnel_stage,
    -- ownership band: BDR owns up to Meeting Set; SE owns from acceptance on
    CASE
        WHEN l."Status" IN ('Meeting Accepted','Meeting Held','Qualified')
        THEN 'SE' ELSE 'BDR'
    END                                 AS funnel_owner_band,
    -- ============= ATTRIBUTION / OWNERSHIP =============
    l."LeadSource"                      AS lead_source,
    l."OwnerId"                         AS current_owner_user_id,
    u.user_name                         AS current_owner_name,
    u.manager_name                      AS current_owner_manager_name,
    COALESCE(oh.original_owner_name, u.user_name) AS bdr_owner_name,
    l."SalesLoft1__Most_Recent_Cadence_Name__c"   AS most_recent_cadence,
    -- ============= LIFECYCLE TIMESTAMPS =============
    l."CreatedDate"                     AS created_at,
    l."CreatedDate"::date               AS new_date,          -- 'New' = creation
    sh.working_at,
    sh.engaged_at,
    sh.meeting_set_at,
    NULL::timestamp with time zone      AS meeting_accepted_at,  -- placeholder; source from l."Accepted_Date__c" once that custom field is built (Lead-to-Opp build #1)
    sh.meeting_held_at,
    sh.qualified_at,
    sh.disqualified_at,
    sh.last_status_change_at,
    (CURRENT_DATE - COALESCE(sh.last_status_change_at, l."CreatedDate")::date) AS days_in_current_status,
    (CURRENT_DATE - l."CreatedDate"::date)                                     AS days_since_created,
    -- ============= CONVERSION =============
    l."IsConverted"                     AS is_converted,
    l."ConvertedDate"                   AS converted_date,
    l."ConvertedOpportunityId"          AS converted_opportunity_id,
    l."ConvertedAccountId"              AS converted_account_id,
    l."ConvertedContactId"              AS converted_contact_id,
    co.opportunity_name                 AS converted_opportunity_name,
    co.stage_name                       AS converted_opportunity_stage,
    co.is_won                           AS converted_opp_is_won,
    -- ============= DERIVED FLAGS =============
    CASE WHEN l."Name" ILIKE 'TEST%' OR l."Company" ILIKE 'TEST%'
         THEN true ELSE false END       AS is_test_lead,
    CASE
        WHEN l."LeadSource" IS NULL OR l."LeadSource" = ''                       THEN 'MISSING_LEAD_SOURCE'
        WHEN l."Status" = 'New' AND l."CreatedDate" < CURRENT_DATE - INTERVAL '30 days'
                                                                                 THEN 'AGING_AT_NEW'
        ELSE 'OK'
    END                                 AS data_quality_flag
FROM bronze."sf_Lead"                  l
LEFT JOIN silver.v_sales_users         u  ON u.user_id        = l."OwnerId"
LEFT JOIN status_hist                  sh ON sh."LeadId"      = l."Id"
LEFT JOIN owner_hist                   oh ON oh."LeadId"      = l."Id"
LEFT JOIN silver.v_sales_opportunities co ON co.opportunity_id = l."ConvertedOpportunityId"
WHERE l."IsDeleted" = false;

COMMENT ON VIEW silver.v_sales_leads IS
'Lead funnel with lifecycle timing & BDR attribution. Lifecycle timestamps come from sf_LeadHistory (no backfill — sparse until leads are worked). Filter is_test_lead = false; data_quality_flag <> ''OK'' surfaces hygiene issues. Forward-ready for the new lifecycle statuses.';
