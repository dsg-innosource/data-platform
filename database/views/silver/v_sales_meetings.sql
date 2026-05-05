-- ============================================================================
-- silver.v_sales_meetings
-- ----------------------------------------------------------------------------
-- Purpose : Customer meetings, deduplicated and classified
-- Grain   : One row per non-deleted, non-child SF Event
-- Notes   :
--   * EXCLUDED: deleted, child events (multi-attendee duplicates),
--               internal recurring meetings (subject ILIKE 'Recurring Meeting Booked:%')
--   * is_qualified_meeting: meeting linked to an Opportunity (strongest signal)
--   * is_salesloft_sourced: meeting came from outbound cadence
--     (subject ILIKE 'Meeting Booked:%' OR SalesLoft type = 'Meeting')
--   * is_group_event: has external attendees
-- Depends : silver.v_sales_users, silver.v_sales_accounts
-- ============================================================================

DROP VIEW IF EXISTS silver.v_sales_meetings;

CREATE VIEW silver.v_sales_meetings AS
SELECT
    e."Id"                          AS meeting_id,
    e."Subject"                     AS subject,
    e."Description"                 AS description,
    e."Location"                    AS location,
    e."ActivityDate"::date          AS meeting_date,
    e."StartDateTime"               AS start_at,
    e."EndDateTime"                 AS end_at,
    e."DurationInMinutes"           AS duration_minutes,
    -- ownership
    e."OwnerId"                     AS owner_user_id,
    u.user_name                     AS owner_name,
    u.manager_user_id               AS owner_manager_user_id,
    u.manager_name                  AS owner_manager_name,
    -- relationships
    e."AccountId"                   AS account_id,
    a.account_name                  AS account_name,
    e."WhoId"                       AS contact_or_lead_id,
    e."WhatId"                      AS related_to_id,
    e."Type"                        AS meeting_type,
    -- ============= DERIVED FLAGS =============
    -- meeting linked to an Opportunity record (006... prefix is Opp Id)
    CASE WHEN e."WhatId" LIKE '006%' THEN true ELSE false END AS is_qualified_meeting,
    -- meeting came from outbound prospecting effort
    CASE
        WHEN e."Subject" ILIKE 'Meeting Booked:%'             THEN true
        WHEN e."SalesLoft1__SalesLoft_Type__c" = 'Meeting'    THEN true
        ELSE false
    END                             AS is_salesloft_sourced,
    -- multi-attendee event (has external participants)
    e."IsGroupEvent"                AS is_group_event,
    -- audit
    e."CreatedDate"                 AS created_at,
    e."LastModifiedDate"            AS last_modified_at
FROM bronze."sf_Event" e
LEFT JOIN silver.v_sales_users    u ON u.user_id    = e."OwnerId"
LEFT JOIN silver.v_sales_accounts a ON a.account_id = e."AccountId"
WHERE e."IsDeleted" = false
  AND COALESCE(e."IsChild", false) = false
  AND COALESCE(e."Subject", '') NOT ILIKE 'Recurring Meeting Booked:%';

COMMENT ON VIEW silver.v_sales_meetings IS
'Customer meetings (excluding internal recurring + multi-attendee duplicates). Use is_qualified_meeting = true for the deck KPI.';
