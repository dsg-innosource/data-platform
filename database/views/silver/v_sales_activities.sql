-- ============================================================================
-- silver.v_sales_activities
-- ----------------------------------------------------------------------------
-- Purpose : Sales activity events normalized into clean activity types
-- Grain   : One row per non-deleted SF Task
-- Notes   : activity_type is the human-readable category.
--           is_outbound_touch follows the "emails + calls + LinkedIn outreach"
--           definition decided with the CRO (Question 5).
-- Depends : silver.v_sales_users
-- ============================================================================

DROP VIEW IF EXISTS silver.v_sales_activities;

CREATE VIEW silver.v_sales_activities AS
SELECT
    t."Id"                          AS activity_id,
    t."Subject"                     AS subject,
    t."Description"                 AS description,
    t."ActivityDate"                AS activity_date,
    t."CreatedDate"                 AS created_at,
    t."CompletedDateTime"           AS completed_at,
    t."Status"                      AS status,
    t."IsClosed"                    AS is_closed,
    t."Priority"                    AS priority,
    -- ownership
    t."OwnerId"                     AS owner_user_id,
    u.user_name                     AS owner_name,
    u.manager_user_id               AS owner_manager_user_id,
    u.manager_name                  AS owner_manager_name,
    -- relationships
    t."WhoId"                       AS contact_or_lead_id,
    t."WhatId"                      AS related_to_id,
    t."AccountId"                   AS account_id,
    -- type classification
    t."TaskSubtype"                 AS task_subtype_raw,
    t."SalesLoft1__SalesLoft_Type__c" AS salesloft_type_raw,
    CASE
        WHEN t."TaskSubtype" = 'Email'
          OR t."SalesLoft1__SalesLoft_Type__c" = 'Email'                  THEN 'Email'
        WHEN t."TaskSubtype" = 'Call'
          OR t."SalesLoft1__SalesLoft_Type__c" = 'Call'                   THEN 'Call'
        WHEN t."TaskSubtype" = 'LinkedIn'
          OR t."SalesLoft1__SalesLoft_Type__c" LIKE 'LinkedIn%'           THEN 'LinkedIn'
        WHEN t."SalesLoft1__SalesLoft_Type__c" = 'Reply'                  THEN 'Email Reply'
        WHEN t."SalesLoft1__SalesLoft_Type__c" = 'Conversation Intelligence Recording'
                                                                          THEN 'Call Recording'
        ELSE 'Other'
    END                             AS activity_type,
    -- outbound classification (per CRO Q5)
    CASE
        WHEN t."TaskSubtype" = 'Email'
          OR t."SalesLoft1__SalesLoft_Type__c" = 'Email'                  THEN true
        WHEN t."TaskSubtype" = 'Call'
          OR t."SalesLoft1__SalesLoft_Type__c" = 'Call'                   THEN true
        WHEN t."SalesLoft1__SalesLoft_Type__c"
                IN ('LinkedIn - Connection Request','LinkedIn - InMail')  THEN true
        ELSE false
    END                             AS is_outbound_touch,
    -- SalesLoft cadence context
    t."SalesLoft1__SalesLoft_Cadence_Name__c"  AS salesloft_cadence_name,
    t."SalesLoft1__SalesLoft_Step_Day__c"      AS salesloft_step_day,
    t."SalesLoft1__SalesLoft_Email_Template_Title__c" AS salesloft_email_template,
    -- engagement signals (when populated)
    t."SalesLoft1__SalesLoft_View_Count__c"    AS email_view_count,
    t."SalesLoft1__SalesLoft_Clicked_Count__c" AS email_click_count,
    t."SalesLoft1__SalesLoft_Replies_Count__c" AS email_reply_count,
    t."SalesLoft1__Call_Sentiment__c"          AS call_sentiment,
    t."SalesLoft1__Call_Disposition__c"        AS call_disposition,
    t."SalesLoft1__Call_Duration_in_Minutes__c" AS call_duration_minutes
FROM bronze."sf_Task" t
LEFT JOIN silver.v_sales_users u ON u.user_id = t."OwnerId"
WHERE t."IsDeleted" = false;

COMMENT ON VIEW silver.v_sales_activities IS
'Sales activity events (Tasks). Use is_outbound_touch = true for the "Outbound Touches" KPI.';
