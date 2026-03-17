-- ============================================================
-- silver.v_applicant_activity_events
--
-- Grain: one row per funnel event (from portal_requisition_statistics)
-- Sources: portal_v1 only — structured for future portal_v2 union
--
-- Included event types:
--   2  Phone Screen
--   3  Interview (Inno Interview)
--   4  Client Interview
--   5  Offer
--   6  Hired
--   8  Accept Offer
--   9  Decline Offer - Pay Rate
--  10  Decline Offer - Another Job
--  11  Decline Offer - Job Fit
--  12  Decline Offer - Start Date
--  13  Decline Offer - Other
--
-- Excluded (intentionally):
--   1  Assigned to Requisition   — positional, not a funnel action
--   7  Rejected - Regional Pool  — volume noise, not a recruiter action
--  14  No call, no show          — stale (last seen 2021)
--  15  Failed Pre-Employment     — stale (last seen 2021)
--  16  Failed Drug Screen        — stale (last seen 2021)
--  17  Phone Screen Initialized  — AI-initiated, not recruiter-completed
-- 680-683  Chatbot events        — stale (last seen 2024)
-- ============================================================

DROP VIEW IF EXISTS silver.v_applicant_activity_events;

CREATE VIEW silver.v_applicant_activity_events AS

WITH recruiters AS (
    SELECT
        u.id,
        u.first_name || ' ' || u.last_name AS recruiter_name,
        u.email AS recruiter_email
    FROM bronze.portal_users u
    JOIN bronze.portal_role_user ru ON ru.user_id = u.id
    WHERE ru.role_id = 6
)

SELECT
    -- Source / system
    'portal_v1'::text                          AS source_system,

    -- Event identity
    rs.id                                      AS event_id,
    rs.created_at                              AS event_at,
    DATE(rs.created_at)                        AS event_date,
    EXTRACT(isoyear FROM rs.created_at)::int   AS event_iso_year,
    EXTRACT(week    FROM rs.created_at)::int   AS event_iso_week,

    -- Event classification
    rs.requisition_statistic_type_id           AS event_type_id,
    rst.name                                   AS event_type,
    CASE rs.requisition_statistic_type_id
        WHEN 2  THEN 'PHONE_SCREEN'
        WHEN 3  THEN 'INNO_INTERVIEW'
        WHEN 4  THEN 'CLIENT_INTERVIEW'
        WHEN 5  THEN 'OFFER'
        WHEN 8  THEN 'OFFER_ACCEPTED'
        WHEN 6  THEN 'HIRED'
        WHEN 9  THEN 'OFFER_DECLINED'
        WHEN 10 THEN 'OFFER_DECLINED'
        WHEN 11 THEN 'OFFER_DECLINED'
        WHEN 12 THEN 'OFFER_DECLINED'
        WHEN 13 THEN 'OFFER_DECLINED'
        ELSE         'OTHER'
    END                                        AS event_category,

    -- Applicant
    rs.applicant_id,
    a.first_name,
    a.last_name,
    a.first_name || ' ' || a.last_name         AS full_name,

    -- Position / Order (Portal V2 naming)
    rs.requisition_id                          AS position_id,
    rs.requisition_id                          AS order_id,
    r.requisition_key                          AS position_key,
    r.position                                 AS position_title,
    r.client_id,
    c.name                                     AS client_name,

    -- Recruiter who performed the action
    rs.created_by_recruiter_id                 AS recruiter_id,
    rec.recruiter_name,
    rec.recruiter_email,

    -- Assigned reqs (formal / positional) — for Section 03 context
    -- This is looked up at the requisition level, not the event level.
    -- Consumers use this to JOIN back to v_orders_unified if needed.

    -- ISO date helpers
    (EXTRACT(isoyear FROM rs.created_at)::text || '-W'
        || LPAD(EXTRACT(week FROM rs.created_at)::text, 2, '0')) AS iso_week_label

FROM bronze.portal_requisition_statistics rs
JOIN bronze.portal_requisition_statistic_types rst
    ON rst.id = rs.requisition_statistic_type_id
LEFT JOIN bronze.portal_applicants a
    ON a.id = rs.applicant_id
LEFT JOIN bronze.portal_requisitions r
    ON r.id = rs.requisition_id
LEFT JOIN bronze.portal_clients c
    ON c.id = r.client_id
LEFT JOIN recruiters rec
    ON rec.id = rs.created_by_recruiter_id

WHERE rs.requisition_statistic_type_id IN (2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13)
  AND rs.created_at >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '2 years'
;