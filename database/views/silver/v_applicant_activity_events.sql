-- ============================================================
-- silver.v_applicant_activity_events
--
-- Grain: one row per funnel event (from portal_requisition_statistics), after
--        collapsing duplicate same-day events of the same type for the same
--        (applicant, requisition) — see the funnel_events CTE.
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
--
-- Excluded recruiters (events suppressed, names never exposed):
--   441  Becky Henke   (bhenke@innosource.com)
--   5673 Mason Dail    (mdail@innosource.com)
-- ============================================================

DROP VIEW IF EXISTS silver.v_applicant_activity_events;

CREATE VIEW silver.v_applicant_activity_events AS

WITH

-- Collapse duplicate same-day funnel events to one row per event.
-- Portal 1 lets the same funnel action be logged more than once for the same
-- applicant on the same requisition on the same day — usually one recruiter
-- clicking the same action twice (>95% of duplicates are single-recruiter),
-- sometimes two recruiters logging the same step on a shared class. Those are
-- data-entry duplicates, NOT distinct actions, and they inflate every COUNT(*)
-- over this view (e.g. Accept Offer ~14%, Client Interview ~5.5%, Hired ~5%).
-- Mirrors the same-day de-dup in v_hires_unified / v_terminations_unified.
-- De-dupe key: (applicant_id, requisition_id, event_type, DATE(created_at)) —
-- event_type is in the key so distinct steps on the same day (a phone screen
-- AND an offer) are NOT collapsed. Keep the earliest of the day (the original
-- action), which also fixes recruiter attribution to whoever logged it first.
-- The type filter and recruiter exclusion run INSIDE this CTE so the kept row
-- is always an in-scope, non-excluded event.
funnel_events AS (
    SELECT *
    FROM (
        SELECT
            rs.*,
            ROW_NUMBER() OVER (
                PARTITION BY rs.applicant_id, rs.requisition_id,
                             rs.requisition_statistic_type_id, DATE(rs.created_at)
                ORDER BY rs.created_at, rs.id
            ) AS dup_rn
        FROM bronze.portal_requisition_statistics rs
        WHERE rs.requisition_statistic_type_id IN (2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13)
          AND rs.created_by_recruiter_id NOT IN (441, 5673)  -- exclude: Becky Henke, Mason Dail
    ) ranked
    WHERE dup_rn = 1
),

recruiters AS (
    -- Excludes specific users from recruiter resolution. Their events are
    -- also dropped inside the funnel_events CTE above.
    SELECT
        u.id,
        u.first_name || ' ' || u.last_name AS recruiter_name,
        u.email                             AS recruiter_email
    FROM bronze.portal_users u
    JOIN bronze.portal_role_user ru ON ru.user_id = u.id
    WHERE ru.role_id = 6
      AND u.id NOT IN (441, 5673)   -- exclude: Becky Henke, Mason Dail
)

SELECT
    -- ── Source ──────────────────────────────────────────────
    'portal_v1'::text                               AS source_system,

    -- ── Event identity ──────────────────────────────────────
    rs.id                                           AS event_id,
    rs.created_at                                   AS event_at,
    DATE(rs.created_at)                             AS event_date,
    EXTRACT(isoyear FROM rs.created_at)::int        AS event_iso_year,
    EXTRACT(week    FROM rs.created_at)::int        AS event_iso_week,

    -- ── Event classification ─────────────────────────────────
    rs.requisition_statistic_type_id                AS event_type_id,
    rst.name                                        AS event_type_name,

    CASE rs.requisition_statistic_type_id
        WHEN 2  THEN 'PHONE_SCREEN'
        WHEN 3  THEN 'INNO_INTERVIEW'
        WHEN 4  THEN 'CLIENT_INTERVIEW'
        WHEN 5  THEN 'OFFER'
        WHEN 6  THEN 'HIRED'
        WHEN 8  THEN 'OFFER_ACCEPTED'
        WHEN 9  THEN 'OFFER_DECLINED'
        WHEN 10 THEN 'OFFER_DECLINED'
        WHEN 11 THEN 'OFFER_DECLINED'
        WHEN 12 THEN 'OFFER_DECLINED'
        WHEN 13 THEN 'OFFER_DECLINED'
    END                                             AS event_category,

    CASE rs.requisition_statistic_type_id
        WHEN 2  THEN 1
        WHEN 3  THEN 2
        WHEN 4  THEN 3
        WHEN 5  THEN 4
        WHEN 8  THEN 4
        WHEN 9  THEN 4
        WHEN 10 THEN 4
        WHEN 11 THEN 4
        WHEN 12 THEN 4
        WHEN 13 THEN 4
        WHEN 6  THEN 5
    END                                             AS funnel_stage,

    -- ── Applicant ────────────────────────────────────────────
    rs.applicant_id,
    a.first_name                                    AS applicant_first_name,
    a.last_name                                     AS applicant_last_name,
    a.first_name || ' ' || a.last_name              AS applicant_full_name,

    -- ── Position / Order (Portal V2 naming convention) ───────
    rs.requisition_id                               AS position_id,
    rs.requisition_id                               AS order_id,
    r.requisition_key                               AS position_key,
    r.position                                      AS position_title,
    r.is_pipeline,

    -- ── Client ───────────────────────────────────────────────
    COALESCE(r.client_id, a.client_id)              AS client_id,
    c.name                                          AS client_name,

    -- ── Recruiter who performed the action ───────────────────
    rs.created_by_recruiter_id                      AS recruiter_id,
    rec.recruiter_name,
    rec.recruiter_email,

    -- ── ISO week label ───────────────────────────────────────
    (EXTRACT(isoyear FROM rs.created_at)::text || '-W'
        || LPAD(EXTRACT(week FROM rs.created_at)::text, 2, '0')) AS iso_week_label

FROM funnel_events rs
JOIN  bronze.portal_requisition_statistic_types rst
        ON rst.id = rs.requisition_statistic_type_id
LEFT JOIN bronze.portal_applicants a
        ON a.id = rs.applicant_id
LEFT JOIN bronze.portal_requisitions r
        ON r.id = rs.requisition_id
LEFT JOIN bronze.portal_clients c
        ON c.id = COALESCE(r.client_id, a.client_id)
LEFT JOIN recruiters rec
        ON rec.id = rs.created_by_recruiter_id
-- The event-type filter and recruiter exclusion now live in the funnel_events
-- CTE, alongside the same-day de-duplication.
;
