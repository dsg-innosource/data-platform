-- ============================================================================
-- Silver Layer View: v_hires_unified
-- Purpose: Present Portal 1.0 hire events in Portal 2.0 structure.
--          Mirrors v_terminations_unified design for symmetry; the two views
--          are intended to join on applicant_id (+ position_id) in reports.
-- Created:  2026-04-17
-- ============================================================================
-- DESIGN NOTES:
--   Grain      : One row per "Hired" funnel event
--                (bronze.portal_requisition_statistics.requisition_statistic_type_id = 6)
--   Row key    : event_id (portal_requisition_statistics.id) — the
--                differentiating grain for applicants hired more than once.
--   Vocabulary : Cajetan (Portal 2) field naming throughout.
--                Portal 1 source fields aliased to match.
--
--   TWO DATES IN THIS VIEW — know which to use:
--
--     event_at / event_date / iso_year / iso_week
--       When the recruiter logged the "Hired" event. The decision to hire.
--       Use for recruiter-activity and "hires made this week" reporting.
--
--     actual_start_date / start_iso_year / start_iso_week
--       Decoration from portal_users.hire_date — the official first day
--       of work. NULL when the user record has no hire_date (applicant
--       was marked hired but never started, or start date not yet entered).
--       Use for "started this week" reporting and tenure math.
--
--     days_to_start = actual_start_date - event_date
--       Positive  = normal onboarding lag
--       Zero      = hired and started same day
--       Negative  = start date backdated
--       NULL      = no start date recorded
--
--   "Never started" cohort:
--     Rows where actual_start_date IS NULL AND a matching termination
--     exists with termination_reason_id = 13 (see v_terminations_unified,
--     column is_excluded). Pair via applicant_id + position_id.
--
--   In Portal 1 : requisition = position = order (1:1:1).
--                 position_id = order_id = rs.requisition_id
--   In Portal 2 : positions and orders are separate (1:many).
--
--   Multiple hires per applicant:
--     Yes. An applicant can appear on multiple rows when hired onto
--     different requisitions (or rehired onto the same one). event_id
--     is the natural differentiator. For "latest hire per applicant":
--       ROW_NUMBER() OVER (PARTITION BY applicant_id ORDER BY event_at DESC) = 1
--
--     LIMITATION — rehires: actual_start_date is decorated from
--     canonical_users, which collapses portal_users to one row per
--     applicant_id (prefers is_active = TRUE, then latest created_at).
--     For applicants with multiple historical stints, every event row
--     gets the SAME (most recent) actual_start_date. This is correct for
--     the latest hire event but imprecise for older rehire events. Accept
--     unless rehire analysis is a primary use case.
--
--   Recruiter exclusions : NONE. All recruiters are included, even those
--     suppressed in v_applicant_activity_events (441 Becky Henke,
--     5673 Mason Dail). This is a fact view — suppressions belong in
--     reporting, not at the silver grain.
--
--   Recruiter field : rs.created_by_recruiter_id — the user who logged
--     the hire event. This is NOT necessarily the applicant's assigned
--     recruiter (portal_applicants.recruiter_id); the two can differ.
--     The event creator matches the "decision to hire" semantic we want.
--
--   Name source : portal_applicants.first_name/last_name (unlike
--     v_terminations_unified, which reads from portal_users). Hire events
--     fire at the applicant stage, before a user record necessarily
--     exists, so portal_applicants is the more reliable source here.
--
--   Date scope : NONE. Callers filter by event_date or actual_start_date.
--
--   Source (now)    : bronze.portal_requisition_statistics (source_system = 'portal_v1')
--   Source (future) : cajetan.<hire_events>                 (source_system = 'cajetan')
--                     Add as UNION ALL branch when Cajetan is in the warehouse.
--                     Column list must exactly match the portal_v1 branch.
--
-- ----------------------------------------------------------------------------
-- JOIN LOGIC FOR REPORTS
-- ----------------------------------------------------------------------------
--   Join keys with other unified views:
--     v_hires_unified.position_id    = v_positions_unified.position_id
--     v_hires_unified.order_id       = v_orders_unified.order_id
--     v_hires_unified.applicant_id   = v_terminations_unified.applicant_id
--     (both same value in Portal 1; diverge in Portal 2)
--
--   Recommended hire → termination pairing (churn / tenure analysis):
--     Portal 1 has no direct FK from a hire event to a termination. Match
--     on (applicant_id, position_id) and take the first termination on or
--     after the hire's actual_start_date (or event_date if start is NULL).
--
--     SELECT
--         h.event_id,
--         h.applicant_id,
--         h.client_name,
--         h.position_title,
--         h.event_date             AS hire_event_date,
--         h.actual_start_date,
--         t.termination_id,
--         t.termination_date,
--         t.termination_category,
--         t.tenure_days
--     FROM silver.v_hires_unified h
--     LEFT JOIN LATERAL (
--         SELECT *
--         FROM silver.v_terminations_unified t
--         WHERE t.applicant_id = h.applicant_id
--           AND t.position_id  = h.position_id
--           AND t.termination_date >= COALESCE(h.actual_start_date, h.event_date)
--         ORDER BY t.termination_date
--         LIMIT 1
--     ) t ON TRUE;
--
--   Active (not-yet-terminated) hires:
--     Same LATERAL pattern, then WHERE t.termination_id IS NULL.
--
-- ----------------------------------------------------------------------------
-- CALLER EXAMPLES
-- ----------------------------------------------------------------------------
--   -- Hires logged in the current ISO week (recruiter activity view)
--   SELECT * FROM silver.v_hires_unified
--   WHERE iso_year = EXTRACT(isoyear FROM CURRENT_DATE)
--     AND iso_week = EXTRACT(week    FROM CURRENT_DATE);
--
--   -- Hires that actually started YTD (event-anchored started cohort)
--   SELECT * FROM silver.v_hires_unified
--   WHERE actual_start_date >= DATE_TRUNC('year', CURRENT_DATE);
--
--   -- "Never started yet" — hire logged 14+ days ago, no start date
--   SELECT * FROM silver.v_hires_unified
--   WHERE actual_start_date IS NULL
--     AND event_date <= CURRENT_DATE - INTERVAL '14 days'
--     AND event_date >= CURRENT_DATE - INTERVAL '6 months';
-- ============================================================================


DROP VIEW IF EXISTS silver.v_hires_unified;

CREATE VIEW silver.v_hires_unified AS

-- ── Portal 1 branch ───────────────────────────────────────────────────────
WITH

-- 1. Resolve duplicate portal_users → one canonical row per applicant_id.
--    Same logic as v_terminations_unified.canonical_users:
--      prefers is_active = TRUE, then most recently created record.
--    Used only to decorate each hire event with actual_start_date.
canonical_users AS (
    SELECT DISTINCT ON (applicant_id)
        id                                      AS user_id,
        applicant_id,
        hire_date
    FROM bronze.portal_users
    WHERE applicant_id IS NOT NULL
    ORDER BY
        applicant_id,
        is_active   DESC,
        created_at  DESC
),

-- 2. Recruiter display names — recruiters are also portal_users rows.
--    NO exclusions here (unlike v_applicant_activity_events). This is a
--    fact view; every hire event counts regardless of who logged it.
recruiters AS (
    SELECT
        u.id,
        u.first_name || ' ' || u.last_name      AS recruiter_name
    FROM bronze.portal_users u
    INNER JOIN bronze.portal_role_user ru ON u.id = ru.user_id
    WHERE ru.role_id = 6
)

SELECT

    -- ── Source tracking ───────────────────────────────────────────────────
    'portal_v1'                                 AS source_system,

    -- ── Hire event (primary grain) ────────────────────────────────────────
    rs.id                                       AS event_id,
    rs.created_at                               AS event_at,
    DATE(rs.created_at)                         AS event_date,
    rs.requisition_statistic_type_id            AS event_type_id,
    rst.name                                    AS event_type_name,

    -- ── Person ────────────────────────────────────────────────────────────
    -- Name from portal_applicants (reliable at hire time); user_id from
    -- canonical_users is nullable (rare — applicant with no user row).
    cu.user_id,
    rs.applicant_id,
    a.first_name,
    a.last_name,
    a.first_name || ' ' || a.last_name          AS full_name,

    -- ── Position / Order context ──────────────────────────────────────────
    -- Portal 1: position_id = order_id = requisition_id (1:1).
    rs.requisition_id                           AS position_id,
    rs.requisition_id                           AS order_id,
    r.requisition_key                           AS position_key,
    r.position                                  AS position_title,
    r.is_pipeline,

    -- ── Client ────────────────────────────────────────────────────────────
    -- Primary: requisition.client_id (req the hire was made against).
    -- Fallback: applicant.client_id (rare — req missing a client).
    COALESCE(r.client_id, a.client_id)          AS client_id,
    c.name                                      AS client_name,

    -- ── Hire timing: decision date vs. actual start date ──────────────────
    cu.hire_date                                AS actual_start_date,
    CASE
        WHEN cu.hire_date IS NOT NULL
        THEN (cu.hire_date - DATE(rs.created_at))::int
        ELSE NULL
    END                                         AS days_to_start,

    -- ── Recruiter (who logged the hire event) ─────────────────────────────
    rs.created_by_recruiter_id                  AS recruiter_id,
    rec.recruiter_name,

    -- ── ISO week helpers: EVENT DATE axis ─────────────────────────────────
    -- "Hires made this week" — recruiter-activity reporting.
    EXTRACT(isoyear FROM rs.created_at)::int    AS iso_year,
    EXTRACT(week    FROM rs.created_at)::int    AS iso_week,

    -- ── ISO week helpers: START DATE axis ─────────────────────────────────
    -- "Started this week" — onboarded-employee reporting. NULL until
    -- the user record has a hire_date.
    EXTRACT(isoyear FROM cu.hire_date)::int     AS start_iso_year,
    EXTRACT(week    FROM cu.hire_date)::int     AS start_iso_week

FROM bronze.portal_requisition_statistics rs

INNER JOIN bronze.portal_requisition_statistic_types rst
    ON rst.id = rs.requisition_statistic_type_id

LEFT JOIN bronze.portal_applicants a
    ON a.id = rs.applicant_id

LEFT JOIN canonical_users cu
    ON cu.applicant_id = rs.applicant_id

LEFT JOIN bronze.portal_requisitions r
    ON r.id = rs.requisition_id

LEFT JOIN bronze.portal_clients c
    ON c.id = COALESCE(r.client_id, a.client_id)

LEFT JOIN recruiters rec
    ON rec.id = rs.created_by_recruiter_id

WHERE rs.requisition_statistic_type_id = 6   -- Hired


-- ══════════════════════════════════════════════════════════════════════════
-- FUTURE: Cajetan (Portal 2) UNION ALL branch
-- Add when cajetan schema is available in the warehouse.
-- Column list and order must exactly match the portal_v1 branch above.
-- ══════════════════════════════════════════════════════════════════════════
--
-- UNION ALL
-- SELECT
--     'cajetan'::text                                 AS source_system,
--     h.id                                            AS event_id,
--     h.created_at                                    AS event_at,
--     DATE(h.created_at)                              AS event_date,
--     ... (all fields matching portal_v1 branch) ...
-- FROM cajetan.<hire_events> h
-- ...
;


COMMENT ON VIEW silver.v_hires_unified IS
'Unified hires view. One row per Hired funnel event
(portal_requisition_statistics.requisition_statistic_type_id = 6).
Uses Cajetan (Portal 2) vocabulary; Portal 1 fields aliased to match.

TWO DATES:
  event_at / event_date / iso_year / iso_week
    When the recruiter logged the Hired event (the decision to hire).
  actual_start_date / start_iso_year / start_iso_week
    Decoration from portal_users.hire_date. NULL if never started
    or start date not yet entered.
  days_to_start: actual_start_date - event_date. NULL if no start date.

Recruiter: rs.created_by_recruiter_id (event creator) — may differ from
the applicants assigned recruiter. No recruiter exclusions applied.

Name source: portal_applicants (reliable at hire time), not portal_users.

Portal 1: position_id = order_id = requisition_id (1:1).
Portal 2: position_id != order_id (add UNION ALL branch when Cajetan is live).

Join keys:
  position_id   -> v_positions_unified.position_id
  order_id      -> v_orders_unified.order_id
  applicant_id  -> v_terminations_unified.applicant_id (pair hire to termination;
                   use LATERAL on (applicant_id, position_id) + termination_date
                   >= COALESCE(actual_start_date, event_date))
  (source_system, event_id) for unique row identity across sources.

Rehire limitation: actual_start_date is decorated per applicant_id (not per
event), so applicants with multiple stints get the most recent start date on
every event row. Correct for the latest event; imprecise for older rehires.';


-- ============================================================================
-- VALIDATION QUERIES
-- Run after deploying to confirm correctness.
-- ============================================================================

-- 1. Row count — should match hire events in bronze
--    SELECT COUNT(*) FROM silver.v_hires_unified;
--    SELECT COUNT(*) FROM bronze.portal_requisition_statistics
--      WHERE requisition_statistic_type_id = 6;

-- 2. No duplicate event_ids within a source_system
--    SELECT source_system, event_id, COUNT(*)
--    FROM silver.v_hires_unified
--    GROUP BY 1, 2
--    HAVING COUNT(*) > 1;

-- 3. Start-date resolution rate — how many hires have an actual_start_date
--    SELECT
--      COUNT(*)                                        AS total,
--      COUNT(actual_start_date)                        AS started,
--      COUNT(*) - COUNT(actual_start_date)             AS no_start_date,
--      ROUND(100.0 * COUNT(actual_start_date) / COUNT(*), 1) AS pct_with_start
--    FROM silver.v_hires_unified;

-- 4. days_to_start distribution — spot-check onboarding lag
--    SELECT
--      CASE
--        WHEN days_to_start IS NULL    THEN 'no start date'
--        WHEN days_to_start < 0        THEN 'backdated'
--        WHEN days_to_start = 0        THEN 'same day'
--        WHEN days_to_start <= 7       THEN '1–7 days'
--        WHEN days_to_start <= 14      THEN '8–14 days'
--        WHEN days_to_start <= 30      THEN '15–30 days'
--        ELSE '31+ days'
--      END AS start_lag_bucket,
--      COUNT(*)
--    FROM silver.v_hires_unified
--    GROUP BY 1
--    ORDER BY MIN(COALESCE(days_to_start, 99999));

-- 5. Client resolution — should be near 100%
--    SELECT
--      COUNT(*)                               AS total,
--      COUNT(client_name)                     AS with_client,
--      COUNT(*) - COUNT(client_name)          AS orphan,
--      ROUND(100.0 * COUNT(client_name) / COUNT(*), 1) AS pct_resolved
--    FROM silver.v_hires_unified;

-- 6. Multi-hire applicants — how many appear more than once
--    SELECT applicant_id, COUNT(*) AS hire_events
--    FROM silver.v_hires_unified
--    GROUP BY 1
--    HAVING COUNT(*) > 1
--    ORDER BY 2 DESC
--    LIMIT 20;

-- 7. Current ISO week hires (operational view)
--    SELECT full_name, client_name, position_title,
--           event_date, actual_start_date, days_to_start,
--           recruiter_name, is_pipeline
--    FROM silver.v_hires_unified
--    WHERE iso_year = EXTRACT(isoyear FROM CURRENT_DATE)
--      AND iso_week = EXTRACT(week    FROM CURRENT_DATE)
--    ORDER BY event_at DESC, client_name, full_name;

-- 8. Hire → termination pairing spot-check (see header for full pattern)
--    SELECT
--        h.event_id, h.applicant_id, h.full_name, h.client_name,
--        h.event_date, h.actual_start_date,
--        t.termination_id, t.termination_date, t.termination_category,
--        t.tenure_days
--    FROM silver.v_hires_unified h
--    LEFT JOIN LATERAL (
--        SELECT *
--        FROM silver.v_terminations_unified t
--        WHERE t.applicant_id = h.applicant_id
--          AND t.position_id  = h.position_id
--          AND t.termination_date >= COALESCE(h.actual_start_date, h.event_date)
--        ORDER BY t.termination_date
--        LIMIT 1
--    ) t ON TRUE
--    WHERE h.event_date >= CURRENT_DATE - INTERVAL '90 days'
--    ORDER BY h.event_at DESC;
