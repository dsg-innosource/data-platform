-- ============================================================================
-- Silver Layer View: v_hires_unified
-- Purpose: Present Portal 1.0 hire events in Portal 2.0 structure.
--          Mirrors v_terminations_unified design for symmetry; the two views
--          are intended to join on applicant_id (+ position_id) in reports.
-- Created:  2026-04-17
-- Modified: 2026-07-01 — EPISODE-BINDING FIX (defect 1 + new-episode visibility).
--           (1) actual_start_date / start_iso_* / days_to_start on the CONFIRMED
--               spine are now bound to the correct hire EPISODE. Previously they
--               were decorated from canonical_users.hire_date (portal_users stores
--               ONE mutable hire_date + onboarding_department_id per applicant —
--               the CURRENT stint), decorated by applicant_id ALONE, so a LATER
--               re-application's start/department was pasted onto an OLDER event
--               row. Now the stored start/onboarding-dept decorate ONLY the
--               applicant's most-recent Hired event (applicant_latest_event CTE),
--               and ONLY when the start falls on/before event_date + 365 (a start
--               years after the event provably belongs to a later, eventless
--               episode). Non-current events and far-later starts get NULL — the
--               honest value, since portal_users overwrites the older stint's
--               start (that value is not recoverable). No lower bound: a start
--               BEFORE the event is the normal event-logging lag, not a defect.
--               The ADP-PIT department lookup gains an upper window bound
--               (snapshot_date <= event_date + 365) so a far-later snapshot can no
--               longer backfill an old event's department; the onboarding-dept
--               fallback is gated to the current episode. Fixture: applicant
--               136818 (Jacqueline Adamson) — her two 2017 Nationwide events no
--               longer show actual_start_date 2026-05-04 / dept "Alliant Energy";
--               they now show NULL start / dept (2017 start overwritten). Drives
--               "confirmed rows with (start - event) > 365d" from 117 -> 0.
--           (2) NEW-EPISODE VISIBILITY: the PENDING anti-double-count guard is now
--               REQUISITION-SCOPED, not applicant-wide. Previously a pending row
--               required NOT EXISTS *any* type-6 event for the applicant, so a
--               genuinely new stint (new requisition, no logged Hired event yet)
--               for anyone ever confirmed-hired was silently dropped. Now a
--               pending row is suppressed only when a type-6 event exists on the
--               SAME inferred requisition (falling back to the applicant-wide rule
--               when no Accept Offer anchors an episode). A rehire onto a new order
--               therefore gets its OWN pending row bound to its own order/start,
--               while a confirmed hire is still never double-counted on its own
--               requisition (verified: 0 (applicant, requisition) pairs are both
--               confirmed and pending). Surfaces ~900 previously-hidden rehire
--               episodes. INVARIANT CHANGE: a person may now be 'confirmed' for one
--               requisition and 'pending' for a DIFFERENT one — uniqueness is per
--               (applicant_id, order_id), no longer per applicant.
--           (3) Evidence-gate arm 2 now ignores "Never started / delayed"
--               (termination_reason_id = 13) terminations — a never-started
--               cancellation is not proof the stint was worked, so it no longer
--               resurrects a phantom hire.
-- Modified: 2026-06-29 — Added a PENDING hire source + hire_confirmation_status
--           column (see "PENDING HIRES" below). The view is no longer driven
--           solely by the logged Hired event: candidates whose portal status
--           is "Hired" (applicant_status_id = 5) with a start date set but no
--           logged Hired event are now surfaced as 'pending' rows, UNION ALL'd
--           into the existing event-backed ('confirmed') spine. Closes the
--           hire-logging lag that hid whole started classes from "starting this
--           week" reporting (GiftHealth 6/22 class — 11 started, 0 logged).
-- Modified: 2026-06-03 — Added offering enrichment (department_code, offering,
--           service, is_offering_excluded, offering_source). Resolves the
--           department-at-hire-time via a LATERAL lookup into
--           bronze.adp_tenure_history (earliest snapshot on/after the event
--           date for the applicant); falls back to
--           portal_users.onboarding_department_id when no ADP row exists.
--           Joined to silver.v_department_offering for the offering label.
--           NOTE: bronze.portal_associates was considered and rejected as a
--           source for current placement — its `id` column overlaps with
--           portal_users.id only by numeric coincidence (~32% of associate
--           rows reference different humans from different time periods, e.g.,
--           an associate record created in 2021 colliding with a user created
--           in 2026). Use adp_tenure_history.applicant_id as the linkage.
--
-- ⚠ DEPLOY VIA deploy_hires.sql — do NOT run this file standalone:
--   silver.v_hire_retention depends on this view, so the DROP VIEW below fails
--   ("cannot drop view ... because other objects depend on it") unless the
--   dependent is dropped first. deploy_hires.sql (same directory) drops
--   v_hire_retention, rebuilds this view, then rebuilds v_hire_retention.
--
-- ⚠ PREREQUISITE — run this index before deploying the view:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_adp_tenure_applicant_snap
--     ON bronze.adp_tenure_history (applicant_id, snapshot_date);
--   Without this index, the offering-enrichment LATERAL will seq-scan a
--   1.16 GB table per hire-event row.
-- ============================================================================
-- DESIGN NOTES:
--   Grain      : One row per "Hired" funnel event
--                (bronze.portal_requisition_statistics.requisition_statistic_type_id = 6),
--                AFTER collapsing duplicate same-day events for the same
--                (applicant_id, requisition_id) — see the hire_events CTE.
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
--       NULL      = no start date recorded (or a 'pending' row — see below)
--
--   PENDING HIRES  (hire_confirmation_status)
--     In Portal 1, "becoming a hire" touches three independent things at
--     different times: (1) applicant_status_id flips to 5 ("Hired"), (2) a
--     start date is set on portal_users.hire_date, and (3) a recruiter logs a
--     "Hired" funnel event (requisition_statistic_type_id = 6). The event (3)
--     is a MANUAL step and frequently lags the start by days to weeks — so a
--     class can be fully started and still invisible to an event-only view
--     (the GiftHealth 6/22 class: 11 started 6/22, 0 events logged by 6/29).
--
--     This view therefore has two row sources, distinguished by
--     hire_confirmation_status:
--
--       'confirmed' — backed by a logged type-6 Hired event (the original
--                     spine). Full event-axis fields (event_at/date, iso_*,
--                     recruiter). This is the pre-2026-06-29 behavior exactly.
--
--       'pending'   — status = "Hired" + start date set + NO logged event,
--                     AND corroborating evidence the start was real (see
--                     EVIDENCE GATE). These have NO event to point at, so all
--                     EVENT-axis fields are NULL (event_at, event_date,
--                     event_type_*, iso_year/week, recruiter_*, days_to_start)
--                     and event_id is a synthetic -applicant_id (negative = not
--                     a real logged event, still globally unique for row
--                     identity). START-axis fields (actual_start_date,
--                     start_iso_*) ARE populated — that is the whole point.
--                     Order/client/position are inferred from the applicant's
--                     most-recent Accept Offer (type 8), since there is no hire
--                     event to read the requisition from.
--
--     NO DOUBLE COUNTING: the pending source requires NOT EXISTS any type-6
--     event for the applicant (on any requisition), so a person is never both
--     'confirmed' and 'pending'. When the recruiter finally logs the event,
--     the same person stops matching the pending source and appears once as
--     'confirmed' — the row flips pending -> confirmed automatically, and its
--     event_id changes from -applicant_id to the real event id.
--
--     CONSUMERS:
--       • "starting this week" / hires / net gain / retention — use ALL rows
--         (default). Pending rows make the picture complete, current and
--         historical.
--       • recruiter activity / "hires logged this week" — filter to
--         hire_confirmation_status = 'confirmed' (pending rows have no
--         recruiter and no event date, so the event-axis filters already
--         exclude them, but filtering explicitly is clearest).
--       • exact pre-2026-06-29 numbers — filter hire_confirmation_status =
--         'confirmed'.
--
--     EVIDENCE GATE (why, and why NOT a rolling window):
--       Logging the Hired event simply was not a reliable step before ~2023
--       (2023–24 have ~1% pending; 2020–22 have thousands), so an unbounded
--       status-only source would resurrect ~10k historical rows. But those are
--       NOT junk: 90%+ are verifiably real hires — they appear on ADP payroll
--       and/or have a termination record (they provably worked). The
--       confirmed-only spine has therefore been UNDERCOUNTING historical hires.
--       A rolling "last-90-days" window would hide these real hires AND make
--       answers to historical questions change with the calendar (a hire would
--       blink out of the view 90 days after it started). Rejected.
--
--       Instead, a 'pending' row is included when ANY of these holds — a
--       STABLE rule (evidence does not expire), so a real hire never ages out:
--         1. an ADP tenure snapshot exists on/after the start date
--            (proof they were on payroll for THIS stint); OR
--         2. a termination exists on/after the start date
--            (proof they worked this stint, then left); OR
--         3. the start date is within the trailing 90 days
--            (brand-new class — evidence has not landed yet; catches fresh
--            unlogged classes like GiftHealth immediately).
--
--       Only arm 3 is CURRENT_DATE-relative, and it governs ONLY un-evidenced
--       brand-new rows. A genuine recent hire gains ADP/termination evidence
--       within weeks and is then included permanently via arm 1/2. The only
--       rows that age out at 90 days are phantoms: status="Hired" with a start
--       date but never on payroll, never terminated, and not recent — i.e. a
--       hire that never actually materialized (~40 across all years).
--
--     HISTORICAL RESTATEMENT: this is a deliberate, large change. The pending
--     source adds ~10k rows spanning 2015→present, so historical hire counts
--     (and net-gain / retention denominators) rise materially vs. the
--     confirmed-only view — e.g. 2022 ~571 -> ~1,640. Consumers needing the old
--     event-only numbers must filter hire_confirmation_status = 'confirmed'.
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
--     different requisitions, or rehired onto the same one on a DIFFERENT
--     day. event_id is the natural differentiator. For "latest hire per
--     applicant":
--       ROW_NUMBER() OVER (PARTITION BY applicant_id ORDER BY event_at DESC) = 1
--
--     Duplicate same-day events for the same (applicant_id, requisition_id)
--     are collapsed in the hire_events CTE (the earliest is kept). Those are
--     data-entry duplicates — e.g. two recruiters both marking a class hire
--     "Hired" — not distinct hires, and they inflated COUNT(*) over this view
--     before this fix (Alliant Energy 3/16 class: 4 rows per associate → 1).
--
--     REHIRES (2026-07-01 episode-binding fix): actual_start_date is
--     decorated from canonical_users.hire_date, which collapses portal_users
--     to one MUTABLE row per applicant_id (prefers is_active = TRUE, then
--     latest created_at) — that hire_date is the CURRENT stint's start and is
--     overwritten on each rehire. It is therefore attached ONLY to the
--     applicant's most-recent Hired event (applicant_latest_event), and ONLY
--     when it falls on/before event_date + 365. Older rehire events and
--     events whose stored start belongs to a later (eventless) episode get
--     actual_start_date = NULL — the older stint's true start was overwritten
--     and is not recoverable here, so NULL is the honest value (previously
--     these rows carried the LATER stint's start, which leaked into retention
--     and produced negative tenure downstream). RESIDUAL: an older stint's
--     start is not reconstructed. A future enhancement could recover it from
--     bronze.adp_tenure_history.hire_date per snapshot where ADP covers the
--     stint (~84% applicant coverage, recent years only).
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
--   OFFERING ENRICHMENT
--     Five columns at the end of the SELECT carry the Net Gain offering
--     classification for each hire event:
--
--       department_code, department_id, department_name, offering, service,
--       is_offering_excluded, offering_source
--
--     Resolution chain (per event):
--       1. ADP point-in-time (`offering_source = 'adp_pit'`):
--          LATERAL into bronze.adp_tenure_history, earliest snapshot where
--          (applicant_id = rs.applicant_id AND snapshot_date >= event_date).
--          This reflects the department the employee was actually paid in
--          on their first post-hire snapshot. Reflects rehires correctly
--          because the lookup is event-anchored, not applicant-anchored.
--       2. Onboarding department fallback (`offering_source = 'onboarding_dept'`):
--          portal_users.onboarding_department_id when ADP returns nothing.
--          Used for never-started hires and pre-2025 events that lack ADP
--          snapshot history.
--       3. NULL (`offering_source = NULL`):
--          No resolution available.
--
--     is_offering_excluded: TRUE when the resolved offering is one of
--     ('Interns', 'HRMS', 'Internal', 'Long-Term'). Net Gain reports should
--     filter is_offering_excluded = FALSE. Distinct from the
--     v_terminations_unified is_excluded column (which is reason-based).
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


-- ── Step 0: Index prerequisite ────────────────────────────────────────────
-- Required by the offering-enrichment LATERAL into bronze.adp_tenure_history.
-- CONCURRENTLY avoids locking the table during the build; IF NOT EXISTS makes
-- this idempotent across deploys and across the two unified views that share
-- the index (v_hires_unified, v_terminations_unified).
--
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_adp_tenure_applicant_snap
  ON bronze.adp_tenure_history (applicant_id, snapshot_date);


-- ── Step 1: Deploy the view ───────────────────────────────────────────────

DROP VIEW IF EXISTS silver.v_hires_unified;

CREATE VIEW silver.v_hires_unified AS

-- ── Portal 1 branch ───────────────────────────────────────────────────────
WITH

-- 1. Collapse duplicate same-day "Hired" events to one canonical row.
--    Portal 1 lets more than one "Hired" funnel record be logged for the same
--    applicant on the same requisition on the same day — e.g. a class hire
--    where two recruiters each mark the roster "Hired" (and sometimes re-mark
--    it minutes later). Those are duplicate data entries, NOT distinct hires,
--    and they inflate every COUNT(*) over this view (the 3/16 Alliant Energy
--    class produced 4 rows per associate). De-dupe key:
--      (applicant_id, requisition_id, DATE(created_at)).
--    Keep the earliest event of the day (the original decision-to-hire),
--    which also fixes the recruiter attribution to whoever logged it first.
--
--    This is deliberately SAME-DAY only. A genuine rehire onto the same
--    requisition on a LATER day stays a separate row — that preserves the
--    multi-event grain this view is designed around (see "Multiple hires per
--    applicant" below). Hires onto a DIFFERENT requisition are untouched.
hire_events AS (
    SELECT *
    FROM (
        SELECT
            rs.*,
            ROW_NUMBER() OVER (
                PARTITION BY rs.applicant_id, rs.requisition_id, DATE(rs.created_at)
                ORDER BY rs.created_at, rs.id
            ) AS dup_rn
        FROM bronze.portal_requisition_statistics rs
        WHERE rs.requisition_statistic_type_id = 6   -- Hired
    ) ranked
    WHERE dup_rn = 1
),

-- 1b. Latest Hired event per applicant — the EPISODE anchor for person-level
--     enrichment. portal_users holds ONE mutable hire_date and one
--     onboarding_department_id per applicant (the CURRENT stint, overwritten on
--     rehire); those values legitimately belong ONLY to the applicant's
--     most-recent Hired event. Every OLDER event must NOT inherit them (doing so
--     pasted a later re-application's start/department onto an older event —
--     applicant 136818's 2017 Nationwide rows showed a 2026 Alliant start).
--     Uses the de-duplicated hire_events so the anchor matches the emitted grain.
applicant_latest_event AS (
    SELECT applicant_id, event_id
    FROM (
        SELECT
            applicant_id,
            id AS event_id,
            ROW_NUMBER() OVER (
                PARTITION BY applicant_id
                ORDER BY created_at DESC, id DESC
            ) AS rn
        FROM hire_events
    ) z
    WHERE rn = 1
),

-- 2. Resolve duplicate portal_users → one canonical row per applicant_id.
--    Same logic as v_terminations_unified.canonical_users:
--      prefers is_active = TRUE, then most recently created record.
--    Used to decorate each hire event with actual_start_date AND to supply
--    onboarding_department_id for the offering-enrichment fallback.
canonical_users AS (
    SELECT DISTINCT ON (applicant_id)
        id                                      AS user_id,
        applicant_id,
        hire_date,
        onboarding_department_id
    FROM bronze.portal_users
    WHERE applicant_id IS NOT NULL
    ORDER BY
        applicant_id,
        is_active   DESC,
        created_at  DESC
),

-- 3. Recruiter display names — recruiters are also portal_users rows.
--    NO exclusions here (unlike v_applicant_activity_events). This is a
--    fact view; every hire event counts regardless of who logged it.
recruiters AS (
    SELECT
        u.id,
        u.first_name || ' ' || u.last_name      AS recruiter_name
    FROM bronze.portal_users u
    INNER JOIN bronze.portal_role_user ru ON u.id = ru.user_id
    WHERE ru.role_id = 6
),

-- 4a. Latest REAL termination date per applicant — evidence-gate input (arm 2).
--     portal_terminations is keyed by user_id; map to applicant via
--     portal_users. Pre-aggregated once here (the table is small, ~22k rows)
--     rather than as a correlated lookup per pending candidate.
--     Excludes termination_reason_id = 13 ("Never started / delayed"): arm 2
--     means "worked this stint, then left" — a never-started cancellation is
--     NOT proof of a worked stint, so it must not resurrect a phantom hire.
terminated_applicants AS (
    SELECT pu.applicant_id, MAX(t.termination_date) AS last_termination_date
    FROM bronze.portal_terminations t
    JOIN bronze.portal_users pu ON pu.id = t.user_id
    WHERE pu.applicant_id IS NOT NULL
      AND t.termination_reason_id <> 13        -- ignore never-started cancellations
    GROUP BY pu.applicant_id
),

-- 4b. PENDING hires — status "Hired" + start date set, but NO logged event,
--    AND evidence the start was real (see header "PENDING HIRES" / EVIDENCE
--    GATE). One row per applicant at most: canonical_users is DISTINCT ON
--    (applicant_id) and the NOT EXISTS removes anyone with any logged type-6
--    event, so this can never overlap the 'confirmed' spine. The evidence gate
--    (not a rolling window) keeps the source stable for historical questions —
--    a real hire is included via payroll/termination evidence permanently;
--    only un-materialized phantoms (no evidence, not recent) are excluded.
pending_hires AS (
    SELECT
        a.id                                    AS applicant_id,
        a.first_name,
        a.last_name,
        a.client_id                             AS applicant_client_id,
        cu.user_id,
        cu.hire_date,
        cu.onboarding_department_id,
        acc.requisition_id
    FROM bronze.portal_applicants a
    JOIN canonical_users cu
        ON cu.applicant_id = a.id
       AND cu.hire_date IS NOT NULL
    -- Infer the hiring requisition from the most-recent Accept Offer (type 8),
    -- since there is no Hired event to read requisition_id from.
    LEFT JOIN LATERAL (
        SELECT rs.requisition_id
        FROM bronze.portal_requisition_statistics rs
        WHERE rs.applicant_id = a.id
          AND rs.requisition_statistic_type_id = 8   -- Accept Offer
        ORDER BY rs.created_at DESC
        LIMIT 1
    ) acc ON TRUE
    LEFT JOIN terminated_applicants ta
        ON ta.applicant_id = a.id
    WHERE a.applicant_status_id = 5                   -- portal status = "Hired"
      -- ...but no logged Hired event for THIS episode's requisition. Scoping the
      -- guard to acc.requisition_id (rather than applicant-wide) lets a genuinely
      -- new stint on a new order surface even for someone with a prior confirmed
      -- hire on a DIFFERENT order — a rehire gets its own row bound to its own
      -- order/start. When no Accept Offer anchors an episode (acc.requisition_id
      -- IS NULL) the guard falls back to applicant-wide (original behavior), so a
      -- pending row with no order context can never shadow a confirmed hire.
      -- A confirmed hire is still never double-counted on its own requisition
      -- (verified: 0 (applicant, requisition) pairs are both confirmed & pending).
      AND NOT EXISTS (
          SELECT 1
          FROM bronze.portal_requisition_statistics h
          WHERE h.applicant_id = a.id
            AND h.requisition_statistic_type_id = 6
            AND (acc.requisition_id IS NULL
                 OR h.requisition_id = acc.requisition_id)
      )
      AND (                                           -- EVIDENCE GATE (any arm; stable rule)
          -- arm 1: on ADP payroll on/after this start (proof of THIS stint).
          --        Uses idx_adp_tenure_applicant_snap (applicant_id, snapshot_date).
          EXISTS (
              SELECT 1 FROM bronze.adp_tenure_history adp
              WHERE adp.applicant_id   = a.id
                AND adp.snapshot_date >= cu.hire_date
          )
          -- arm 2: terminated on/after this start (proof they worked, then left).
          OR ta.last_termination_date >= cu.hire_date
          -- arm 3: brand-new — evidence not landed yet (catches fresh classes).
          OR cu.hire_date >= CURRENT_DATE - INTERVAL '90 days'
      )
)

SELECT

    -- ── Source tracking ───────────────────────────────────────────────────
    'portal_v1'                                 AS source_system,

    -- Event-backed rows are the original spine — confirmed hires.
    -- (The pending branch below sets this to 'pending'.)
    'confirmed'::text                           AS hire_confirmation_status,

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
    -- EPISODE-BOUND (2026-07-01): es.episode_start is cu.hire_date attached ONLY
    -- to the applicant's latest Hired event and only when the start is on/before
    -- event_date + 365. NULL on older rehire events and on far-later starts that
    -- belong to a subsequent eventless episode. See the es LATERAL below.
    es.episode_start                            AS actual_start_date,
    CASE
        WHEN es.episode_start IS NOT NULL
        THEN (es.episode_start - DATE(rs.created_at))::int
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
    EXTRACT(isoyear FROM es.episode_start)::int AS start_iso_year,
    EXTRACT(week    FROM es.episode_start)::int AS start_iso_week,

    -- ── Offering enrichment ───────────────────────────────────────────────
    -- See header "OFFERING ENRICHMENT" for the full resolution story.
    -- Resolved department code first (ADP PIT, then onboarding fallback);
    -- everything else is joined from v_department_offering on department_code.
    COALESCE(adp_pit.dept_code, onboard_dept.code)  AS department_code,
    vdo.department_id,
    vdo.department_name,
    vdo.offering,
    vdo.service,
    COALESCE(vdo.is_excluded, FALSE)                AS is_offering_excluded,
    CASE
        WHEN adp_pit.dept_code  IS NOT NULL THEN 'adp_pit'
        WHEN onboard_dept.code  IS NOT NULL THEN 'onboarding_dept'
        ELSE                                         NULL
    END                                             AS offering_source

FROM hire_events rs

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

-- ── Episode binding for person-level enrichment (2026-07-01) ──────────────
-- ale = the applicant's latest Hired event; es exposes whether THIS event is
-- the current episode and the episode-bound start date. portal_users stores one
-- mutable hire_date + onboarding_department_id per applicant (the current stint),
-- so those decorate ONLY the latest event, and the start only when it is not a
-- far-later (eventless) episode's start (on/before event_date + 365). No lower
-- bound: a start BEFORE the event is the normal event-logging lag, not a defect.
LEFT JOIN applicant_latest_event ale
    ON ale.applicant_id = rs.applicant_id
LEFT JOIN LATERAL (
    SELECT
        (rs.id = ale.event_id
         AND (cu.hire_date IS NULL
              OR cu.hire_date <= DATE(rs.created_at) + 365))  AS is_current_episode,
        CASE
            WHEN rs.id = ale.event_id
             AND cu.hire_date IS NOT NULL
             AND cu.hire_date <= DATE(rs.created_at) + 365
            THEN cu.hire_date
        END                                                    AS episode_start
) es ON TRUE

-- ── Offering enrichment joins ─────────────────────────────────────────────
-- 1. ADP point-in-time: earliest snapshot in the window [event_date,
--    event_date + 365] for the applicant. Event-anchored (uses rs.created_at,
--    not cu.hire_date) so rehires resolve to their per-event department. The
--    upper bound (added 2026-07-01) stops a far-LATER snapshot from backfilling
--    an old event's department across an episode gap (applicant 136818's 2017
--    events were picking up a 2026 Alliant department this way). A legitimate
--    recent hire's first ADP snapshot lands within days, well inside the window.
--    Requires idx_adp_tenure_applicant_snap.
LEFT JOIN LATERAL (
    SELECT TRIM(adp.home_department_code)       AS dept_code
    FROM bronze.adp_tenure_history adp
    WHERE adp.applicant_id   = rs.applicant_id
      AND adp.snapshot_date >= DATE(rs.created_at)
      AND adp.snapshot_date <= DATE(rs.created_at) + 365
    ORDER BY adp.snapshot_date
    LIMIT 1
) adp_pit ON TRUE

-- 2. Onboarding department fallback for events with no usable ADP snapshot
--    (never-started hires, pre-2025 events). Joined to portal_departments only
--    to expose `code`, which is what v_department_offering joins on. Gated to
--    the current episode (es.is_current_episode): onboarding_department_id, like
--    hire_date, reflects the CURRENT stint, so it must not decorate older events.
LEFT JOIN bronze.portal_departments onboard_dept
    ON onboard_dept.id = CASE WHEN es.is_current_episode
                              THEN cu.onboarding_department_id END

-- 3. Offering crosswalk. Joined by the resolved department code; vdo is
--    NULL for both columns when neither ADP nor onboarding resolve.
LEFT JOIN silver.v_department_offering vdo
    ON vdo.department_code = COALESCE(adp_pit.dept_code, onboard_dept.code)

-- The Hired filter (requisition_statistic_type_id = 6) now lives in the
-- hire_events CTE, alongside the same-day de-duplication.


-- ── Pending branch ────────────────────────────────────────────────────────
-- Status-backed hires with a start date but no logged event yet. Column list
-- and order MUST match the confirmed branch above exactly. Event-axis fields
-- are NULL (there is no event); start-axis fields are populated. See header
-- "PENDING HIRES".
UNION ALL

SELECT

    -- ── Source tracking ───────────────────────────────────────────────────
    'portal_v1'                                 AS source_system,
    'pending'::text                             AS hire_confirmation_status,

    -- ── Hire event (NONE — synthetic, event-axis is empty) ────────────────
    -- event_id = -applicant_id: negative flags "not a real logged event" and
    -- stays globally unique so (source_system, event_id) row identity holds.
    (-ph.applicant_id)::bigint                  AS event_id,
    NULL::timestamptz                           AS event_at,
    NULL::date                                  AS event_date,
    NULL::bigint                                AS event_type_id,
    NULL::varchar                               AS event_type_name,

    -- ── Person ────────────────────────────────────────────────────────────
    ph.user_id,
    ph.applicant_id,
    ph.first_name,
    ph.last_name,
    ph.first_name || ' ' || ph.last_name        AS full_name,

    -- ── Position / Order context (inferred from Accept Offer) ─────────────
    ph.requisition_id                           AS position_id,
    ph.requisition_id                           AS order_id,
    r.requisition_key                           AS position_key,
    r.position                                  AS position_title,
    r.is_pipeline,

    -- ── Client ────────────────────────────────────────────────────────────
    COALESCE(r.client_id, ph.applicant_client_id)  AS client_id,
    c.name                                      AS client_name,

    -- ── Hire timing: start date known, event date unknown ─────────────────
    ph.hire_date                                AS actual_start_date,
    NULL::int                                   AS days_to_start,   -- lag undefined w/o event

    -- ── Recruiter: none (no event was logged) ─────────────────────────────
    NULL::bigint                                AS recruiter_id,
    NULL::text                                  AS recruiter_name,

    -- ── ISO week helpers: EVENT axis is empty for pending rows ────────────
    NULL::int                                   AS iso_year,
    NULL::int                                   AS iso_week,

    -- ── ISO week helpers: START axis (the point of this branch) ───────────
    EXTRACT(isoyear FROM ph.hire_date)::int     AS start_iso_year,
    EXTRACT(week    FROM ph.hire_date)::int     AS start_iso_week,

    -- ── Offering enrichment (anchored on the start date) ──────────────────
    COALESCE(adp_pit.dept_code, onboard_dept.code)  AS department_code,
    vdo.department_id,
    vdo.department_name,
    vdo.offering,
    vdo.service,
    COALESCE(vdo.is_excluded, FALSE)                AS is_offering_excluded,
    CASE
        WHEN adp_pit.dept_code  IS NOT NULL THEN 'adp_pit'
        WHEN onboard_dept.code  IS NOT NULL THEN 'onboarding_dept'
        ELSE                                         NULL
    END                                             AS offering_source

FROM pending_hires ph

LEFT JOIN bronze.portal_requisitions r
    ON r.id = ph.requisition_id

LEFT JOIN bronze.portal_clients c
    ON c.id = COALESCE(r.client_id, ph.applicant_client_id)

-- Offering enrichment, same chain as the confirmed branch but anchored on the
-- start date (no event date exists). Requires idx_adp_tenure_applicant_snap.
LEFT JOIN LATERAL (
    SELECT TRIM(adp.home_department_code)       AS dept_code
    FROM bronze.adp_tenure_history adp
    WHERE adp.applicant_id   = ph.applicant_id
      AND adp.snapshot_date >= ph.hire_date
    ORDER BY adp.snapshot_date
    LIMIT 1
) adp_pit ON TRUE

LEFT JOIN bronze.portal_departments onboard_dept
    ON onboard_dept.id = ph.onboarding_department_id

LEFT JOIN silver.v_department_offering vdo
    ON vdo.department_code = COALESCE(adp_pit.dept_code, onboard_dept.code)


-- ══════════════════════════════════════════════════════════════════════════
-- FUTURE: Cajetan (Portal 2) UNION ALL branch
-- Add when cajetan schema is available in the warehouse.
-- Column list and order must exactly match the portal_v1 branch above.
-- ══════════════════════════════════════════════════════════════════════════
--
-- UNION ALL
-- SELECT
--     'cajetan'::text                                 AS source_system,
--     'confirmed'::text                               AS hire_confirmation_status,
--     h.id                                            AS event_id,
--     h.created_at                                    AS event_at,
--     DATE(h.created_at)                              AS event_date,
--     ... (all fields matching portal_v1 branch) ...
-- FROM cajetan.<hire_events> h
-- ...
;


COMMENT ON VIEW silver.v_hires_unified IS
'Unified hires view. One row per Hired funnel event
(portal_requisition_statistics.requisition_statistic_type_id = 6), after
collapsing duplicate same-day events for the same (applicant_id,
requisition_id) — see the hire_events CTE. Duplicate same-day "Hired"
entries (e.g. two recruiters marking a class hire) are data-entry
artifacts, not distinct hires, and were inflating COUNT(*) over this view.
Genuine rehires onto the same requisition on a different day, and hires
onto a different requisition, remain separate rows.
Uses Cajetan (Portal 2) vocabulary; Portal 1 fields aliased to match.

TWO ROW SOURCES (hire_confirmation_status):
  confirmed  Backed by a logged Hired event (type 6) — the original spine,
             full event-axis fields. Filter to this for exact pre-2026-06-29
             numbers and for recruiter-activity / "hires logged this week".
  pending    Portal status = Hired + start date set + NO logged event on this
             episode''s requisition, AND evidence the start was real: on ADP
             payroll on/after the start, OR terminated (for a REAL reason, not
             "Never started / delayed") on/after the start, OR started within the
             trailing 90 days (brand-new, evidence not yet landed). Event-axis
             fields are NULL; start-axis fields populated; event_id = -applicant_id
             (synthetic); order/client inferred from the most-recent Accept
             Offer (type 8). Surfaces real hires the event-logging step missed
             (~10k rows, 2015->present — the confirmed-only spine undercounts
             history; e.g. 2022 ~571 -> ~1,640). ANTI-DOUBLE-COUNT (2026-07-01):
             the guard is REQUISITION-SCOPED — a pending row is suppressed only
             when a type-6 event exists on the SAME inferred requisition, so a
             confirmed hire is never double-counted on its own order, yet a
             genuinely new stint on a NEW order surfaces even for someone with a
             prior confirmed hire (a rehire gets its own row). When no Accept
             Offer anchors an episode the guard falls back to applicant-wide.
             A pending row flips to confirmed automatically when its event is
             logged. The evidence gate is STABLE — a real hire never ages out;
             only un-materialized phantoms (no payroll, no real termination, not
             recent) drop after 90 days. Default queries include both sources;
             filter to confirmed for event-only counts.

INVARIANT (2026-07-01): row identity / uniqueness is per (source_system,
event_id) as before, but confirmed-vs-pending exclusivity is now per
(applicant_id, order_id), NOT per applicant. A person may be confirmed on one
order and pending on a DIFFERENT order (a rehire in progress). No (applicant_id,
order_id) is ever both.

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

Rehire binding (2026-07-01): actual_start_date (and start_iso_*, days_to_start)
is bound to the correct EPISODE. portal_users holds one mutable hire_date per
applicant (the current stint), so it decorates ONLY the applicant''s most-recent
Hired event, and only when the start is on/before event_date + 365. Older rehire
events and far-later (eventless-episode) starts get NULL — the honest value,
since the older stint''s start was overwritten and is not recoverable here.
Previously every event carried the latest start, which leaked into retention and
produced negative downstream tenure.

OFFERING ENRICHMENT (2026-06-03):
  department_code / department_id / department_name / offering / service /
  is_offering_excluded / offering_source

  Resolved by:
    1. adp_pit          (bronze.adp_tenure_history, earliest snapshot in
                         [event_date, event_date + 365])
    2. onboarding_dept  (portal_users.onboarding_department_id, current episode only)
    3. NULL

  Rehires resolve correctly because the ADP lookup is event-anchored and window-
  bounded (a far-later snapshot cannot backfill an old event), and the onboarding
  fallback is gated to the current episode.
  is_offering_excluded = TRUE for Interns / HRMS / Internal / Long-Term;
  Net Gain reports should filter is_offering_excluded = FALSE.

  Requires: idx_adp_tenure_applicant_snap on
            bronze.adp_tenure_history (applicant_id, snapshot_date).
  Source rejected: portal_associates.id is NOT a reliable proxy for
                   portal_users.id (~32% of associate rows collide with
                   unrelated user rows from different time periods).';


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

-- 2b. Same-day de-dup held — no (applicant, order, event_date) appears twice.
--     Expect zero rows. Multi-DAY rehires onto the same order are allowed and
--     will NOT show up here (different event_date).
--    SELECT applicant_id, order_id, event_date, COUNT(*)
--    FROM silver.v_hires_unified
--    GROUP BY 1, 2, 3
--    HAVING COUNT(*) > 1;

-- 2c. Alliant Energy 3/16 class regression check — expect 10, not 40.
--    SELECT COUNT(*)
--    FROM silver.v_hires_unified
--    WHERE client_name ILIKE '%Alliant%'
--      AND actual_start_date >= '2026-03-01'
--      AND actual_start_date <  '2026-04-01'
--      AND is_offering_excluded = FALSE;

-- 2d. PENDING source — confirmed/pending split. Pending spans 2015->present
--     (~10k evidence-gated rows as of 2026-06); confirmed is the event spine.
--    SELECT hire_confirmation_status, COUNT(*),
--           MIN(actual_start_date), MAX(actual_start_date)
--    FROM silver.v_hires_unified
--    GROUP BY 1;

-- 2d2. Evidence gate held — every pending row has payroll/termination evidence
--      on/after its start, OR started in the trailing 90 days. Expect zero rows.
--    SELECT COUNT(*)
--    FROM silver.v_hires_unified h
--    WHERE h.hire_confirmation_status = 'pending'
--      AND h.actual_start_date < CURRENT_DATE - INTERVAL '90 days'
--      AND NOT EXISTS (SELECT 1 FROM bronze.adp_tenure_history adp
--                        WHERE adp.applicant_id = h.applicant_id
--                          AND adp.snapshot_date >= h.actual_start_date)
--      AND NOT EXISTS (SELECT 1 FROM bronze.portal_terminations t
--                        JOIN bronze.portal_users pu ON pu.id = t.user_id
--                       WHERE pu.applicant_id = h.applicant_id
--                         AND t.termination_date >= h.actual_start_date);

-- 2e. No (applicant_id, order_id) is both confirmed AND pending. As of the
--     2026-07-01 requisition-scoped guard, exclusivity is per (applicant, order),
--     NOT per applicant — a person MAY be confirmed on one order and pending on
--     another (a rehire). This check must be zero rows; the old per-applicant
--     version will (correctly) show the ~900 rehire episodes and is superseded.
--    SELECT applicant_id, order_id
--    FROM silver.v_hires_unified
--    GROUP BY applicant_id, order_id
--    HAVING COUNT(DISTINCT hire_confirmation_status) > 1;
--
-- 2e2. Episode binding — no confirmed row has (actual_start_date - event_date)
--      > 365. Expect zero rows.
--    SELECT COUNT(*) FROM silver.v_hires_unified
--    WHERE hire_confirmation_status = 'confirmed'
--      AND actual_start_date IS NOT NULL
--      AND (actual_start_date - event_date) > 365;

-- 2f. GiftHealth 6/22 class surfaces as pending — expect 11 (orders 9342/9344).
--    SELECT full_name, order_id, actual_start_date, hire_confirmation_status
--    FROM silver.v_hires_unified
--    WHERE client_name ILIKE '%Gifthealth%'
--      AND actual_start_date = '2026-06-22'
--    ORDER BY order_id, full_name;

-- 2g. Pending rows carry NO event-axis data (all NULL) and a negative event_id.
--     Expect zero rows.
--    SELECT COUNT(*)
--    FROM silver.v_hires_unified
--    WHERE hire_confirmation_status = 'pending'
--      AND (event_at IS NOT NULL OR event_date IS NOT NULL
--           OR iso_year IS NOT NULL OR recruiter_id IS NOT NULL
--           OR event_id >= 0 OR actual_start_date IS NULL);

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

-- 9. Offering resolution coverage — what % of recent hires get an offering
--    SELECT
--      COUNT(*)                                          AS total,
--      COUNT(offering)                                   AS with_offering,
--      COUNT(*) FILTER (WHERE offering_source = 'adp_pit')         AS via_adp,
--      COUNT(*) FILTER (WHERE offering_source = 'onboarding_dept') AS via_onboarding,
--      COUNT(*) FILTER (WHERE offering_source IS NULL)             AS unresolved,
--      ROUND(100.0 * COUNT(offering) / COUNT(*), 1)      AS pct_with_offering
--    FROM silver.v_hires_unified
--    WHERE event_date >= CURRENT_DATE - INTERVAL '90 days';

-- 10. Offering distribution on Net Gain eligible hires (last 90 days)
--    SELECT offering, service, COUNT(*)
--    FROM silver.v_hires_unified
--    WHERE event_date >= CURRENT_DATE - INTERVAL '90 days'
--      AND is_offering_excluded = FALSE
--    GROUP BY 1, 2
--    ORDER BY 3 DESC;

-- 11. Rehires sanity check — same applicant, different events, expect
--     potentially different offerings if they switched dept between stints
--    SELECT applicant_id, event_date, event_id, offering, offering_source
--    FROM silver.v_hires_unified
--    WHERE applicant_id IN (
--      SELECT applicant_id FROM silver.v_hires_unified
--      GROUP BY 1 HAVING COUNT(*) > 1
--    )
--    ORDER BY applicant_id, event_at;
